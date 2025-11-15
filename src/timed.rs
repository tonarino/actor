//! Tools to make a given actor able to receive delayed and recurring messages.
//!
//! To apply this to a given (receiving) actor:
//! * Use [`TimedContext<Self::Message>`] as [`Actor::Context`] associated type.
//!   * Such actors cannot be spawned unless wrapped, making it impossible to forget wrapping it.
//! * Wrap the actor in [`Timed`] before spawning.
//!
//! The wrapped actor will accept [`TimedMessage<M>`] with convenience conversion from `M`.
//! [`RecipientExt`] becomes available for [`Recipient<TimedMessage<M>>`]s which provides methods like
//! `send_delayed()`, `send_recurring()`.
//!
//! Once accepted by the actor, delayed and recurring messages do not occupy place in actor's
//! channel inbox, they are placed to internal queue instead. Due to the design, delayed and
//! recurring messages have always lower priority than instant messages when the actor is
//! saturated.
//!
//! See `delay_actor.rs` example for usage.

use crate::{Actor, BareContext, Context, Event, Priority, Recipient, SendError};
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    ops::Deref,
    time::{Duration, Instant},
};

/// A message that can be delivered now, at certain time and optionally repeatedly.
pub enum TimedMessage<M> {
    Instant { message: M },
    Delayed { message: M, fire_at: Instant },
    Recurring { factory: Box<dyn FnMut() -> M + Send>, fire_at: Instant, interval: Duration },
}

/// This implementation allows sending direct unwrapped messages to wrapped actors.
impl<M> From<M> for TimedMessage<M> {
    fn from(message: M) -> Self {
        Self::Instant { message }
    }
}

/// Convenience methods for [`Recipient`]s that accept [`TimedMessage`]s.
pub trait RecipientExt<M> {
    /// Send a `message` now. Convenience to wrap message in [`TimedMessage::Instant`].
    fn send_now(&self, message: M) -> Result<(), SendError>;

    /// Send a `message` to be delivered later at a certain instant.
    fn send_timed(&self, message: M, fire_at: Instant) -> Result<(), SendError>;

    /// Send a `message` to be delivered later after some time from now.
    fn send_delayed(&self, message: M, delay: Duration) -> Result<(), SendError> {
        self.send_timed(message, Instant::now() + delay)
    }

    /// Schedule sending of message at `fire_at` plus at regular `interval`s from that point on.
    fn send_recurring(
        &self,
        factory: impl FnMut() -> M + Send + 'static,
        fire_at: Instant,
        interval: Duration,
    ) -> Result<(), SendError>;
}

impl<M> RecipientExt<M> for Recipient<TimedMessage<M>> {
    fn send_now(&self, message: M) -> Result<(), SendError> {
        self.send(TimedMessage::Instant { message })
    }

    fn send_timed(&self, message: M, fire_at: Instant) -> Result<(), SendError> {
        self.send(TimedMessage::Delayed { message, fire_at })
    }

    fn send_recurring(
        &self,
        factory: impl FnMut() -> M + Send + 'static,
        fire_at: Instant,
        interval: Duration,
    ) -> Result<(), SendError> {
        self.send(TimedMessage::Recurring { factory: Box::new(factory), fire_at, interval })
    }
}

/// A [`Context`] variant available to actors wrapped by the [`Timed`] actor wrapper.
/// Wraps and dereferences to [`BareContext`] with the message wrapped in [`TimedMessage`].
pub struct TimedContext<M>(BareContext<TimedMessage<M>>);

impl<M> TimedContext<M> {
    fn from_context(context: &Context<TimedMessage<M>>) -> Self {
        Self(context.deref().clone())
    }
}

impl<M: 'static> TimedContext<M> {
    /// Subscribe current actor to event of type `E`. Events will be delivered as instant messages.
    /// A variant of [`BareContext::subscribe()`] that performs one extra message conversion:
    /// `E` -> `M` -> `TimedMessage<M>`.
    pub fn subscribe<E: Event + Into<M>>(&self) {
        // The recipient() call performs conversion from `M` to an immediate `TimedMessage<M>`.
        self.system_handle.subscribe_recipient::<M, E>(self.myself.recipient());
    }

    /// Subscribe current actor to event of type `E` and send the last cached event to it.
    /// Events will be delivered as instant messages.
    /// A variant of [`BareContext::subscribe_and_receive_latest()`] that performs one extra message
    /// conversion: `E` -> `M` -> `TimedMessage<M>`.
    pub fn subscribe_and_receive_latest<E: Event + Into<M>>(&self) -> Result<(), SendError> {
        self.system_handle.subscribe_and_receive_latest::<M, E>(self.myself.recipient())
    }
}

impl<M> Deref for TimedContext<M> {
    type Target = BareContext<TimedMessage<M>>;

    fn deref(&self) -> &BareContext<TimedMessage<M>> {
        &self.0
    }
}

/// A wrapper around actors to add ability to receive delayed and recurring messages.
/// See [module documentation](self) for a complete recipe.
pub struct Timed<A: Actor> {
    inner: A,
    queue: BinaryHeap<QueueItem<A::Message>>,
}

impl<M: Send + 'static, A: Actor<Context = TimedContext<M>, Message = M>> Timed<A> {
    pub fn new(inner: A) -> Self {
        Self { inner, queue: Default::default() }
    }

    /// Process any pending messages in the internal queue, calling wrapped actor's `handle()`.
    fn process_queue(&mut self, context: &mut <Self as Actor>::Context) -> Result<(), A::Error> {
        // Handle all messages that should have been handled by now.
        let now = Instant::now();
        while self.queue.peek().map(|m| m.fire_at <= now).unwrap_or(false) {
            let item = self.queue.pop().expect("heap is non-empty, we have just peeked");

            let message = match item.payload {
                Payload::Delayed { message } => message,
                Payload::Recurring { mut factory, interval } => {
                    let message = factory();
                    self.queue.push(QueueItem {
                        fire_at: item.fire_at + interval,
                        payload: Payload::Recurring { factory, interval },
                    });
                    message
                },
            };

            // Let inner actor do its job.
            //
            // Alternatively, we could send an `Instant` message to ourselves.
            // - The advantage would be that it would go into the queue with proper priority. But it
            //   is unclear what should be handled first: normal-priority message that should have
            //   been processed a while ago, or a high-priority message that was delivered now.
            // - Disadvantage is we could easily overflow the queue if many messages fire at once.
            self.inner.handle(&mut TimedContext::from_context(context), message)?;
        }

        Ok(())
    }

    fn schedule_timeout(&self, context: &mut <Self as Actor>::Context) {
        // Schedule next timeout if the queue is not empty.
        context.set_deadline(self.queue.peek().map(|earliest| earliest.fire_at));
    }
}

impl<M: Send + 'static, A: Actor<Context = TimedContext<M>, Message = M>> Actor for Timed<A> {
    type Context = Context<Self::Message>;
    type Error = A::Error;
    type Message = TimedMessage<M>;

    const DEFAULT_CAPACITY_HIGH: usize = A::DEFAULT_CAPACITY_HIGH;
    const DEFAULT_CAPACITY_NORMAL: usize = A::DEFAULT_CAPACITY_NORMAL;

    fn handle(
        &mut self,
        context: &mut Self::Context,
        timed_message: Self::Message,
    ) -> Result<(), Self::Error> {
        match timed_message {
            TimedMessage::Instant { message } => {
                self.inner.handle(&mut TimedContext::from_context(context), message)?;
            },
            TimedMessage::Delayed { message, fire_at } => {
                self.queue.push(QueueItem { fire_at, payload: Payload::Delayed { message } });
            },
            TimedMessage::Recurring { factory, fire_at, interval } => {
                self.queue
                    .push(QueueItem { fire_at, payload: Payload::Recurring { factory, interval } });
            },
        };

        // Process any expired items in the queue. In case that the actor is non-stop busy (there's
        // always a message in its queue, perhaps because it sends a message to itself in handle()),
        // this would be the only occasion where we go through it.
        self.process_queue(context)?;

        self.schedule_timeout(context);
        Ok(())
    }

    fn name() -> &'static str {
        A::name()
    }

    fn priority(message: &Self::Message) -> Priority {
        match message {
            // Use underlying message priority if we can reference it.
            TimedMessage::Instant { message } | TimedMessage::Delayed { message, .. } => {
                A::priority(message)
            },
            // Recurring message is only received once, the recurring instances go through the
            // internal queue (and not actor's channel). Assign high priority to the request to
            // set-up the recurrent sending.
            TimedMessage::Recurring { .. } => Priority::High,
        }
    }

    fn started(&mut self, context: &mut Self::Context) -> Result<(), Self::Error> {
        self.inner.started(&mut TimedContext::from_context(context))
    }

    fn stopped(&mut self, context: &mut Self::Context) -> Result<(), Self::Error> {
        self.inner.stopped(&mut TimedContext::from_context(context))
    }

    fn deadline_passed(
        &mut self,
        context: &mut Self::Context,
        _deadline: Instant,
    ) -> Result<(), Self::Error> {
        self.process_queue(context)?;
        self.schedule_timeout(context);
        Ok(())
    }
}

/// Access wrapped actor.
impl<A: Actor> Deref for Timed<A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Implementation detail, element of message queue ordered by time to fire at.
struct QueueItem<M> {
    fire_at: Instant,
    payload: Payload<M>,
}

impl<M> PartialEq for QueueItem<M> {
    fn eq(&self, other: &Self) -> bool {
        self.fire_at == other.fire_at
    }
}

// We cannot derive because that would add too strict bounds.
impl<M> Eq for QueueItem<M> {}

impl<M> PartialOrd for QueueItem<M> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<M> Ord for QueueItem<M> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse because [BinaryHeap] is a *max* heap, but we want pop() to return lowest `fire_at`.
        self.fire_at.cmp(&other.fire_at).reverse()
    }
}

enum Payload<M> {
    Delayed { message: M },
    Recurring { factory: Box<dyn FnMut() -> M + Send>, interval: Duration },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::System;
    use std::{
        sync::{Arc, Mutex},
        thread,
    };

    struct TimedTestActor {
        received: Arc<Mutex<Vec<usize>>>,
    }

    impl Actor for TimedTestActor {
        type Context = TimedContext<Self::Message>;
        type Error = String;
        type Message = usize;

        fn handle(&mut self, context: &mut Self::Context, message: usize) -> Result<(), String> {
            {
                let mut guard = self.received.lock().unwrap();
                guard.push(message);
            }

            // Messages 1 or 3 are endless self-sending ones, keep the loop spinning.
            if message == 1 || message == 3 {
                thread::sleep(Duration::from_millis(100));
                context.myself.send_now(3).unwrap();
            }

            Ok(())
        }

        fn started(&mut self, context: &mut Self::Context) -> Result<(), String> {
            context
                .myself
                .send_recurring(
                    || 2,
                    Instant::now() + Duration::from_millis(50),
                    Duration::from_millis(100),
                )
                .map_err(|e| e.to_string())
        }
    }

    #[test]
    fn recurring_messages_for_busy_actors() {
        let received = Arc::new(Mutex::new(Vec::new()));

        let mut system = System::new("timed test");
        let address =
            system.spawn(Timed::new(TimedTestActor { received: Arc::clone(&received) })).unwrap();
        address.send_now(1).unwrap();
        thread::sleep(Duration::from_millis(225));

        // The order of messages should be:
        // 1 (initial message),
        // 2 (first recurring scheduled message),
        // 3 (first self-sent message),
        // 2 (second recurring message)
        // 3 (second self-sent message)
        assert_eq!(*received.lock().unwrap(), vec![1, 2, 3, 2, 3]);
        system.shutdown().unwrap();
    }
}
