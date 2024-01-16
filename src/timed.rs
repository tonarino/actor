//! Tools to make a given actor able to receive delayed and recurring messages.
//!
//! To apply this to a given (receiving) actor:
//! * Use [`TimedContext<Self::Message>`] as [`Actor::Context`] associated type.
//!   * Such actors cannot be spawned unless wrapped, making it impossible to forget wrapping it.
//! * Wrapped actor's `Error` must implement [`From<SendError>`].
//! * Wrap the actor in [`Timed`] before spawning.
//!
//! The wrapped actor will accept [`TimedMessage<M>`] with convenience conversion from `M`.
//! [`RecipientExt`] becomes available for [`Recipient<TimedMessage<M>>`]s which provides methods like
//! `send_delayed()`, `send_recurring()`.
//!
//! Once accepted by the actor, delayed and recurring messages do not occupy place in actor's
//! channel inbox, they are placed to internal queue instead. When delayed/recurring message become
//! due, they go through the actor's regular inboxes (subject to prioritization).
//!
//! See `delay_actor.rs` example for usage.

use crate::{Actor, Context, Event, Priority, Recipient, SendError, SystemHandle};
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    ops::Deref,
    time::{Duration, Instant},
};

/// A message that can be enqueued now, at certain time and optionally repeatedly.
pub enum TimedMessage<M> {
    /// Instant message `handle()`d by the wrapped actor right away.
    Instant { message: M },
    /// Request to setup a delayed message. Goes to internal [`Timed`] wrapper queue and then gets
    /// sent to ourselves as an `Instant` message at the specified time.
    Delayed { message: M, enqueue_at: Instant },
    /// Request to setup a recurring message. Goes to internal [`Timed`] wrapper queue and then gets
    /// sent to ourselves as an `Instant` message regularly at the specified pace.
    Recurring { factory: Box<dyn FnMut() -> M + Send>, enqueue_at: Instant, interval: Duration },
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

    /// Send a `message` to be enqueued later at a certain instant.
    fn send_timed(&self, message: M, enqueue_at: Instant) -> Result<(), SendError>;

    /// Send a `message` to be enqueued later after some time from now.
    fn send_delayed(&self, message: M, delay: Duration) -> Result<(), SendError> {
        self.send_timed(message, Instant::now() + delay)
    }

    /// Schedule sending of message at `enqueue_at` plus at regular `interval`s from that point on.
    fn send_recurring(
        &self,
        factory: impl FnMut() -> M + Send + 'static,
        enqueue_at: Instant,
        interval: Duration,
    ) -> Result<(), SendError>;
}

impl<M> RecipientExt<M> for Recipient<TimedMessage<M>> {
    fn send_now(&self, message: M) -> Result<(), SendError> {
        self.send(TimedMessage::Instant { message })
    }

    fn send_timed(&self, message: M, enqueue_at: Instant) -> Result<(), SendError> {
        self.send(TimedMessage::Delayed { message, enqueue_at })
    }

    fn send_recurring(
        &self,
        factory: impl FnMut() -> M + Send + 'static,
        enqueue_at: Instant,
        interval: Duration,
    ) -> Result<(), SendError> {
        self.send(TimedMessage::Recurring { factory: Box::new(factory), enqueue_at, interval })
    }
}

/// A [`Context`] variant available to actors wrapped by the [`Timed`] actor wrapper.
pub struct TimedContext<M> {
    pub system_handle: SystemHandle,
    pub myself: Recipient<TimedMessage<M>>,
}

impl<M> TimedContext<M> {
    fn from_context(context: &Context<TimedMessage<M>>) -> Self {
        TimedContext {
            system_handle: context.system_handle.clone(),
            myself: context.myself.clone(),
        }
    }

    /// Subscribe current actor to event of type `E`. Events will be delivered as instant messages.
    /// See [`crate::Context::subscribe()`].
    pub fn subscribe<E: Event + Into<M>>(&self)
    where
        M: 'static,
    {
        // The recipient() call allows conversion from M to TimedMessage<M>.
        self.system_handle.subscribe_recipient::<M, E>(self.myself.recipient());
    }
}

/// A wrapper around actors to add ability to receive delayed and recurring messages.
/// See [module documentation](self) for a complete recipe.
pub struct Timed<A: Actor> {
    inner: A,
    queue: BinaryHeap<QueueItem<A::Message>>,
}

impl<M: Send + 'static, A: Actor<Context = TimedContext<M>, Message = M>> Timed<A>
where
    <A as Actor>::Error: From<SendError>,
{
    pub fn new(inner: A) -> Self {
        Self { inner, queue: Default::default() }
    }

    /// Process any pending messages in the internal queue, calling wrapped actor's `handle()`.
    fn process_queue(&mut self, context: &mut <Self as Actor>::Context) -> Result<(), SendError> {
        // If the message on top of the queue is due, send it to ourselves as `Instant` to enqueue
        // it in the regular actor queue.
        // No problem if there are multiple such messages, the next Timed::handle() will call
        // process_queue() again.
        if self.queue.peek().map(|m| m.enqueue_at <= Instant::now()).unwrap_or(false) {
            let item = self.queue.pop().expect("heap is non-empty, we have just peeked");

            let message = match item.payload {
                Payload::Delayed { message } => message,
                Payload::Recurring { mut factory, interval } => {
                    let message = factory();
                    self.queue.push(QueueItem {
                        enqueue_at: item.enqueue_at + interval,
                        payload: Payload::Recurring { factory, interval },
                    });
                    message
                },
            };

            // Enqueue an immediate message to process. Alternative would be to call inner handle(),
            // but we don't want to effectively call child handle() twice in the parent handle().
            context.myself.send_now(message)?;
        }

        Ok(())
    }

    fn schedule_timeout(&self, context: &mut <Self as Actor>::Context) {
        // Schedule next timeout if the queue is not empty.
        context.set_deadline(self.queue.peek().map(|earliest| earliest.enqueue_at));
    }
}

impl<M: Send + 'static, A: Actor<Context = TimedContext<M>, Message = M>> Actor for Timed<A>
where
    <A as Actor>::Error: From<SendError>,
{
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
            TimedMessage::Delayed { message, enqueue_at } => {
                self.queue.push(QueueItem { enqueue_at, payload: Payload::Delayed { message } });
            },
            TimedMessage::Recurring { factory, enqueue_at, interval } => {
                self.queue.push(QueueItem {
                    enqueue_at,
                    payload: Payload::Recurring { factory, interval },
                });
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
            // Use underlying message priority for instant messages.
            TimedMessage::Instant { message } => A::priority(message),
            // These priorities apply to the *set-up* of Delayed and Recurring messages and we
            // want to handle that pronto.
            // The resulting inner message then comes back as `Instant` and is prioritized per its
            // underlying priority.
            TimedMessage::Recurring { .. } | TimedMessage::Delayed { .. } => Priority::High,
        }
    }

    fn started(&mut self, context: &mut Self::Context) {
        self.inner.started(&mut TimedContext::from_context(context))
    }

    fn stopped(&mut self, context: &mut Self::Context) {
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
    enqueue_at: Instant,
    payload: Payload<M>,
}

impl<M> PartialEq for QueueItem<M> {
    fn eq(&self, other: &Self) -> bool {
        self.enqueue_at == other.enqueue_at
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
        // Reverse because [BinaryHeap] is a *max* heap, but we want pop() to return lowest `enqueue_at`.
        self.enqueue_at.cmp(&other.enqueue_at).reverse()
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
        recurring_message_sleep: Duration,
        received: Arc<Mutex<Vec<usize>>>,
    }

    impl Actor for TimedTestActor {
        type Context = TimedContext<Self::Message>;
        type Error = SendError;
        type Message = usize;

        fn name() -> &'static str {
            "TimedTestActor"
        }

        fn handle(&mut self, context: &mut Self::Context, message: usize) -> Result<(), SendError> {
            {
                let mut guard = self.received.lock().unwrap();
                guard.push(message);
            }

            // Messages 1 or 3 are endless self-sending ones, keep the loop spinning.
            if message == 1 || message == 3 {
                thread::sleep(Duration::from_millis(100));
                context.myself.send_now(3).unwrap();
            }

            // Message 2 is a recurring one, sleep based on a parameter.
            if message == 2 {
                thread::sleep(self.recurring_message_sleep);
            }

            Ok(())
        }
    }

    /// Tests that recurring messages still get in for actors that have one "tick" message type that
    /// does `block_for_some_time(); myself.send_now(Tick);` in its handle().
    #[test]
    fn recurring_messages_for_self_looping_actors() {
        let received = Arc::new(Mutex::new(Vec::new()));

        let mut system = System::new("timed test");
        let address = system
            .spawn(Timed::new(TimedTestActor {
                recurring_message_sleep: Duration::ZERO,
                received: Arc::clone(&received),
            }))
            .unwrap();
        address
            .send_recurring(
                || 2,
                Instant::now() + Duration::from_millis(50),
                Duration::from_millis(100),
            )
            .unwrap();

        address.send_now(1).unwrap();
        thread::sleep(Duration::from_millis(225));
        system.shutdown().unwrap();

        // The timeline (order of messages received) is:
        // at   0 ms: 1 (initial message, takes 100 ms to handle),
        // at 100 ms: 3 (first self-sent message, 100 ms to handle),
        // at 200 ms: 2 (first recurring scheduled message, delivered 150 ms late),
        // at 200 ms: 3 (second self-sent message, 100 ms to handle)
        // at 225 ms:   (control message to shut down the actor sent)
        // at 300 ms:   (control signal to shut down finally delivered to the actor)
        assert_eq!(*received.lock().unwrap(), vec![1, 3, 2, 3]);
    }

    /// Test that actors with recurring messages that take longer to handle than what the recurring
    /// delay is still get other and control messages.
    #[test]
    fn recurring_messages_handled_slower_than_generated() {
        let received = Arc::new(Mutex::new(Vec::new()));

        let mut system = System::new("timed test");
        let address = system
            .spawn(Timed::new(TimedTestActor {
                recurring_message_sleep: Duration::from_millis(100),
                received: Arc::clone(&received),
            }))
            .unwrap();
        address.send_recurring(|| 2, Instant::now(), Duration::from_millis(10)).unwrap();

        thread::sleep(Duration::from_millis(150));
        address.send_now(4).unwrap();
        thread::sleep(Duration::from_millis(125));
        system.shutdown().unwrap();

        // The timeline (order of messages received) is:
        // at   0 ms: 2 (first recurring message, 100 ms to handle)
        // at 100 ms: 2 (second recurring message, 90 ms late, 100 ms to handle)
        // at 150 ms:   (message "4" sent to the actor from the main thread)
        // at 200 ms: 4 (actor wakes up, processes message 4 that was sent before the recurring one)
        // at 200 ms: 2 (third recurring message, 180 ms late, 100 ms to handle)
        // at 275 ms:   (control message to shut down actor sent)
        // at 300 ms:   (control message to shut down received at highest priority)
        assert_eq!(*received.lock().unwrap(), vec![2, 2, 4, 2]);
    }
}
