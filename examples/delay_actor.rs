use anyhow::Error;
use env_logger::Env;
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    time::{Duration, Instant},
};
use tonari_actor::{Actor, Context, Recipient, SendError, System};

/// Something that can be sent. A message with a recipient.
trait Envelope {
    fn send(self: Box<Self>) -> Result<(), SendError>;
}

impl<M> Envelope for (Recipient<M>, M) {
    fn send(self: Box<Self>) -> Result<(), SendError> {
        self.0.send(self.1)
    }
}

struct DelayedMessage {
    fire_at: Instant,
    message: Box<dyn Envelope + Send>,
}

impl DelayedMessage {
    fn new<M: Send + 'static>(fire_at: Instant, recipient: &Recipient<M>, message: M) -> Self {
        Self { fire_at, message: Box::new((recipient.clone(), message)) }
    }
}

impl PartialEq for DelayedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.fire_at == other.fire_at
    }
}

// We cannot derive because that would add too strict bounds.
impl Eq for DelayedMessage {}

impl PartialOrd for DelayedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Reverse because [BinaryHeap] is a *max* heap, but we want pop() to return lowest `fire_at`.
        Some(self.fire_at.cmp(&other.fire_at).reverse())
    }
}

impl Ord for DelayedMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("we can always compare")
    }
}

/// A simple actor to send messages with delay. Possible extensions:
/// * Support recurring messages.
/// * Support cancellation.
/// * Implement some `DelayActorExt` trait on `Recipient<DelayActor>` for caller convenience.
struct DelayActor {
    queue: BinaryHeap<DelayedMessage>,
}

impl DelayActor {
    fn new() -> Self {
        Self { queue: Default::default() }
    }

    fn schedule_timeout(&self, context: &mut Context<DelayedMessage>) {
        // Schedule next timeout if the queue is not empty.
        context.set_deadline(self.queue.peek().map(|earliest| earliest.fire_at));
    }
}

impl Actor for DelayActor {
    type Context = Context<Self::Message>;
    type Error = Error;
    type Message = DelayedMessage;

    fn name() -> &'static str {
        "DelayActor"
    }

    fn handle(
        &mut self,
        context: &mut Self::Context,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        self.queue.push(message);
        self.schedule_timeout(context);
        Ok(())
    }

    fn deadline_passed(
        &mut self,
        context: &mut Self::Context,
        _deadline: Instant,
    ) -> Result<(), Error> {
        // Send all messages that should have been sent by now.
        let now = Instant::now();
        while self.queue.peek().map(|m| m.fire_at <= now).unwrap_or(false) {
            let message = self.queue.pop().expect("heap is non-empty, we have just peeked");
            message.message.send()?;
        }

        self.schedule_timeout(context);
        Ok(())
    }
}

struct FinalConsumer {
    started_at: Instant,
}

impl Actor for FinalConsumer {
    type Context = Context<Self::Message>;
    type Error = Error;
    type Message = String;

    fn name() -> &'static str {
        "FinalConsumer"
    }

    fn handle(&mut self, context: &mut Self::Context, message: String) -> Result<(), Error> {
        println!("Got a message: {:?} at {:?}", message, self.started_at.elapsed());
        if message == "last" {
            context.system_handle.shutdown()?;
        }
        Ok(())
    }
}

fn main() -> Result<(), Error> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let mut system = System::new("Example Timer System");

    let delay_actor = system.spawn(DelayActor::new())?;

    let consumer = system.spawn(FinalConsumer { started_at: Instant::now() })?.recipient();

    let now = Instant::now();
    delay_actor.send(DelayedMessage::new(now + Duration::from_secs(3), &consumer, "last"))?;
    delay_actor.send(DelayedMessage::new(
        now + Duration::from_secs(4),
        &consumer,
        "never received",
    ))?;
    delay_actor.send(DelayedMessage::new(now + Duration::from_secs(2), &consumer, "second"))?;
    delay_actor.send(DelayedMessage::new(now + Duration::from_secs(1), &consumer, "first"))?;

    system.run()?;

    Ok(())
}
