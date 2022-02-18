use anyhow::Error;
use env_logger::Env;
use std::time::{Duration, Instant};
use tonari_actor::{
    timed::{RecipientExt, Timed, TimedContext},
    Actor, System,
};

struct FinalConsumer {
    started_at: Instant,
}

impl Actor for FinalConsumer {
    type Context = TimedContext<Self::Message>;
    type Error = Error;
    type Message = String;

    const DEFAULT_CAPACITY_NORMAL: usize = 6;

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

    let consumer = system.spawn(Timed::new(FinalConsumer { started_at: Instant::now() }))?;

    let now = Instant::now();
    consumer.send_recurring(
        || "recurring".to_string(),
        now + Duration::from_millis(500),
        Duration::from_secs(1),
    )?;
    consumer.send_delayed("last".to_string(), Duration::from_secs(3))?;
    consumer.send_delayed("never received".to_string(), Duration::from_secs(4))?;
    consumer.send_timed("second".to_string(), now + Duration::from_secs(2))?;
    consumer.send_timed("first".to_string(), now + Duration::from_secs(1))?;

    // `impl<M> From<M> for TimedMessage<M>` allows us to send original message type to the wrapped actor.
    let string_recipient = consumer.recipient();
    string_recipient.send("string".to_string())?;

    // We can chain .recipient() calls to further convert message type that is accepted.
    let str_reference_recipient = string_recipient.recipient();
    str_reference_recipient.send("str reference")?;

    system.run()?;

    Ok(())
}
