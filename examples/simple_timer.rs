use anyhow::Error;
use env_logger::Env;
use std::time::Duration;
use tonari_actor::{Actor, Context, System};

#[derive(Debug)]
enum TimerMessage {
    Once,
    Periodic,
}

struct TimerExampleActor {}

impl TimerExampleActor {
    pub fn new() -> Self {
        Self {}
    }
}

impl Actor for TimerExampleActor {
    type Error = Error;
    type Message = TimerMessage;

    fn name() -> &'static str {
        "TimerExampleActor"
    }

    fn started(&mut self, context: &mut Context<Self>) {
        let myself = context.myself.clone();

        context.run_once(Duration::from_millis(1500), move |_| {
            if let Err(e) = myself.send(TimerMessage::Once) {
                println!("Error sending TimerMessage::Once message: {}", e);
            }
        });

        let myself = context.myself.clone();

        context.run_periodically(Duration::from_secs(0), Duration::from_secs(1), move |_| {
            if let Err(e) = myself.send(TimerMessage::Periodic) {
                println!("Error sending TimerMessage::Periodic message: {}", e);
            }

            tonari_actor::TimerControlFlow::Continue
        });
    }

    fn handle(
        &mut self,
        _context: &mut Context<Self>,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        println!("Got a message: {:?}", message);
        Ok(())
    }
}

fn main() -> Result<(), Error> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let mut system = System::new("Example Timer System");

    let timer_actor = TimerExampleActor::new();
    system.prepare(timer_actor).run_and_block()?;

    Ok(())
}
