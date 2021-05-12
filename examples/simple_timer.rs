use anyhow::Error;
use env_logger::Env;
use std::time::{Duration, Instant};
use tonari_actor::{Actor, Context, System};

#[derive(Debug)]
enum TimerMessage {
    Once,
    Periodic,
}

struct TimerExampleActor {
    started_at: Instant,
}

impl TimerExampleActor {
    pub fn new() -> Self {
        Self { started_at: Instant::now() }
    }
}

impl Actor for TimerExampleActor {
    type Error = Error;
    type Message = TimerMessage;

    fn name() -> &'static str {
        "TimerExampleActor"
    }

    fn started(&mut self, context: &mut Context<Self>) {
        context.send_once_to_self(Duration::from_millis(1500), TimerMessage::Once);

        context.send_recurring_to_self(Duration::from_secs(0), Duration::from_secs(1), || {
            TimerMessage::Periodic
        });
    }

    fn handle(
        &mut self,
        _context: &mut Context<Self>,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        println!("Got a message: {:?} at {:?}", message, self.started_at.elapsed());
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
