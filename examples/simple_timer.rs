use anyhow::{Error, Result};
use env_logger::Env;
use std::time::{Duration, Instant};
use tonari_actor::{Actor, Context, System};

#[derive(Debug)]
enum TimerMessage {
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
    type Context = Context<Self::Message>;
    type Error = Error;
    type Message = TimerMessage;

    fn started(&mut self, context: &mut Self::Context) -> Result<()> {
        context.set_deadline(Some(self.started_at + Duration::from_millis(1500)));
        Ok(())
    }

    fn handle(&mut self, _context: &mut Self::Context, message: Self::Message) -> Result<()> {
        println!("Got a message: {:?} at {:?}", message, self.started_at.elapsed());
        Ok(())
    }

    fn deadline_passed(&mut self, context: &mut Self::Context, deadline: Instant) -> Result<()> {
        context.myself.send(TimerMessage::Periodic)?;
        context.set_deadline(Some(deadline + Duration::from_secs(1)));
        Ok(())
    }
}

fn main() -> Result<(), Error> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let mut system = System::new("Example Timer System");

    let timer_actor = TimerExampleActor::new();
    system.prepare(timer_actor).with_default_capacity().run_and_block()?;

    Ok(())
}
