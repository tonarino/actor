use anyhow::Error;
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
    type Error = Error;
    type Message = TimerMessage;

    fn name() -> &'static str {
        "TimerExampleActor"
    }

    fn started(&mut self, context: &mut Context<Self>) {
        context.receive_deadline = Some(self.started_at + Duration::from_millis(1500));
    }

    fn handle(
        &mut self,
        _context: &mut Context<Self>,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        println!("Got a message: {:?} at {:?}", message, self.started_at.elapsed());
        Ok(())
    }

    fn deadline_passed(&mut self, context: &mut Context<Self>, _deadline: Instant) {
        context.myself.send(TimerMessage::Periodic).unwrap();

        // A: Schedule one second from now (even if delayed); drifting tick.
        context.receive_deadline = Some(Instant::now() + Duration::from_secs(1));

        // B: Schedule one second from deadline; non-drifting tick.
        // context.receive_deadline = Some(deadline + Duration::from_secs(1));

        // C: Schedule one second from deadline, but don't fire multiple times if delayed.
        // context.receive_deadline = Some(max(deadline + Duration::from_secs(1), Instant::now()));
    }
}

fn main() -> Result<(), Error> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let mut system = System::new("Example Timer System");

    let timer_actor = TimerExampleActor::new();
    system.prepare(timer_actor).run_and_block()?;

    Ok(())
}
