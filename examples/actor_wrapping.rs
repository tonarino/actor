use anyhow::Error;
use env_logger::Env;
use log::debug;
use std::time::{Duration, Instant};
use tonari_actor::{Actor, Context, System};

/// An actor that wraps any other actor and adds some debug logging around its calls.
struct LoggingAdapter<A> {
    inner: A,
}

impl<A: Actor> Actor for LoggingAdapter<A> {
    type Error = A::Error;
    type Message = A::Message;

    fn handle(
        &mut self,
        context: &mut Context<Self::Message>,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        debug!("LoggingAdapter: handle()");
        self.inner.handle(context, message)
    }

    fn name() -> &'static str {
        A::name()
    }

    fn started(&mut self, context: &mut Context<Self::Message>) {
        debug!("LoggingAdapter: started()");
        self.inner.started(context)
    }

    fn stopped(&mut self, context: &mut Context<Self::Message>) {
        debug!("LoggingAdapter: stopped()");
        self.inner.stopped(context)
    }

    fn deadline_passed(&mut self, context: &mut Context<Self::Message>, deadline: Instant) {
        debug!("LoggingAdapter: deadline_passed()");
        self.inner.deadline_passed(context, deadline)
    }
}

struct TestActor {}

impl Actor for TestActor {
    type Error = Error;
    type Message = String;

    fn handle(&mut self, context: &mut Context<String>, message: String) -> Result<(), Error> {
        println!("Got a message: {}. Shuting down.", message);
        context.system_handle.shutdown().map_err(Error::from)
    }

    fn name() -> &'static str {
        "TestActor"
    }

    fn started(&mut self, context: &mut Context<String>) {
        context.set_timeout(Some(Duration::from_millis(100)))
    }

    fn deadline_passed(&mut self, context: &mut Context<String>, deadline: Instant) {
        context.myself.send(format!("deadline was {:?}", deadline)).unwrap();
    }
}

fn main() -> Result<(), Error> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let mut system = System::new("Actor Wrapping Example");

    let actor = LoggingAdapter { inner: TestActor {} };
    system.prepare(actor).run_and_block()?;

    Ok(())
}
