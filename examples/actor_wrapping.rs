use anyhow::{Error, Result};
use env_logger::Env;
use log::debug;
use std::time::{Duration, Instant};
use tonari_actor::{Actor, Context, Priority, System};

/// An actor that wraps any other actor and adds some debug logging around its calls.
struct LoggingAdapter<A> {
    inner: A,
}

impl<A: Actor> Actor for LoggingAdapter<A> {
    type Context = A::Context;
    type Error = A::Error;
    type Message = A::Message;

    const DEFAULT_CAPACITY_HIGH: usize = A::DEFAULT_CAPACITY_HIGH;
    const DEFAULT_CAPACITY_NORMAL: usize = A::DEFAULT_CAPACITY_NORMAL;

    fn handle(
        &mut self,
        context: &mut Self::Context,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        debug!("LoggingAdapter: handle()");
        self.inner.handle(context, message)
    }

    fn name() -> &'static str {
        A::name()
    }

    fn priority(message: &Self::Message) -> Priority {
        A::priority(message)
    }

    fn started(&mut self, context: &mut Self::Context) -> Result<(), Self::Error> {
        debug!("LoggingAdapter: started()");
        self.inner.started(context)
    }

    fn stopped(&mut self, context: &mut Self::Context) -> Result<(), Self::Error> {
        debug!("LoggingAdapter: stopped()");
        self.inner.stopped(context)
    }

    fn deadline_passed(
        &mut self,
        context: &mut Self::Context,
        deadline: Instant,
    ) -> Result<(), Self::Error> {
        debug!("LoggingAdapter: deadline_passed()");
        self.inner.deadline_passed(context, deadline)
    }
}

struct TestActor {}

impl Actor for TestActor {
    type Context = Context<Self::Message>;
    type Error = Error;
    type Message = String;

    fn handle(&mut self, context: &mut Self::Context, message: String) -> Result<()> {
        println!("Got a message: {message}. Shuting down.");
        context.system_handle.shutdown().map_err(Error::from)
    }

    fn started(&mut self, context: &mut Self::Context) -> Result<()> {
        context.set_timeout(Some(Duration::from_millis(100)));
        Ok(())
    }

    fn deadline_passed(&mut self, context: &mut Self::Context, deadline: Instant) -> Result<()> {
        context.myself.send(format!("deadline was {deadline:?}")).map_err(Error::from)
    }
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let mut system = System::new("Actor Wrapping Example");

    let actor = LoggingAdapter { inner: TestActor {} };
    system.prepare(actor).with_default_capacity().run_and_block()?;

    Ok(())
}
