use anyhow::Error;
use env_logger::Env;
use std::time::{Duration, Instant};
use tonari_actor::{Actor, Context, System};

#[derive(Debug)]
enum PublisherMessage {
    Periodic,
    Text(String),
}

impl From<String> for PublisherMessage {
    fn from(text: String) -> Self {
        PublisherMessage::Text(text)
    }
}

struct PublisherActor {
    counter: usize,
    started_at: Instant,
}

impl PublisherActor {
    pub fn new() -> Self {
        Self { counter: 0, started_at: Instant::now() }
    }
}

impl Actor for PublisherActor {
    type Context = Context<Self::Message>;
    type Error = Error;
    type Message = PublisherMessage;

    fn name() -> &'static str {
        "PublisherActor"
    }

    fn started(&mut self, context: &mut Self::Context) {
        context.set_deadline(Some(self.started_at + Duration::from_millis(1500)));
        context.subscribe::<String>();
    }

    fn handle(
        &mut self,
        context: &mut Self::Context,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        match message {
            PublisherMessage::Periodic => {
                println!(
                    "Got a periodic message: {:?} at {:?}",
                    message,
                    self.started_at.elapsed()
                );

                let text = format!("Hello from PublisherActor - counter = {}", self.counter);
                self.counter += 1;

                context.system_handle.publish(text);
            },
            PublisherMessage::Text(text) => {
                println!("PublisherActor got a text message: {:?}", text);
            },
        }

        context.system_handle.publish(());
        Ok(())
    }

    fn deadline_passed(
        &mut self,
        context: &mut Self::Context,
        deadline: Instant,
    ) -> Result<(), Error> {
        context.myself.send(PublisherMessage::Periodic)?;
        context.set_deadline(Some(deadline + Duration::from_secs(1)));
        Ok(())
    }
}

enum SubscriberMessage {
    Text(String),
}

impl From<String> for SubscriberMessage {
    fn from(text: String) -> Self {
        SubscriberMessage::Text(text)
    }
}

struct SubscriberActor1;
struct SubscriberActor2;

impl Actor for SubscriberActor1 {
    type Context = Context<Self::Message>;
    type Error = Error;
    type Message = SubscriberMessage;

    fn name() -> &'static str {
        "SubscriberActor1"
    }

    fn started(&mut self, context: &mut Self::Context) {
        context.subscribe::<String>();
    }

    fn handle(
        &mut self,
        _context: &mut Self::Context,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        match message {
            SubscriberMessage::Text(text) => {
                println!("SubscriberActor1 got a text message: {:?}", text);
            },
        }

        Ok(())
    }
}

impl Actor for SubscriberActor2 {
    type Context = Context<Self::Message>;
    type Error = Error;
    type Message = SubscriberMessage;

    fn name() -> &'static str {
        "SubscriberActor1"
    }

    fn started(&mut self, context: &mut Self::Context) {
        context.subscribe::<String>();
    }

    fn handle(
        &mut self,
        _context: &mut Self::Context,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        match message {
            SubscriberMessage::Text(text) => {
                println!("SubscriberActor2 got a text message: {:?}", text);
            },
        }

        Ok(())
    }
}

fn main() -> Result<(), Error> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let mut system = System::new("Example PubSub System");

    let publisher_actor = PublisherActor::new();
    let _ = system.prepare(publisher_actor).spawn()?;
    let _ = system.prepare(SubscriberActor1).spawn()?;
    let _ = system.prepare(SubscriberActor2).spawn()?;

    system.publish("Hello from the main thread!".to_string());

    system.run()?;

    Ok(())
}
