use anyhow::{bail, Error};
use criterion::{criterion_group, criterion_main, Criterion};
use std::time::{Duration, Instant};
use tonari_actor::{Actor, Addr, Context, Event, System};

#[derive(Debug, Clone)]
struct StringEvent(String);

impl Event for StringEvent {}

enum PublisherMessage {
    SubscriberStarted,
    PublishEvents,
}

struct PublisherActor {
    subscriber_count: usize,
    iterations: u64,
    result_sender: flume::Sender<Duration>,
}

impl Actor for PublisherActor {
    type Context = Context<Self::Message>;
    type Error = Error;
    type Message = PublisherMessage;

    fn name() -> &'static str {
        "PublisherActor"
    }

    fn handle(
        &mut self,
        context: &mut Self::Context,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        match message {
            PublisherMessage::SubscriberStarted => {
                self.subscriber_count = self.subscriber_count.checked_sub(1).unwrap();
                if self.subscriber_count == 0 {
                    // Start publishing.
                    context.myself.send(PublisherMessage::PublishEvents)?;
                }
            },
            PublisherMessage::PublishEvents => {
                let start = Instant::now();
                for _i in 0..self.iterations {
                    context.system_handle.publish(StringEvent("hello".to_string()));
                }
                let elapsed = start.elapsed();

                self.result_sender.try_send(elapsed)?;
                bail!("Our work is done.");
            },
        }

        Ok(())
    }
}

struct SubscriberActor {
    publisher_addr: Addr<PublisherActor>,
}

impl SubscriberActor {
    fn new(publisher_addr: Addr<PublisherActor>) -> Self {
        Self { publisher_addr }
    }
}

impl Actor for SubscriberActor {
    type Context = Context<Self::Message>;
    type Error = Error;
    type Message = StringEvent;

    fn name() -> &'static str {
        "SubscriberActor"
    }

    fn started(&mut self, context: &mut Self::Context) {
        context.subscribe::<StringEvent>();
        self.publisher_addr.send(PublisherMessage::SubscriberStarted).unwrap();
    }

    fn handle(
        &mut self,
        _context: &mut Self::Context,
        _message: Self::Message,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn run_pubsub_system(iterations: u64) -> Duration {
    let subscriber_count = 1;
    let mut system = System::new("pub sub bench");
    let (result_sender, result_receiver) = flume::bounded(1);

    let publisher_actor = PublisherActor { subscriber_count, iterations, result_sender };
    let publisher_addr = system.spawn(publisher_actor).unwrap();
    system
        .prepare(SubscriberActor::new(publisher_addr))
        .with_capacity(iterations as usize)
        .spawn()
        .unwrap();

    system.run().unwrap();

    result_receiver.try_recv().unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish");
    group.throughput(criterion::Throughput::Elements(1));

    // iter_custom() is explicitly recommended for threaded systems when measured code may live
    // in a different thread.
    group.bench_function("1 publisher 1 consumer", |b| b.iter_custom(run_pubsub_system));
}

criterion_group!(pub_sub, criterion_benchmark);
criterion_main!(pub_sub);
