use anyhow::Error;
use criterion::{criterion_group, criterion_main, Criterion};
use std::time::{Duration, Instant};
use tonari_actor::{Actor, Context, Event, Recipient, System};

#[derive(Debug, Clone)]
struct StringEvent;

impl Event for StringEvent {}

enum PublisherMessage {
    SubscriberStarted,
    PublishEvents,
}

#[derive(Clone)]
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
                    context.system_handle.publish(StringEvent)?;
                }
                let elapsed = start.elapsed();

                self.result_sender.try_send(elapsed)?;
            },
        }

        Ok(())
    }
}

struct SubscriberActor {
    publisher_addrs: Vec<Recipient<PublisherMessage>>,
}

impl SubscriberActor {
    fn new(publisher_addrs: Vec<Recipient<PublisherMessage>>) -> Self {
        Self { publisher_addrs }
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
        for publisher_addr in self.publisher_addrs.iter() {
            publisher_addr.send(PublisherMessage::SubscriberStarted).unwrap();
        }
    }

    fn handle(
        &mut self,
        _context: &mut Self::Context,
        _message: Self::Message,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn run_pubsub_system(publishers: usize, subscribers: usize, iterations: u64) -> Duration {
    let mut system = System::new("pub sub bench");
    let (result_sender, result_receiver) = flume::bounded(publishers);

    // Divide work equally. We assume that number of iterations is high and rounding doesn't matter.
    let per_publisher_iterations = iterations / publishers as u64;
    let publisher_actors = vec![
        PublisherActor {
            subscriber_count: subscribers,
            iterations: per_publisher_iterations,
            result_sender
        };
        publishers
    ];
    let publisher_addrs: Vec<Recipient<PublisherMessage>> = publisher_actors
        .into_iter()
        .map(|actor| system.prepare(actor).with_capacity(subscribers).spawn().unwrap().recipient())
        .collect();

    for _i in 0..subscribers {
        system
            .prepare(SubscriberActor::new(publisher_addrs.clone()))
            .with_capacity(iterations as usize)
            .spawn()
            .unwrap();
    }

    let mut duration_sum = Duration::default();
    for _in in 0..publishers {
        // This waits for all publishers to finish their work.
        duration_sum += result_receiver.recv_timeout(Duration::from_secs(60)).unwrap();
    }

    system.shutdown().unwrap();

    duration_sum
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish");
    group.throughput(criterion::Throughput::Elements(1));

    for subscribers in [10, 100] {
        for publishers in [1, 2, 10] {
            // iter_custom() is explicitly recommended for multi-threaded systems.
            group.bench_function(
                format!("{publishers} publishers {subscribers} subscribers"),
                |b| {
                    b.iter_custom(|iterations| {
                        run_pubsub_system(publishers, subscribers, iterations)
                    })
                },
            );
        }
    }
}

criterion_group!(pub_sub, criterion_benchmark);
criterion_main!(pub_sub);
