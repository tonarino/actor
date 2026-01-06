use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use flume::Receiver;
use std::hint::black_box;
use tonari_actor::{Actor, Addr, Context, System};

struct ChainLink {
    next: Addr<u64>,
    finished: flume::Sender<()>,
}

impl Actor for ChainLink {
    type Context = Context<Self::Message>;
    type Error = String;
    type Message = u64;

    fn handle(
        &mut self,
        _context: &mut Self::Context,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        if message > 0 {
            self.next.send(message - 1).unwrap();
        } else {
            self.finished.try_send(()).unwrap();
        }

        Ok(())
    }
}

fn make_chain(num_actors: usize) -> (System, Addr<u64>, Receiver<()>) {
    let mut system = System::new("chain");
    let (finish_sender, finish_receiver) = flume::bounded(1);

    let first = ChainLink::addr();
    let mut previous = first.clone();

    // Spawn all actors except the first one.
    for _ in 1..num_actors {
        previous =
            system.spawn(ChainLink { next: previous, finished: finish_sender.clone() }).unwrap();
    }

    system
        .prepare(ChainLink { next: previous, finished: finish_sender })
        .with_addr(first.clone())
        .spawn()
        .unwrap();

    (system, first, finish_receiver)
}

fn spawn_bench(c: &mut Criterion) {
    // The benchmark may fail at runtime if we i.e. spawn too many async runtimes. The failures
    // look mysterious without the logs. Logging may be already ini by another bench.
    env_logger::try_init().ok();

    let mut spawn = c.benchmark_group("spawn");

    for num_actors in [1, 10, 50] {
        spawn.throughput(Throughput::Elements(num_actors));

        spawn.bench_function(format!("spawn & tear down {num_actors}"), |b| {
            b.iter(|| make_chain(black_box(num_actors as usize)))
        });

        #[cfg(feature = "async")]
        spawn.bench_function(format!("spawn & tear down {num_actors} async"), |b| {
            b.iter(|| async_benches::make_chain(black_box(num_actors as usize)))
        });
    }
}

fn run_chain((system, first, finish_receiver): (System, Addr<u64>, Receiver<()>)) -> System {
    first.send(1000).unwrap();
    finish_receiver.recv().unwrap();

    // Pass the system out. That way its destructor that joins actor threads is not timed.
    system
}

fn circular_bench(c: &mut Criterion) {
    // The benchmark may fail at runtime if we i.e. spawn too many async runtimes. The failures
    // look mysterious without the logs. Logging may be already ini by another bench.
    env_logger::try_init().ok();

    let mut circular = c.benchmark_group("circular");
    circular.throughput(Throughput::Elements(1000));

    let num_cpus = num_cpus::get();
    for num_actors in [1, 2, 3, num_cpus - 1, num_cpus, 50] {
        circular.bench_function(format!("circular ({num_actors} actors)"), |b| {
            b.iter_batched(|| make_chain(num_actors), run_chain, BatchSize::LargeInput)
        });

        // We need to use `LargeInput` not to spawn hundreds of thousands of async runtimes.
        // Use that for the sync benchmark too for fairness.
        #[cfg(feature = "async")]
        circular.bench_function(format!("circular ({num_actors} actors) async"), |b| {
            b.iter_batched(
                || async_benches::make_chain(num_actors),
                run_chain,
                BatchSize::LargeInput,
            )
        });
    }
}

criterion_group!(benches, spawn_bench, circular_bench);
criterion_main!(benches);

#[cfg(feature = "async")]
mod async_benches {
    use crate::ChainLink;
    use flume::Receiver;
    use tonari_actor::{Addr, BareContext, System, r#async::AsyncActor};

    impl AsyncActor for ChainLink {
        type Error = String;
        type Message = u64;

        async fn handle(
            &mut self,
            _context: &BareContext<Self::Message>,
            message: Self::Message,
        ) -> Result<(), Self::Error> {
            if message > 0 {
                self.next.send(message - 1).unwrap();
            } else {
                self.finished.try_send(()).unwrap();
            }

            Ok(())
        }
    }

    pub(super) fn make_chain(num_actors: usize) -> (System, Addr<u64>, Receiver<()>) {
        let mut system = System::new("chain");
        let (finish_sender, finish_receiver) = flume::bounded(1);

        let first = ChainLink::addr();
        let mut previous = first.clone();

        // Spawn all actors except the first one.
        for _ in 1..num_actors {
            previous = system
                .spawn_async(ChainLink { next: previous, finished: finish_sender.clone() })
                .unwrap();
        }

        system
            .prepare_async(ChainLink { next: previous, finished: finish_sender })
            .with_addr(first.clone())
            .spawn()
            .unwrap();

        (system, first, finish_receiver)
    }
}
