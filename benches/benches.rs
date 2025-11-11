use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::{hint::black_box, time::Duration};
use tonari_actor::{Actor, Addr, Context, System};

struct ChainLink {
    next: Addr<ChainLink>,
}

impl Actor for ChainLink {
    type Context = Context<Self::Message>;
    type Error = String;
    type Message = u64;

    fn handle(
        &mut self,
        context: &mut Self::Context,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        if message > 0 {
            self.next.send(message - 1).unwrap();
        } else {
            context.system_handle.shutdown().unwrap();
        }

        Ok(())
    }
}

fn make_chain(num_actors: usize) -> (System, Addr<ChainLink>, Addr<ChainLink>) {
    let mut system = System::new("chain");

    let addr = Addr::default();
    let mut next = addr.clone();
    for _ in 0..num_actors {
        next = system.spawn(ChainLink { next }).unwrap();
    }

    (system, next, addr)
}

fn run_chain((mut system, start, end): (System, Addr<ChainLink>, Addr<ChainLink>)) {
    start.send(1000).unwrap();
    system.prepare(ChainLink { next: start }).with_addr(end).run_and_block().unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    {
        let mut spawn = c.benchmark_group("spawn");
        spawn.throughput(Throughput::Elements(1));
        spawn.measurement_time(Duration::from_secs(10));
        spawn.bench_function("spawn 1", |b| b.iter(|| make_chain(black_box(1))));
        spawn.bench_function("spawn 10", |b| b.iter(|| make_chain(black_box(10))));
        spawn.bench_function("spawn 50", |b| b.iter(|| make_chain(black_box(50))));
    }

    {
        let mut circular = c.benchmark_group("circular");
        circular.measurement_time(Duration::from_secs(10));
        circular.throughput(Throughput::Elements(1000));
        circular.bench_function("circular (2 actors)", |b| {
            b.iter_batched(|| make_chain(1), run_chain, BatchSize::SmallInput)
        });
        circular.bench_function("circular (native CPU count - 1)", |b| {
            b.iter_batched(|| make_chain(num_cpus::get() - 2), run_chain, BatchSize::SmallInput)
        });
        circular.bench_function("circular (native CPU count)", |b| {
            b.iter_batched(|| make_chain(num_cpus::get() - 1), run_chain, BatchSize::SmallInput)
        });
        circular.bench_function("circular (50 actors)", |b| {
            b.iter_batched(|| make_chain(49), run_chain, BatchSize::SmallInput)
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
