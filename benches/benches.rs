use actor::{Actor, Addr, Context, System};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};

struct ChainLink {
    next: Option<Addr<ChainLink>>,
}

impl Actor for ChainLink {
    type Error = ();
    type Message = bool;

    fn name() -> &'static str {
        "ChainLink"
    }

    fn handle(
        &mut self,
        context: &Context<Self>,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        if let Some(ref next) = self.next {
            next.send(message).unwrap();
        } else if message {
            context.system_handle.shutdown().unwrap();
        }

        Ok(())
    }
}

fn make_chain(num_actors: usize) -> (System, Addr<ChainLink>, Addr<ChainLink>) {
    let mut system = System::new("chain");

    let addr = Addr::default();
    let mut next = Some(addr.clone());
    for _ in 0..num_actors {
        next = Some(system.spawn(ChainLink { next }).unwrap());
    }

    (system, next.unwrap(), addr)
}

fn run_chain((mut system, start, end): (System, Addr<ChainLink>, Addr<ChainLink>)) {
    start.send(true).unwrap();
    system.run_on_main(ChainLink { next: None }, end).unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("chain");
    group.throughput(Throughput::Elements(1));
    group.bench_function("spawn 1", |b| b.iter(|| make_chain(black_box(1))));
    group.bench_function("spawn 10", |b| b.iter(|| make_chain(black_box(10))));
    group.bench_function("spawn 50", |b| b.iter(|| make_chain(black_box(50))));
    group.bench_function("chain 1", |b| {
        b.iter_batched(|| make_chain(1), run_chain, BatchSize::SmallInput)
    });
    group.bench_function("chain 10", |b| {
        b.iter_batched(|| make_chain(10), run_chain, BatchSize::SmallInput)
    });
    group.bench_function("chain 50", |b| {
        b.iter_batched(|| make_chain(50), run_chain, BatchSize::SmallInput)
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
