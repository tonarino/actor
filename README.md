# tonari-actor

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]

[crates-badge]: https://img.shields.io/crates/v/tonari-actor.svg
[crates-url]: https://crates.io/crates/tonari-actor
[docs-badge]: https://docs.rs/tonari-actor/badge.svg
[docs-url]: https://docs.rs/tonari-actor

This crate aims to provide a minimalist and high-performance actor framework
for Rust with significantly less complexity than other frameworks like
[Actix](https://docs.rs/actix/).

In this framework, each `Actor` is its own OS-level thread. This makes debugging
noticeably simpler, and is suitably performant when the number of actors
is less than or equal to the number of CPU threads.

# Example
```rust
use tonari_actor::{Actor, Context, System};

struct TestActor {}

impl Actor for TestActor {
    type Error = ();
    type Message = usize;

    fn name() -> &'static str {
        "TestActor"
    }

    fn handle(&mut self, _context: &Context<Self>, message: Self::Message) -> Result<(), ()> {
        println!("message: {}", message);

        Ok(())
    }
}

fn main() {
    let mut system = System::new("default");

    // will spin up a new thread running this actor
    let addr = system.spawn(TestActor {}).unwrap();

    // send messages to actors to spin off work...
    addr.send(1usize).unwrap();

    // ask the actors to finish and join the threads.
    system.shutdown().unwrap();
}
```

## Dependencies
- cargo
- rustc

## Build

```
$ cargo build --release
```

## Testing

```
$ cargo test
```

## Code Format

The formatting options currently use nightly-only options.

```
$ cargo +nightly fmt
```

## Code Linting

```
$ cargo clippy
```
