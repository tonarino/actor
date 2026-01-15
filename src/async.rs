//! # Async Actors
//!
//! `tonari-actor` lets you freely combine sync (blocking) and async actors within one system.
//! Available under the `async` crate feature (disabled by default).
//!
//! While sync actors implement the [`Actor`](crate::Actor) trait and are spawned using the
//! [`System::spawn()`], [`System::prepare()`] and [`System::prepare_fn()`] family of methods,
//! async actors implement [`AsyncActor`] and are spawned using [`System::spawn_async()`],
//! [`System::prepare_async()`] and [`System::prepare_async_factory()`].
//!
//! Sync and async actors share the same [`Addr`] and [`Recipient`](crate::Recipient) types.
//!
//! Async actors share the same paradigm as sync actors: each one gets its own OS-level thread.
//! More specifically a single-threaded async runtime is spawned for every async actor.
//!
//! `tonari-actor` currently uses the [`tokio`][^tokio] ecosystem, more specifically its
//! [`LocalRuntime`]. It allows spawning futures that are _not_ [`Send`], which means you
//! can use [`Rc`](https://doc.rust-lang.org/std/rc/struct.Rc.html) and
//! [`RefCell`](https://doc.rust-lang.org/std/cell/struct.RefCell.html) instead of
//! [`Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) and mutexes in your futures.
//! It also allows the [`AsyncActor`] trait (and the implementors) to use the `async fn` syntax.
//!
//! Tokio's [`LocalRuntime`] is currently [gated behind the `tokio_unstable` _Rust
//! flag_](https://docs.rs/tokio/latest/tokio/index.html#unstable-features). Note that this isn't
//! a cargo feature flag (that would go to `Cargo.toml`); it goes to `.cargo/config.toml` or
//! `RUSTFLAGS`, which override the former. It needs to be specified in our leaf project/workspace
//! (it doesn't propagate from `tonari-actor`). Stabilization of [`LocalRuntime`] is tracked in
//! [tokio-rs/tokio#7558](https://github.com/tokio-rs/tokio/issues/7558).
//!
//! With [`AsyncActor::handle()`] being an `async fn`, you gain an access to the wide async library
//! ecosystem (currently those compatible with [`tokio`]), and you can employ concurrency (still
//! within the single thread) when processing each message by using the various future combinators.
//!
//! But the incoming messages are still processed sequentially (the actor framework won't start
//! multiple concurrent [`AsyncActor::handle()`] futures of a given actor). If you want to process
//! the _messages_ concurrently, spawn an async task to handle the message and return from the
//! `handle()` method immediately so new messages can arrive for processing.
//!
//! Async tasks can be spawned using [`tokio::task::spawn_local()`], or [`tokio::spawn()`] if the
//! [`Send`] bound of the latter doesn't limit you.
//!
//! Note that an async equivalent of [`crate::SpawnBuilderWithAddress::run_and_block()`] is not
//! currently implemented (contributions welcome).
//!
//! [^tokio]: on a logical level, `tonari-actor` isn't tied to any specific async runtime (it
//!     doesn't do any runtime-specific operations like I/O or timers), it just needs to spawn
//!     _some_ async runtime in the actor loop. Tokio was just a pragmatic choice that many crates
//!     in the ecosystem use. We could add support for alternative ones, even runtime-configurable.

use crate::{
    ActorError, Addr, BareContext, Capacity, Control, Priority, RegistryEntry, System, SystemHandle,
};
use futures_util::{StreamExt, select_biased};
use log::{debug, trace};
use std::{any::type_name, fmt, future, thread};
use tokio::runtime::LocalRuntime;

/// The actor trait - async variant.
// Ad. the #[allow]: using `async fn` in a trait doesn't allow us to specify `Send` (or other)
// bounds, but we don't really need any bounds, because we use [`LocalRuntime`].
#[allow(async_fn_in_trait)]
pub trait AsyncActor {
    /// The expected type of a message to be received.
    type Message: Send + 'static;
    /// The type to return on error in the handle method.
    type Error: fmt::Display;

    /// Default capacity of actor's normal-priority inbox unless overridden by `.with_capacity()`.
    const DEFAULT_CAPACITY_NORMAL: usize = 5;
    /// Default capacity of actor's high-priority inbox unless overridden by `.with_capacity()`.
    const DEFAULT_CAPACITY_HIGH: usize = 5;

    /// The name of the Actor. Used only for logging/debugging.
    /// Default implementation uses [`type_name()`].
    fn name() -> &'static str {
        type_name::<Self>()
    }

    /// Determine priority of a `message` before it is sent to this actor.
    /// Default implementation returns [`Priority::Normal`].
    fn priority(_message: &Self::Message) -> Priority {
        Priority::Normal
    }

    /// An optional callback when the Actor has been started.
    async fn started(&mut self, _context: &BareContext<Self::Message>) -> Result<(), Self::Error> {
        Ok(())
    }

    /// The primary function of this trait, allowing an actor to handle incoming messages of a certain type.
    async fn handle(
        &mut self,
        context: &BareContext<Self::Message>,
        message: Self::Message,
    ) -> Result<(), Self::Error>;

    /// An optional callback when the Actor has been stopped.
    async fn stopped(&mut self, _context: &BareContext<Self::Message>) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Create address for this actor with default capacities.
    fn addr() -> Addr<Self::Message> {
        let capacity =
            Capacity { normal: Self::DEFAULT_CAPACITY_NORMAL, high: Self::DEFAULT_CAPACITY_HIGH };
        Self::addr_with_capacity(capacity)
    }

    /// Create address for this actor, specifying its inbox size. Accepts [`Capacity`] or [`usize`].
    fn addr_with_capacity(capacity: impl Into<Capacity>) -> Addr<Self::Message> {
        Addr::new(capacity, Self::name(), Self::priority)
    }
}

/// A builder for configuring [`AsyncActor`] spawning.
/// You can specify your own [`Addr`] for the Actor, or let the system create
/// a new address with either provided or default capacity.
#[must_use = "You must call .with_addr(), .with_capacity(), or .with_default_capacity() to \
              configure this builder"]
pub struct AsyncSpawnBuilderWithoutAddress<'a, A: AsyncActor, F: IntoFuture<Output = A>> {
    system: &'a mut System,
    factory: F,
}

impl<'a, A: AsyncActor, F: IntoFuture<Output = A>> AsyncSpawnBuilderWithoutAddress<'a, A, F> {
    /// Specify an existing [`Addr`] to use with this Actor.
    pub fn with_addr(self, addr: Addr<A::Message>) -> AsyncSpawnBuilderWithAddress<'a, A, F> {
        AsyncSpawnBuilderWithAddress { spawn_builder: self, addr }
    }

    /// Specify a capacity for the actor's receiving channel. Accepts [`Capacity`] or [`usize`].
    pub fn with_capacity(
        self,
        capacity: impl Into<Capacity>,
    ) -> AsyncSpawnBuilderWithAddress<'a, A, F> {
        let addr = A::addr_with_capacity(capacity);
        AsyncSpawnBuilderWithAddress { spawn_builder: self, addr }
    }

    /// Use the default capacity for the actor's receiving channel.
    pub fn with_default_capacity(self) -> AsyncSpawnBuilderWithAddress<'a, A, F> {
        let addr = A::addr();
        AsyncSpawnBuilderWithAddress { spawn_builder: self, addr }
    }
}

/// After having configured the builder with an address
/// it is possible to create and run the actor on a new thread with [`Self::spawn()`].
///
/// Not yet implemented for async actors: `run_and_block()` on the current thread.
/// File an issue if you need it.
#[must_use = "You must call .spawn() to run the actor"]
pub struct AsyncSpawnBuilderWithAddress<'a, A: AsyncActor, F: IntoFuture<Output = A>> {
    spawn_builder: AsyncSpawnBuilderWithoutAddress<'a, A, F>,
    addr: Addr<A::Message>,
}

impl<A: AsyncActor, F: IntoFuture<Output = A> + Send + 'static>
    AsyncSpawnBuilderWithAddress<'_, A, F>
{
    /// Spawn this async actor into a new thread managed by the [`System`].
    pub fn spawn(self) -> Result<Addr<A::Message>, ActorError> {
        let builder = self.spawn_builder;
        builder.system.spawn_async_fn_with_addr(builder.factory, self.addr.clone())?;
        Ok(self.addr)
    }
}

impl System {
    /// Prepare an async actor to be spawned. Returns an [`AsyncSpawnBuilderWithoutAddress`]
    /// which has to be further configured before spawning the actor.
    pub fn prepare_async<A>(
        &mut self,
        actor: A,
    ) -> AsyncSpawnBuilderWithoutAddress<'_, A, future::Ready<A>>
    where
        A: AsyncActor,
    {
        AsyncSpawnBuilderWithoutAddress { system: self, factory: future::ready(actor) }
    }

    /// Similar to [`Self::prepare_async()`], but an async actor factory is passed instead
    /// of an [`AsyncActor`] itself. This is used when an actor needs to be
    /// created on its own thread instead of the calling thread.
    /// Returns an [`AsyncSpawnBuilderWithoutAddress`] which has to be further
    /// configured before spawning the actor.
    pub fn prepare_async_factory<A, F>(
        &mut self,
        factory: F,
    ) -> AsyncSpawnBuilderWithoutAddress<'_, A, F>
    where
        A: AsyncActor,
        F: IntoFuture<Output = A>,
    {
        AsyncSpawnBuilderWithoutAddress { system: self, factory }
    }

    /// Spawn an [`AsyncActor`] in the system, returning its address when successful.
    /// This address is created by the system and uses a default capacity.
    /// If you need to customize the address see [`Self::prepare_async()`] or
    /// [`Self::prepare_async_factory()`].
    pub fn spawn_async<A>(&mut self, actor: A) -> Result<Addr<A::Message>, ActorError>
    where
        A: AsyncActor + Send + 'static,
    {
        self.prepare_async(actor).with_default_capacity().spawn()
    }

    fn spawn_async_fn_with_addr<F, A>(
        &mut self,
        factory: F,
        addr: Addr<A::Message>,
    ) -> Result<(), ActorError>
    where
        F: IntoFuture<Output = A> + Send + 'static,
        A: AsyncActor,
    {
        // Hold the lock until the end of the function to prevent the race
        // condition between spawn and shutdown.
        let system_state_lock = self.handle.system_state.read();
        if !system_state_lock.is_running() {
            return Err(ActorError::SystemStopped { actor_name: A::name() });
        }

        let system_handle = self.handle.clone();
        let context =
            BareContext { system_handle: system_handle.clone(), myself: addr.recipient.clone() };
        let control_addr = addr.control_tx.clone();

        let thread_handle = thread::Builder::new()
            .name(A::name().into())
            .spawn(move || {
                let runtime = match LocalRuntime::new() {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        Self::report_error_shutdown(
                            &system_handle,
                            A::name(),
                            "creating async runtime",
                            e,
                        );
                        return;
                    },
                };

                let main_task = async {
                    let mut actor = factory.await;

                    if let Err(error) = actor.started(&context).await {
                        Self::report_error_shutdown(&system_handle, A::name(), "started()", error);
                        return;
                    }
                    debug!("[{}] started async actor: {}", system_handle.name, A::name());

                    Self::run_async_actor_select_loop(actor, addr, &context, &system_handle).await
                };

                runtime.block_on(main_task)
            })
            .map_err(|_| ActorError::SpawnFailed { actor_name: A::name() })?;

        self.handle
            .registry
            .lock()
            .push(RegistryEntry::BackgroundThread(control_addr, thread_handle));

        Ok(())
    }

    /// Keep logically in sync with [`Self::run_actor_select_loop()`].
    async fn run_async_actor_select_loop<A>(
        mut actor: A,
        addr: Addr<A::Message>,
        context: &BareContext<A::Message>,
        system_handle: &SystemHandle,
    ) where
        A: AsyncActor,
    {
        /// What can be received during one actor event loop.
        enum Received<M> {
            Control(Control),
            Message(M),
        }

        let mut control_stream = addr.control_rx.into_stream();
        let mut high_prio_stream = addr.priority_rx.into_stream();
        let mut normal_prio_stream = addr.message_rx.into_stream();

        loop {
            // We have a nuanced requirements on combinator for the futures:
            // 1. If multiple futures in the combinator are ready, it should return the one with
            //    higher priority (control > high > normal);
            // 2. Otherwise it would wait for the first message to be ready and return that.
            //
            // Tokio's `select` macro documentation contains nice survey of ecosystem alternatives:
            // https://docs.rs/tokio/latest/tokio/macro.select.html#racing-futures
            let received = select_biased!(
                control = control_stream.next() => {
                    Received::Control(control.expect("We keep control_tx alive through addr."))
                },
                high_prio = high_prio_stream.next() => {
                    Received::Message(high_prio.expect("We keep priority_tx alive through addr."))
                },
                normal_prio = normal_prio_stream.next() => {
                    Received::Message(normal_prio.expect("We keep message_tx alive through addr."))
                },
            );

            // Process the event. Returning ends actor loop, the normal operation is to fall through.
            match received {
                Received::Control(Control::Stop) => {
                    if let Err(error) = actor.stopped(context).await {
                        // FWIW this should always hit the "while shutting down" variant.
                        Self::report_error_shutdown(system_handle, A::name(), "stopped()", error);
                    }
                    debug!("[{}] stopped actor: {}", system_handle.name, A::name());
                    return;
                },
                Received::Message(msg) => {
                    trace!("[{}] message received by {}", system_handle.name, A::name());
                    if let Err(error) = actor.handle(context, msg).await {
                        Self::report_error_shutdown(system_handle, A::name(), "handle()", error);
                        return;
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Actor, Context, Recipient};
    use anyhow::Error;
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    struct AsyncTestActor {
        recorder: Recipient<TestMessage>,
    }

    impl AsyncActor for AsyncTestActor {
        type Error = Error;
        type Message = TestMessage;

        fn priority(message: &TestMessage) -> Priority {
            match message {
                TestMessage::HighPrio(_) => Priority::High,
                _ => Priority::Normal,
            }
        }

        async fn started(&mut self, _: &BareContext<TestMessage>) -> Result<(), Error> {
            debug!("AsyncActor started hook");
            self.recorder.send(TestMessage::Event("started"))?;
            Ok(())
        }

        async fn handle(
            &mut self,
            context: &BareContext<TestMessage>,
            message: TestMessage,
        ) -> Result<(), Error> {
            self.recorder.send(message.clone())?;

            if message == TestMessage::DelayedTask {
                let recorder = self.recorder.clone();
                tokio::spawn(async move {
                    debug!("delayed task started");
                    tokio::time::sleep(Duration::from_millis(10)).await;

                    recorder.send(TestMessage::Event("delayed task finished"))?;
                    debug!("delayed task finished");
                    Ok::<(), Error>(())
                });
            }

            if message == TestMessage::DelayedShutdown {
                let system_handle = context.system_handle.clone();
                tokio::spawn(async move {
                    debug!("delayed shutdown started");
                    tokio::time::sleep(Duration::from_millis(20)).await;

                    debug!("delayed shutdown shutting down now");
                    system_handle.shutdown()
                });
            }

            Ok(())
        }

        async fn stopped(&mut self, _: &BareContext<TestMessage>) -> Result<(), Error> {
            trace!("AsyncActor stopped hook");
            self.recorder.send(TestMessage::Event("stopped"))?;
            Ok(())
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TestMessage {
        Event(&'static str),
        HighPrio(usize),
        NormalPrio(usize),
        DelayedTask,
        DelayedShutdown,
    }

    struct SyncRecorder {
        received: Arc<Mutex<Vec<TestMessage>>>,
    }

    impl Actor for SyncRecorder {
        type Context = Context<Self::Message>;
        type Error = Error;
        type Message = TestMessage;

        fn handle(
            &mut self,
            _context: &mut Self::Context,
            message: Self::Message,
        ) -> Result<(), Self::Error> {
            self.received.lock().expect("lock should not be poisoned").push(message);
            Ok(())
        }
    }

    #[test]
    fn async_priorities() {
        // Logger might have been initialized by another test, so just try on best-effort basis.
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace"))
            .try_init()
            .ok();

        let mut system = System::new("async priorities");

        let received = Arc::new(Mutex::new(Vec::new()));
        let recorder_actor = SyncRecorder { received: Arc::clone(&received) };
        let recorder_addr = system.prepare(recorder_actor).with_capacity(10).spawn().unwrap();

        let async_actor = AsyncTestActor { recorder: recorder_addr.recipient() };
        let async_addr = system.prepare_async(async_actor).with_capacity(10).spawn().unwrap();

        async_addr.send(TestMessage::DelayedTask).unwrap();
        async_addr.send(TestMessage::DelayedShutdown).unwrap();
        async_addr.send(TestMessage::NormalPrio(1)).unwrap();
        async_addr.send(TestMessage::NormalPrio(2)).unwrap();
        async_addr.send(TestMessage::HighPrio(3)).unwrap();
        async_addr.send(TestMessage::HighPrio(4)).unwrap();

        system.run().unwrap();

        let received = Arc::into_inner(received)
            .expect("arc has a single reference at this point")
            .into_inner()
            .expect("Mutex should not be poisoned");
        assert_eq!(
            received,
            [
                TestMessage::Event("started"),
                TestMessage::HighPrio(3),
                TestMessage::HighPrio(4),
                TestMessage::DelayedTask,
                TestMessage::DelayedShutdown,
                TestMessage::NormalPrio(1),
                TestMessage::NormalPrio(2),
                TestMessage::Event("delayed task finished")
            ]
        );
    }

    #[test]
    fn async_error() {
        // Logger might have been initialized by another test, so just try on best-effort basis.
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace"))
            .try_init()
            .ok();

        struct ErroringActor;

        impl AsyncActor for ErroringActor {
            type Error = String;
            type Message = ();

            async fn handle(&mut self, _c: &BareContext<()>, _m: ()) -> Result<(), String> {
                Err(String::from("Raising an error"))
            }
        }

        let mut system = System::new("async error");
        let addr = system.spawn_async(ErroringActor).unwrap();
        addr.send(()).unwrap();

        // The Error isn't really propagated here, but at least we can test that the system doesn't
        // continue running (i.e. this test finishes quickly, doesn't hang here indefinitely).
        system.run().unwrap();
    }
}
