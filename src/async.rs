//! # Async Actors
//!
//! `tonari-actor` lets you freely combine sync (blocking) and `async` actors within one system.
//!
//! While sync actors implement the [`Actor`](crate::Actor) trait and are spawned using the
//! [`System::spawn()`], [`System::prepare()`] and [`System::prepare_fn()`] family of methods,
//! `async` actors implement [`AsyncActor`] and are spawned using [`System::spawn_async()`],
//! [`System::prepare_async()`] and [`System::prepare_async_fn()`].
//!
//! Sync and `async` actors share the same [`Addr`] and [`Recipient`](crate::Recipient) types.
//!
//! `async` actors share the same paradigm as sync actors: each one gets its own OS-level thread.
//! More specifically a single-threaded async runtime is spawned for every `async` actor.
//!
//! `tonari-actor` currently uses the [`tokio`] ecosystem, more specifically its [`LocalRuntime`][^tokio].
//!
//! TODO explain tokio feature flags and that downstreams may need to enable more.
//!
//! TODO lacking feature: block on
//!
//! [^tokio]: TODO explain that any runtime is sufficient (no dependency on tokio-specific features)
//!     we only need to spawn _some_ runtime in the actor loop. tokio was just a pragmatic choice.
//!     we could add support for alternative ones, even runtime-configurable.

use crate::{
    ActorError, Addr, BareContext, Capacity, Control, Priority, RegistryEntry, System,
    SystemHandle, SystemState,
};
use flume::RecvError;
use futures_lite::FutureExt;
use log::{debug, trace};
use std::{any::type_name, fmt, future, thread};
use tokio::runtime::LocalRuntime;

/// The actor trait - async variant.
// Ad. the #[allow]: using `async` fn in a trait doesn't allow us to specify `Send` (or other)
// bounds, but we don't really need any bounds, because TODO - we use single-threaded runtime
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
    pub fn prepare_async_fn<A, F>(
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
    /// [`Self::prepare_async_fn()`].
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
        match *system_state_lock {
            SystemState::ShuttingDown | SystemState::Stopped => {
                return Err(ActorError::SystemStopped { actor_name: A::name() });
            },
            SystemState::Running => {},
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

        loop {
            // BIG TODO(Matej): is it okay to create a future every time and then drop it?
            // Should we work with streams instead?
            let receive_control = async {
                match addr.control_rx.recv_async().await {
                    Ok(control) => Received::Control(control),
                    Err(RecvError::Disconnected) => {
                        panic!("We keep control_tx alive through addr, should not happen.");
                    },
                }
            };
            let receive_high = async {
                match addr.priority_rx.recv_async().await {
                    Ok(msg) => Received::Message(msg),
                    Err(RecvError::Disconnected) => {
                        panic!("We keep priority_tx alive through addr, should not happen.");
                    },
                }
            };
            let receive_normal = async {
                match addr.message_rx.recv_async().await {
                    Ok(msg) => Received::Message(msg),
                    Err(RecvError::Disconnected) => {
                        panic!("We keep message_tx alive through addr, should not happen.");
                    },
                }
            };

            // We have a nuanced requirements on combinator for the futures:
            // 1. If multiple futures in the combinator are ready, it should return the one with
            //    higher priority (control > high > normal);
            // 2. Otherwise it would wait for the first message to be ready and return that.
            let receive_per_priority = receive_control.or(receive_high).or(receive_normal);
            let received = receive_per_priority.await;

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
        let recorder_addr = system.spawn(recorder_actor).unwrap();

        let async_actor = AsyncTestActor { recorder: recorder_addr.recipient() };
        let async_addr = system.spawn_async(async_actor).unwrap();

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
