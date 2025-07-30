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
//! [^tokio]: TODO explain that any runtime is sufficient (no dependency on tokio-specific features)
//!     we only need to spawn _some_ runtime in the actor loop. tokio was just a pragmatic choice.
//!     we could add support for alternative ones, even runtime-configurable.

use crate::{
    ActorError, Addr, Capacity, Context, Control, Priority, RegistryEntry, System, SystemHandle,
    SystemState,
};
use flume::RecvError;
use futures_lite::FutureExt;
use log::{debug, trace};
use std::{any::type_name, fmt, thread};
use tokio::runtime::LocalRuntime;

/// The actor trait - async variant.
// Ad. the #[allow]: using `async` fn in a trait doesn't allow us to specify `Send` (or other)
// bounds, but we don't really need any bounds, because
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
    async fn started(&mut self, _context: &mut Context<Self::Message>) -> Result<(), Self::Error> {
        Ok(())
    }

    /// The primary function of this trait, allowing an actor to handle incoming messages of a certain type.
    async fn handle(
        &mut self,
        context: &mut Context<Self::Message>,
        message: Self::Message,
    ) -> Result<(), Self::Error>;

    /// An optional callback when the Actor has been stopped.
    async fn stopped(&mut self, _context: &mut Context<Self::Message>) -> Result<(), Self::Error> {
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

impl System {
    pub fn spawn_async<A>(&mut self, actor: A) -> Result<Addr<A::Message>, ActorError>
    where
        A: AsyncActor + Send + 'static,
    {
        let addr = A::addr();
        let factory = move || actor;
        self.spawn_async_fn_with_addr(factory, addr.clone())?;
        Ok(addr)
    }

    fn spawn_async_fn_with_addr<F, A>(
        &mut self,
        factory: F,
        addr: Addr<A::Message>,
    ) -> Result<(), ActorError>
    where
        F: FnOnce() -> A + Send + 'static,
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
        // TODO(Matej): async actors likely should have a different context?
        let mut context = Context::new(system_handle.clone(), addr.recipient.clone());
        let control_addr = addr.control_tx.clone();

        let thread_handle = thread::Builder::new()
            .name(A::name().into())
            .spawn(move || {
                let mut actor = factory();

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
                    if let Err(error) = actor.started(&mut context).await {
                        Self::report_error_shutdown(&system_handle, A::name(), "started()", error);
                        return;
                    }
                    debug!("[{}] started async actor: {}", system_handle.name, A::name());

                    Self::run_async_actor_select_loop(actor, addr, &mut context, &system_handle)
                        .await
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
        context: &mut Context<A::Message>,
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
    use crate::{Actor, Recipient};
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

        async fn started(&mut self, _: &mut Context<TestMessage>) -> Result<(), Error> {
            debug!("AsyncActor started hook");
            self.recorder.send(TestMessage::Event("started".into()))?;
            Ok(())
        }

        async fn handle(
            &mut self,
            context: &mut Context<TestMessage>,
            message: TestMessage,
        ) -> Result<(), Error> {
            self.recorder.send(message.clone())?;

            if message == TestMessage::DelayedTask() {
                let recorder = self.recorder.clone();
                tokio::spawn(async move {
                    debug!("delayed task started");
                    tokio::time::sleep(Duration::from_millis(10)).await;

                    recorder.send(TestMessage::Event("delayed task finished".into()))?;
                    debug!("delayed task finished");
                    Ok::<(), Error>(())
                });
            }

            if message == TestMessage::DelayedShutdown() {
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

        async fn stopped(&mut self, _: &mut Context<TestMessage>) -> Result<(), Error> {
            trace!("AsyncActor stopped hook");
            self.recorder.send(TestMessage::Event("stopped".into()))?;
            Ok(())
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TestMessage {
        Event(String),
        HighPrio(usize),
        NormalPrio(usize),
        DelayedTask(),
        DelayedShutdown(),
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

        let mut system = System::new("async_priorities_system");

        let received = Arc::new(Mutex::new(Vec::new()));
        let recorder_actor = SyncRecorder { received: Arc::clone(&received) };
        let recorder_addr = system.spawn(recorder_actor).unwrap();

        let async_actor = AsyncTestActor { recorder: recorder_addr.recipient() };
        let async_addr = system.spawn_async(async_actor).unwrap();

        async_addr.send(TestMessage::DelayedTask()).unwrap();
        async_addr.send(TestMessage::DelayedShutdown()).unwrap();
        async_addr.send(TestMessage::NormalPrio(1)).unwrap();
        async_addr.send(TestMessage::NormalPrio(2)).unwrap();
        async_addr.send(TestMessage::HighPrio(3)).unwrap();
        async_addr.send(TestMessage::HighPrio(4)).unwrap();

        thread::sleep(Duration::from_millis(5));
        system.shutdown().unwrap()
    }
}
