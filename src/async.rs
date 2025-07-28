use crate::{
    ActorError, Addr, Capacity, Context, Priority, RegistryEntry, System, SystemHandle, SystemState,
};
use log::debug;
use std::{any::type_name, fmt, thread};
use tokio::runtime::LocalRuntime;

/// The actor trait - async variant.
// TODO(Matej): this allow should be fine: we cannot add `Send` bounds, but we don't them?
#[allow(async_fn_in_trait)]
pub trait AsyncActor {
    /// The expected type of a message to be received.
    type Message: Send + 'static;
    /// The type to return on error in the handle method.
    type Error: fmt::Display;
    /// What kind of context this actor accepts. Usually [`Context<Self::Message>`].
    type Context;

    /// Default capacity of actor's normal-priority inbox unless overridden by `.with_capacity()`.
    const DEFAULT_CAPACITY_NORMAL: usize = 5;
    /// Default capacity of actor's high-priority inbox unless overridden by `.with_capacity()`.
    const DEFAULT_CAPACITY_HIGH: usize = 5;

    /// The name of the Actor - used only for logging/debugging.
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
    async fn started(&mut self, _context: &mut Self::Context) -> Result<(), Self::Error> {
        Ok(())
    }

    /// The primary function of this trait, allowing an actor to handle incoming messages of a certain type.
    async fn handle(
        &mut self,
        context: &mut Self::Context,
        message: Self::Message,
    ) -> Result<(), Self::Error>;

    /// An optional callback when the Actor has been stopped.
    async fn stopped(&mut self, _context: &mut Self::Context) -> Result<(), Self::Error> {
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
        A: AsyncActor<Context = Context<<A as AsyncActor>::Message>> + Send + 'static,
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
        A: AsyncActor<Context = Context<<A as AsyncActor>::Message>>, // + 'static,
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
                        Self::report_error_shutdown(&system_handle, A::name(), "started", error);
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

    async fn run_async_actor_select_loop<A>(
        mut actor: A,
        addr: Addr<A::Message>,
        context: &mut Context<A::Message>,
        system_handle: &SystemHandle,
    ) where
        A: AsyncActor<Context = Context<<A as AsyncActor>::Message>>,
    {
        // FIXME(Matej): actually run the loop
    }
}
