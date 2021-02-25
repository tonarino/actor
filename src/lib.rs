#![warn(clippy::all)]

//! This crate aims to provide a minimalist and high-performance actor framework
//! for Rust with *significantly* less complexity than other frameworks like
//! [Actix](https://docs.rs/actix/).
//!
//! In this framework, each `Actor` is its own OS-level thread. This makes debugging
//! significantly simpler, and in systems with a number of actors similar to the number
//! of available cores, should mean better performance due to less overhead and fewer
//! context switches.
//!
//! # Example
//! ```rust
//! use actor::{Actor, Context, System};
//!
//! struct TestActor {}
//! impl Actor for TestActor {
//!     type Error = ();
//!     type Message = usize;
//!
//!     fn name() -> &'static str {
//!         "TestActor"
//!     }
//!
//!     fn handle(&mut self, _context: &Context<Self>, message: Self::Message) -> Result<(), ()> {
//!         println!("message: {}", message);
//!
//!         Ok(())
//!     }
//! }
//!
//! let mut system = System::new("default");
//!
//! // will spin up a new thread running this actor
//! let addr = system.spawn(TestActor {}).unwrap();
//!
//! // send messages to actors to spin off work...
//! addr.send(1usize).unwrap();
//!
//! // ask the actors to finish and join the threads.
//! system.shutdown().unwrap();
//! ```
//!

use crossbeam_channel::{self as channel, select, Receiver, SendError, Sender, TrySendError};
use log::*;
use parking_lot::{Mutex, RwLock};
use std::{fmt, ops::Deref, sync::Arc, thread, time::Duration};

#[cfg(test)]
pub mod testing;

// TODO(jake): make configurable per actor.
static MAX_CHANNEL_BLOAT: usize = 5;

#[derive(Debug)]
pub enum ActorError {
    /// The system has stopped, and a new actor can not be started.
    SystemStopped { actor_name: &'static str },
    /// The actor message channel is disconnected.
    ChannelDisconnected { actor_name: &'static str },
    /// Failed to spawn an actor thread.
    SpawnFailed { actor_name: &'static str },
    /// A panic occurred inside an actor thread.
    ActorPanic,
}

impl fmt::Display for ActorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ActorError::SystemStopped { actor_name } => {
                write!(f, "The system is not running. The actor {} can not be started.", actor_name)
            },
            ActorError::ChannelDisconnected { actor_name } => {
                write!(f, "The message channel is disconnected for the actor {}.", actor_name)
            },
            ActorError::SpawnFailed { actor_name } => {
                write!(f, "Failed to spawn a thread for the actor {}.", actor_name)
            },
            ActorError::ActorPanic => {
                write!(f, "A panic inside an actor thread. See above for more verbose logs.")
            },
        }
    }
}

impl std::error::Error for ActorError {}

/// Systems are responsible for keeping track of their spawned actors, and managing
/// their lifecycles appropriately.
///
/// You may run multiple systems in the same application, each system being responsible
/// for its own pool of actors.
#[derive(Default)]
pub struct System {
    handle: SystemHandle,
}

type SystemCallback = Box<dyn Fn() -> Result<(), ActorError> + Send + Sync>;

#[derive(Default)]
pub struct SystemCallbacks {
    pub preshutdown: Option<SystemCallback>,
    pub postshutdown: Option<SystemCallback>,
}

#[derive(Debug, PartialEq)]
enum SystemState {
    /// The system is running and able to spawn new actors, or be asked to shut down
    Running,

    /// The system is in the process of shutting down, actors cannot be spawned
    /// or request for the system to shut down again
    ShuttingDown,

    /// The system has finished shutting down and is no longer running.
    /// All actors have stopped and their threads have been joined. No actors
    /// may be spawned at this point.
    Stopped,
}

impl Default for SystemState {
    fn default() -> Self {
        SystemState::Running
    }
}

/// Contains the "metadata" of the system, including information about the registry
/// of actors currently existing within the system.
#[derive(Default, Clone)]
pub struct SystemHandle {
    name: String,
    registry: Arc<Mutex<Vec<RegistryEntry>>>,
    system_state: Arc<RwLock<SystemState>>,
    callbacks: Arc<SystemCallbacks>,
}

/// An execution context for a specific actor. Specifically, this is useful for managing
/// the lifecycle of itself (through the `myself` field) and other actors via the `SystemHandle`
/// provided.
pub struct Context<A: Actor + ?Sized> {
    pub system_handle: SystemHandle,
    pub myself: Addr<A>,
}

impl System {
    /// Creates a new System with a given name.
    pub fn new(name: &str) -> Self {
        System::with_callbacks(name, Default::default())
    }

    pub fn with_callbacks(name: &str, callbacks: SystemCallbacks) -> Self {
        Self {
            handle: SystemHandle {
                name: name.to_owned(),
                callbacks: Arc::new(callbacks),
                ..SystemHandle::default()
            },
        }
    }

    /// Spawn a normal [`Actor`] in the system.
    pub fn spawn<A>(&mut self, actor: A) -> Result<Addr<A>, ActorError>
    where
        A: Actor + Send + 'static,
    {
        self.spawn_fn(move || actor)
    }

    /// Spawn a normal [`Actor`] in the system, with non-default capacity for its input channel.
    pub fn spawn_with_capacity<A>(
        &mut self,
        actor: A,
        capacity: usize,
    ) -> Result<Addr<A>, ActorError>
    where
        A: Actor + Send + 'static,
    {
        self.spawn_fn_with_capacity(move || actor, capacity)
    }

    /// Spawn a normal Actor in the system, using a factory that produces an [`Actor`].
    ///
    /// This method is useful if your actor does not implement [`Send`], since it can create
    /// the struct directly within the thread.
    pub fn spawn_fn<F, A>(&mut self, factory: F) -> Result<Addr<A>, ActorError>
    where
        F: FnOnce() -> A + Send + 'static,
        A: Actor + 'static,
    {
        self.spawn_fn_with_capacity(factory, MAX_CHANNEL_BLOAT)
    }

    /// Spawn a normal Actor in the system, using a factory that produces an [`Actor`],
    /// with non-default capacity for its input channel. See [`System::spawn_fn()`].
    pub fn spawn_fn_with_capacity<F, A>(
        &mut self,
        factory: F,
        capacity: usize,
    ) -> Result<Addr<A>, ActorError>
    where
        F: FnOnce() -> A + Send + 'static,
        A: Actor + 'static,
    {
        let addr = Addr::<A>::with_capacity(capacity);
        self.spawn_fn_with_addr(factory, addr.clone())?;
        Ok(addr)
    }

    /// Spawn a normal Actor in the system, using a factory that produces an [`Actor`],
    /// and an address that will be assigned to the Actor.
    ///
    /// This method is useful if you need to model circular dependencies between `Actor`s.
    pub fn spawn_fn_with_addr<F, A>(&mut self, factory: F, addr: Addr<A>) -> Result<(), ActorError>
    where
        F: FnOnce() -> A + Send + 'static,
        A: Actor + 'static,
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
        let context = Context { system_handle: system_handle.clone(), myself: addr.clone() };
        let control_addr = addr.control_addr();

        let thread_handle = thread::Builder::new()
            .name(A::name().into())
            .spawn(move || {
                let mut actor = factory();

                actor.started(&context);
                debug!("[{}] started actor: {}", system_handle.name, A::name());

                let actor_result =
                    Self::run_actor_select_loop(actor, addr, &context, &system_handle);
                if let Err(err) = &actor_result {
                    error!("run_actor_select_loop returned an error: {}", err);
                }

                actor_result
            })
            .map_err(|_| ActorError::SpawnFailed { actor_name: A::name() })?;

        self.handle
            .registry
            .lock()
            .push(RegistryEntry::BackgroundThread(control_addr, thread_handle));

        Ok(())
    }

    /// Block the current thread until the system is shutdown.
    pub fn run(&mut self) -> Result<(), ActorError> {
        while *self.system_state.read() != SystemState::Stopped {
            thread::sleep(Duration::from_millis(10));
        }

        Ok(())
    }

    /// Takes an actor and its address and runs it on the calling thread. This function
    /// will exit once the actor has stopped.
    pub fn run_on_main<A>(&mut self, mut actor: A, addr: Addr<A>) -> Result<(), ActorError>
    where
        A: Actor,
    {
        // Prevent race condition of spawn and shutdown.
        if !self.is_running() {
            return Err(ActorError::SystemStopped { actor_name: A::name() });
        }

        let system_handle = &self.handle;
        let context = Context { system_handle: system_handle.clone(), myself: addr.clone() };

        let control_addr = addr.control_addr();
        self.handle.registry.lock().push(RegistryEntry::CurrentThread(control_addr));

        actor.started(&context);
        debug!("[{}] started actor: {}", system_handle.name, A::name());
        Self::run_actor_select_loop(actor, addr, &context, system_handle)?;

        // Wait for the system to shutdown before we exit, otherwise the process
        // would exit before the system is completely shutdown
        // TODO(bschwind) - We could possibly use a parking_lot::CondVar here
        //                  for more efficient waiting
        while *self.system_state.read() != SystemState::Stopped {
            thread::sleep(Duration::from_millis(10));
        }

        Ok(())
    }

    fn run_actor_select_loop<A>(
        mut actor: A,
        addr: Addr<A>,
        context: &Context<A>,
        system_handle: &SystemHandle,
    ) -> Result<(), ActorError>
    where
        A: Actor,
    {
        loop {
            select! {
                recv(addr.control_rx) -> msg => {
                    match msg {
                        Ok(Control::Stop) => {
                            actor.stopped(&context);
                            debug!("[{}] stopped actor: {}", system_handle.name, A::name());
                            return Ok(());
                        },
                        Err(_) => {
                            error!("[{}] control channel empty and disconnected. ending actor thread.", A::name());
                            return Ok(());
                        }
                    }
                },

                recv(addr.message_rx) -> msg => {
                    match msg {
                        Ok(msg) => {
                            trace!("[{}] message received by {}", system_handle.name, A::name());
                            if let Err(err) = actor.handle(&context, msg) {
                                error!("{} error: {:?}", A::name(), err);
                                let _ = context.system_handle.shutdown();

                                return Ok(());
                            }
                        },
                        Err(_) => {
                            return Err(ActorError::ChannelDisconnected{actor_name:A::name()});
                        }
                    }
                },
            };
        }
    }
}

impl Drop for System {
    fn drop(&mut self) {
        self.shutdown().unwrap();
    }
}

impl Deref for System {
    type Target = SystemHandle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl SystemHandle {
    /// Stops all actors spawned by this system.
    pub fn shutdown(&self) -> Result<(), ActorError> {
        let current_thread = thread::current();
        let current_thread_name = current_thread.name().unwrap_or("Unknown thread id");
        info!("Thread [{}] shutting down the actor system", current_thread_name);

        // Use an inner scope to prevent holding the lock for the duration of shutdown
        {
            let mut system_state_lock = self.system_state.write();

            match *system_state_lock {
                SystemState::ShuttingDown | SystemState::Stopped => {
                    debug!("Thread [{}] called system.shutdown() but the system is already shutting down or stopped", current_thread_name);
                    return Ok(());
                },
                SystemState::Running => {
                    debug!(
                        "Thread [{}] setting the system_state value to ShuttingDown",
                        current_thread_name
                    );
                    *system_state_lock = SystemState::ShuttingDown;
                },
            }
        }

        info!("[{}] system shutting down.", self.name);

        if let Some(callback) = self.callbacks.preshutdown.as_ref() {
            info!("[{}] calling pre-shutdown callback.", self.name);
            if let Err(err) = callback() {
                warn!("[{}] pre-shutdown callback failed, reason: {}", self.name, err);
            }
        }

        let err_count = {
            let mut registry = self.registry.lock();
            debug!("[{}] joining {} actor threads.", self.name, registry.len());
            // Joining actors in the reverse order in which they are spawn.
            registry
                .drain(..)
                .rev()
                .enumerate()
                .filter_map(|(i, mut entry)| {
                    let actor_name = entry.name();

                    if let Err(e) = entry.control_addr().stop() {
                        warn!("control channel is closed: {} ({})", actor_name, e);
                    }

                    match entry {
                        RegistryEntry::CurrentThread(_) => None,
                        RegistryEntry::BackgroundThread(_control_addr, thread_handle) => {
                            if thread_handle.thread().id() == current_thread.id() {
                                return None;
                            }

                            debug!("[{}] [{}] joining actor thread: {}", self.name, i, actor_name);

                            let join_result = thread_handle
                                .join()
                                .map_err(|e| {
                                    error!("a panic inside actor thread {}: {:?}", actor_name, e)
                                })
                                .and_then(|actor_result| {
                                    actor_result.map_err(|e| {
                                        error!(
                                            "actor thread {} returned an error: {:?}",
                                            actor_name, e
                                        )
                                    })
                                });

                            debug!("[{}] [{}] joined actor thread:  {}", self.name, i, actor_name);
                            join_result.err()
                        },
                    }
                })
                .count()
        };

        info!("[{}] system finished shutting down.", self.name);

        if let Some(callback) = self.callbacks.postshutdown.as_ref() {
            info!("[{}] calling post-shutdown callback.", self.name);
            if let Err(err) = callback() {
                warn!("[{}] post-shutdown callback failed, reason: {}", self.name, err);
            }
        }

        *self.system_state.write() = SystemState::Stopped;

        if err_count > 0 {
            Err(ActorError::ActorPanic)
        } else {
            Ok(())
        }
    }

    pub fn is_running(&self) -> bool {
        *self.system_state.read() == SystemState::Running
    }
}

enum RegistryEntry {
    CurrentThread(ControlAddr),
    BackgroundThread(ControlAddr, thread::JoinHandle<Result<(), ActorError>>),
}

impl RegistryEntry {
    fn name(&self) -> String {
        match self {
            RegistryEntry::CurrentThread(_) => {
                thread::current().name().unwrap_or("unnamed").to_owned()
            },
            RegistryEntry::BackgroundThread(_, thread_handle) => {
                thread_handle.thread().name().unwrap_or("unnamed").to_owned()
            },
        }
    }

    fn control_addr(&mut self) -> &mut ControlAddr {
        match self {
            RegistryEntry::CurrentThread(control_addr) => control_addr,
            RegistryEntry::BackgroundThread(control_addr, _) => control_addr,
        }
    }
}

/// The set of available control messages that all actors respond to.
pub enum Control {
    /// Stop the actor
    Stop,
}

/// The base actor trait.
pub trait Actor {
    /// The expected type of a message to be received.
    type Message: Send;
    /// The type to return on error in the handle method.
    type Error: std::fmt::Debug;

    /// The primary function of this trait, allowing an actor to handle incoming messages of a certain type.
    fn handle(
        &mut self,
        context: &Context<Self>,
        message: Self::Message,
    ) -> Result<(), Self::Error>;

    /// The name of the Actor - used only for logging/debugging.
    fn name() -> &'static str;

    /// An optional callback when the Actor has been started.
    fn started(&mut self, _context: &Context<Self>) {}

    /// An optional callback when the Actor has been stopped.
    fn stopped(&mut self, _context: &Context<Self>) {}
}

pub struct Addr<A: Actor + ?Sized> {
    recipient: Recipient<A::Message>,
    message_rx: Receiver<A::Message>,
    control_rx: Receiver<Control>,
}

impl<A: Actor> Default for Addr<A> {
    fn default() -> Self {
        Self::with_capacity(MAX_CHANNEL_BLOAT)
    }
}

impl<A: Actor> Clone for Addr<A> {
    fn clone(&self) -> Self {
        Self {
            recipient: self.recipient.clone(),
            message_rx: self.message_rx.clone(),
            control_rx: self.control_rx.clone(),
        }
    }
}

impl<A, M> Deref for Addr<A>
where
    A: Actor<Message = M>,
{
    type Target = Recipient<M>;

    fn deref(&self) -> &Self::Target {
        &self.recipient
    }
}

impl<A: Actor> Addr<A> {
    pub fn with_capacity(capacity: usize) -> Self {
        let (message_tx, message_rx) = channel::bounded::<A::Message>(capacity);
        let (control_tx, control_rx) = channel::bounded(MAX_CHANNEL_BLOAT);

        Self {
            recipient: Recipient { actor_name: A::name().into(), message_tx, control_tx },
            message_rx,
            control_rx,
        }
    }

    /// "Genericize" an address to, rather than point to a specific actor,
    /// be applicable to any actor that handles a given message-response type.
    pub fn recipient(&self) -> Recipient<A::Message> {
        self.recipient.clone()
    }
}

/// Similar to `Addr`, but rather than pointing to a specific actor,
/// it is typed for any actor that handles a given message-response type.
pub struct Recipient<M> {
    actor_name: String,
    message_tx: Sender<M>,
    control_tx: Sender<Control>,
}

// #[derive(Clone)] adds Clone bound to M, which is not necessary.
// https://github.com/rust-lang/rust/issues/26925
impl<M> Clone for Recipient<M> {
    fn clone(&self) -> Self {
        Self {
            actor_name: self.actor_name.clone(),
            message_tx: self.message_tx.clone(),
            control_tx: self.control_tx.clone(),
        }
    }
}

impl<M> Recipient<M> {
    /// Non-blocking call to send a message. Use this if you need to react when
    /// the channel is full.
    pub fn try_send<N: Into<M>>(&self, message: N) -> Result<(), TrySendError<M>> {
        self.message_tx.try_send(message.into())
    }

    /// Non-blocking call to send a message. Use this if there is nothing you can
    /// do when the channel is full. The method still logs a warning for you in
    /// that case.
    pub fn send<N: Into<M>>(&self, message: N) -> Result<(), TrySendError<M>> {
        let result = self.try_send(message.into());
        if let Err(TrySendError::Full(_)) = &result {
            trace!("[{}] dropped message (channel bloat)", self.actor_name);
            return Ok(());
        }
        result
    }

    /// Non-blocking call to send a message. Use this if you do not care if
    /// messages are being dropped.
    pub fn send_quiet<N: Into<M>>(&self, message: N) {
        let _ = self.try_send(message.into());
    }

    /// Non-blocking call to send a message. Use if you expect the channel to be
    /// frequently full (slow consumer), but would still like to be notified if a
    /// different error occurs (e.g. disconnection).
    pub fn send_if_not_full<N: Into<M>>(&self, message: N) -> Result<(), TrySendError<M>> {
        if self.remaining_capacity().unwrap_or(usize::max_value()) > 1 {
            return self.send(message);
        }

        Ok(())
    }

    pub fn stop(&self) -> Result<(), SendError<()>> {
        self.control_tx.send(Control::Stop).map_err(|_| SendError(()))
    }

    // TODO(ryo): Properly support the concept of priority channels.
    pub fn remaining_capacity(&self) -> Option<usize> {
        self.message_tx.capacity().map(|capacity| capacity - self.message_tx.len())
    }

    pub fn control_addr(&self) -> ControlAddr {
        ControlAddr { control_tx: self.control_tx.clone() }
    }
}

/// An address to an actor that can *only* handle lifecycle control.
#[derive(Clone)]
pub struct ControlAddr {
    control_tx: Sender<Control>,
}

impl ControlAddr {
    pub fn stop(&self) -> Result<(), channel::SendError<Control>> {
        self.control_tx.send(Control::Stop)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    struct TestActor {}
    impl Actor for TestActor {
        type Error = ();
        type Message = usize;

        fn name() -> &'static str {
            "TestActor"
        }

        fn handle(&mut self, _: &Context<Self>, message: usize) -> Result<(), ()> {
            println!("message: {}", message);

            Ok(())
        }

        fn started(&mut self, _: &Context<Self>) {
            println!("started");
        }

        fn stopped(&mut self, _: &Context<Self>) {
            println!("stopped");
        }
    }

    #[test]
    fn it_works() {
        let mut system = System::new("hi");
        let address = system.spawn(TestActor {}).unwrap();
        let _ = system.spawn(TestActor {}).unwrap();
        let _ = system.spawn(TestActor {}).unwrap();
        let _ = system.spawn(TestActor {}).unwrap();
        let _ = system.spawn(TestActor {}).unwrap();
        address.send(1337usize).unwrap();
        address.send(666usize).unwrap();
        address.send(1usize).unwrap();
        thread::sleep(Duration::from_millis(100));

        system.shutdown().unwrap();
        thread::sleep(Duration::from_millis(100));
    }
}
