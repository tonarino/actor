#![warn(clippy::all)]

//! This crate aims to provide a minimalist and high-performance actor framework
//! for Rust with significantly less complexity than other frameworks like
//! [Actix](https://docs.rs/actix/).
//!
//! In this framework, each `Actor` is its own OS-level thread. This makes debugging
//! noticeably simpler, and is suitably performant when the number of actors
//! is less than or equal to the number of CPU threads.
//!
//! # Example
//! ```rust
//! use tonari_actor::{Actor, Context, System};
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
//!     fn handle(&mut self, _context: &mut Context<Self>, message: Self::Message) -> Result<(), ()> {
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

use crate::timer::{ScheduleToken, TimerControlFlow, TimerHandle, TimerRef};
use crossbeam_channel::{self as channel, select, Receiver, Sender};
use log::*;
use parking_lot::{Mutex, RwLock};
use std::{fmt, ops::Deref, sync::Arc, thread, time::Duration};

#[cfg(test)]
pub mod testing;

pub mod timer;

// Default capacity for channels unless overridden by `.with_capacity()`.
static DEFAULT_CHANNEL_CAPACITY: usize = 5;

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

/// Reasons why sending a message to an actor can fail.
#[derive(Debug)]
pub enum SendError {
    /// The channel's capacity is full.
    Full,
    /// The recipient of the message no longer exists.
    Disconnected,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Full => write!(f, "The channel's capacity is full."),
            SendError::Disconnected => DisconnectedError.fmt(f),
        }
    }
}

impl std::error::Error for SendError {}

/// The actor message channel is disconnected.
#[derive(Debug)]
pub struct DisconnectedError;

impl fmt::Display for DisconnectedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "The recipient of the message no longer exists.")
    }
}

impl std::error::Error for DisconnectedError {}

impl<M> From<channel::TrySendError<M>> for SendError {
    fn from(orig: channel::TrySendError<M>) -> Self {
        match orig {
            channel::TrySendError::Full(_) => Self::Full,
            channel::TrySendError::Disconnected(_) => Self::Disconnected,
        }
    }
}

/// Systems are responsible for keeping track of their spawned actors, and managing
/// their lifecycles appropriately.
///
/// You may run multiple systems in the same application, each system being responsible
/// for its own pool of actors.
pub struct System {
    handle: SystemHandle,
    timer_handle: TimerHandle,
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
    timer: TimerRef,
}

impl<A: Actor + ?Sized> Context<A> {
    pub fn run_once<F>(&mut self, delay: Duration, callback: F) -> ScheduleToken
    where
        F: FnOnce(ScheduleToken) + Send + 'static,
    {
        run_once(&mut self.timer, delay, callback)
    }

    pub fn run_recurring<F>(
        &mut self,
        delay: Duration,
        interval: Duration,
        callback: F,
    ) -> ScheduleToken
    where
        F: FnMut(ScheduleToken) -> TimerControlFlow + Send + 'static,
    {
        run_recurring(&mut self.timer, delay, interval, callback)
    }
}

/// A builder for specifying how to spawn an [`Actor`].
/// You can specify your own [`Addr`] for the Actor,
/// the capacity of the Actor's inbox, and you can specify
/// whether to spawn the Actor into its own thread or block
/// on the current calling thread.
#[must_use = "You must call .spawn() or .block_on() to run this actor"]
pub struct SpawnBuilder<'a, A: Actor, F: FnOnce() -> A> {
    system: &'a mut System,
    capacity: Option<usize>,
    addr: Option<Addr<A>>,
    factory: F,
}

impl<'a, A: 'static + Actor, F: FnOnce() -> A> SpawnBuilder<'a, A, F> {
    /// Specify a capacity for the actor's receiving channel.
    pub fn with_capacity(self, capacity: usize) -> Self {
        Self { capacity: Some(capacity), ..self }
    }

    /// Specify an existing [`Addr`] to use with this Actor.
    pub fn with_addr(self, addr: Addr<A>) -> Self {
        Self { addr: Some(addr), ..self }
    }

    /// Run this Actor on the current calling thread. This is a
    /// blocking call. This function will exit when the Actor
    /// has stopped.
    pub fn run_and_block(self) -> Result<(), ActorError> {
        let factory = self.factory;
        let capacity = self.capacity.unwrap_or(DEFAULT_CHANNEL_CAPACITY);
        let addr = self.addr.unwrap_or_else(|| Addr::with_capacity(capacity));

        self.system.block_on(factory(), addr)
    }
}

impl<'a, A: 'static + Actor, F: FnOnce() -> A + Send + 'static> SpawnBuilder<'a, A, F> {
    /// Spawn this Actor into a new thread managed by the [`System`].
    pub fn spawn(self) -> Result<Addr<A>, ActorError> {
        let factory = self.factory;
        let capacity = self.capacity.unwrap_or(DEFAULT_CHANNEL_CAPACITY);
        let addr = self.addr.unwrap_or_else(|| Addr::with_capacity(capacity));

        self.system.spawn_fn_with_addr(factory, addr.clone()).map(move |_| addr)
    }
}

fn run_once<F>(timer: &mut TimerRef, delay: Duration, callback: F) -> ScheduleToken
where
    F: FnOnce(ScheduleToken) + Send + 'static,
{
    timer.run_once(delay, callback)
}

fn run_recurring<F>(
    timer: &mut TimerRef,
    delay: Duration,
    interval: Duration,
    callback: F,
) -> ScheduleToken
where
    F: FnMut(ScheduleToken) -> TimerControlFlow + Send + 'static,
{
    timer.run_recurring(delay, interval, callback)
}

impl System {
    /// Creates a new System with a given name.
    pub fn new(name: &str) -> Self {
        System::with_callbacks(name, Default::default())
    }

    pub fn with_callbacks(name: &str, callbacks: SystemCallbacks) -> Self {
        // TODO(bschwind) - Make this configurable
        let timer_resolution = Duration::from_millis(1);

        Self {
            handle: SystemHandle {
                name: name.to_owned(),
                callbacks: Arc::new(callbacks),
                ..SystemHandle::default()
            },
            timer_handle: TimerHandle::new(timer_resolution),
        }
    }

    /// Prepare an actor to be spawned. Returns a [`SpawnBuilder`]
    /// which can be used to customize the spawning of the actor.
    pub fn prepare<A>(&mut self, actor: A) -> SpawnBuilder<A, impl FnOnce() -> A>
    where
        A: Actor + 'static,
    {
        SpawnBuilder { system: self, capacity: None, addr: None, factory: move || actor }
    }

    /// Similar to `prepare`, but an actor factory is passed instead
    /// of an [`Actor`] itself. This is used when an actor needs to be
    /// created on its own thread instead of the calling thread.
    /// Returns a [`SpawnBuilder`] which can be used to customize the
    /// spawning of the actor.
    pub fn prepare_fn<A, F>(&mut self, factory: F) -> SpawnBuilder<A, F>
    where
        A: Actor + 'static,
        F: FnOnce() -> A + Send + 'static,
    {
        SpawnBuilder { system: self, capacity: None, addr: None, factory }
    }

    /// Spawn a normal [`Actor`] in the system, returning its address when successful.
    pub fn spawn<A>(&mut self, actor: A) -> Result<Addr<A>, ActorError>
    where
        A: Actor + Send + 'static,
    {
        self.prepare(actor).spawn()
    }

    pub fn run_once<F>(&mut self, delay: Duration, callback: F) -> ScheduleToken
    where
        F: FnOnce(ScheduleToken) + Send + 'static,
    {
        run_once(&mut self.timer_handle.timer_ref(), delay, callback)
    }

    pub fn run_recurring<F>(
        &mut self,
        delay: Duration,
        interval: Duration,
        callback: F,
    ) -> ScheduleToken
    where
        F: FnMut(ScheduleToken) -> TimerControlFlow + Send + 'static,
    {
        run_recurring(&mut self.timer_handle.timer_ref(), delay, interval, callback)
    }

    pub fn cancel_timer(&mut self, schedule_token: ScheduleToken) {
        let mut timer = self.timer_handle.timer_ref();

        timer.cancel(schedule_token);
    }

    /// Spawn a normal Actor in the system, using a factory that produces an [`Actor`],
    /// and an address that will be assigned to the Actor.
    ///
    /// This method is useful if you need to model circular dependencies between `Actor`s.
    fn spawn_fn_with_addr<F, A>(&mut self, factory: F, addr: Addr<A>) -> Result<(), ActorError>
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
        let timer_ref = self.timer_handle.timer_ref();

        let mut context = Context {
            system_handle: system_handle.clone(),
            myself: addr.clone(),
            timer: timer_ref,
        };
        let control_addr = addr.control_tx.clone();

        let thread_handle = thread::Builder::new()
            .name(A::name().into())
            .spawn(move || {
                let mut actor = factory();

                actor.started(&mut context);
                debug!("[{}] started actor: {}", system_handle.name, A::name());

                let actor_result =
                    Self::run_actor_select_loop(actor, addr, &mut context, &system_handle);
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
    fn block_on<A>(&mut self, mut actor: A, addr: Addr<A>) -> Result<(), ActorError>
    where
        A: Actor,
    {
        // Prevent race condition of spawn and shutdown.
        if !self.is_running() {
            return Err(ActorError::SystemStopped { actor_name: A::name() });
        }

        let system_handle = &self.handle;
        let timer_ref = self.timer_handle.timer_ref();

        let mut context = Context {
            system_handle: system_handle.clone(),
            myself: addr.clone(),
            timer: timer_ref,
        };

        self.handle.registry.lock().push(RegistryEntry::CurrentThread(addr.control_tx.clone()));

        actor.started(&mut context);
        debug!("[{}] started actor: {}", system_handle.name, A::name());
        Self::run_actor_select_loop(actor, addr, &mut context, system_handle)?;

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
        context: &mut Context<A>,
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
                            actor.stopped(context);
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
                            if let Err(err) = actor.handle(context, msg) {
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
        // TODO(bschwind) - Shut down the timer thread
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

                    if let Err(e) = entry.control_addr().send(Control::Stop) {
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
    CurrentThread(Sender<Control>),
    BackgroundThread(Sender<Control>, thread::JoinHandle<Result<(), ActorError>>),
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

    fn control_addr(&mut self) -> &mut Sender<Control> {
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
    // 'static required to create trait object in Addr, https://stackoverflow.com/q/29740488/4345715
    type Message: Send + 'static;
    /// The type to return on error in the handle method.
    type Error: std::fmt::Debug;

    /// The primary function of this trait, allowing an actor to handle incoming messages of a certain type.
    fn handle(
        &mut self,
        context: &mut Context<Self>,
        message: Self::Message,
    ) -> Result<(), Self::Error>;

    /// The name of the Actor - used only for logging/debugging.
    fn name() -> &'static str;

    /// An optional callback when the Actor has been started.
    fn started(&mut self, _context: &mut Context<Self>) {}

    /// An optional callback when the Actor has been stopped.
    fn stopped(&mut self, _context: &mut Context<Self>) {}
}

pub struct Addr<A: Actor + ?Sized> {
    recipient: Recipient<A::Message>,
    message_rx: Receiver<A::Message>,
    control_rx: Receiver<Control>,
}

impl<A: Actor> Default for Addr<A> {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_CHANNEL_CAPACITY)
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
        let (control_tx, control_rx) = channel::bounded(DEFAULT_CHANNEL_CAPACITY);

        let message_tx = Arc::new(message_tx);
        Self { recipient: Recipient { message_tx, control_tx }, message_rx, control_rx }
    }

    /// "Genericize" an address to, rather than point to a specific actor,
    /// be applicable to any actor that handles a given message-response type.
    /// Allows you to create recipient not only of `A::Message`, but of any `M: Into<A::Message>`.
    pub fn recipient<M: Into<A::Message>>(&self) -> Recipient<M> {
        Recipient {
            // Each level of boxing adds one .into() call, so box here to convert A::Message to M.
            message_tx: Arc::new(self.recipient.message_tx.clone()),
            control_tx: self.recipient.control_tx.clone(),
        }
    }
}

/// Similar to [`Addr`], but rather than pointing to a specific actor,
/// it is typed for any actor that handles a given message-response type.
pub struct Recipient<M> {
    message_tx: Arc<dyn SenderTrait<M>>,
    control_tx: Sender<Control>,
}

// #[derive(Clone)] adds Clone bound to M, which is not necessary.
// https://github.com/rust-lang/rust/issues/26925
impl<M> Clone for Recipient<M> {
    fn clone(&self) -> Self {
        Self { message_tx: self.message_tx.clone(), control_tx: self.control_tx.clone() }
    }
}

impl<M> Recipient<M> {
    /// Non-blocking call to send a message. Use this if you need to react when
    /// the channel is full. See [`SendResultExt`] trait for convenient handling of errors.
    pub fn send(&self, message: M) -> Result<(), SendError> {
        self.message_tx.try_send(message).map_err(SendError::from)
    }

    /// The remaining capacity for the message channel.
    pub fn remaining_capacity(&self) -> Option<usize> {
        let message_tx = &self.message_tx as &dyn SenderTrait<M>;
        message_tx.capacity().map(|capacity| capacity - message_tx.len())
    }
}

pub trait SendResultExt {
    /// Don't return an `Err` when the recipient is at full capacity, run `func` in such a case instead.
    fn on_full<F: FnOnce()>(self, func: F) -> Result<(), DisconnectedError>;

    /// Don't return an `Err` when the recipient is at full capacity.
    fn ignore_on_full(self) -> Result<(), DisconnectedError>;
}

impl SendResultExt for Result<(), SendError> {
    fn on_full<F: FnOnce()>(self, callback: F) -> Result<(), DisconnectedError> {
        self.or_else(|e| match e {
            SendError::Full => {
                callback();
                Ok(())
            },
            _ => Err(DisconnectedError),
        })
    }

    fn ignore_on_full(self) -> Result<(), DisconnectedError> {
        self.on_full(|| ())
    }
}

/// Internal trait to generalize over [`Sender`].
trait SenderTrait<M>: Send + Sync {
    fn try_send(&self, message: M) -> Result<(), SendError>;

    fn len(&self) -> usize;

    fn capacity(&self) -> Option<usize>;
}

/// [`SenderTrait`] is implemented for concrete crossbeam [`Sender`].
impl<M: Send> SenderTrait<M> for Sender<M> {
    fn try_send(&self, message: M) -> Result<(), SendError> {
        self.try_send(message).map_err(SendError::from)
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn capacity(&self) -> Option<usize> {
        self.capacity()
    }
}

/// [`SenderTrait`] is also implemented for boxed version of itself, incluling M -> N conversion.
impl<M: Into<N>, N> SenderTrait<M> for Arc<dyn SenderTrait<N>> {
    fn try_send(&self, message: M) -> Result<(), SendError> {
        self.deref().try_send(message.into())
    }

    fn len(&self) -> usize {
        self.deref().len()
    }

    fn capacity(&self) -> Option<usize> {
        self.deref().capacity()
    }
}

#[cfg(test)]
mod tests {
    use std::{rc::Rc, time::Duration};

    use super::*;

    struct TestActor;
    impl Actor for TestActor {
        type Error = ();
        type Message = usize;

        fn name() -> &'static str {
            "TestActor"
        }

        fn handle(&mut self, _: &mut Context<Self>, message: usize) -> Result<(), ()> {
            println!("message: {}", message);

            Ok(())
        }

        fn started(&mut self, _: &mut Context<Self>) {
            println!("started");
        }

        fn stopped(&mut self, _: &mut Context<Self>) {
            println!("stopped");
        }
    }

    #[test]
    fn it_works() {
        let mut system = System::new("hi");
        let address = system.spawn(TestActor).unwrap();
        let _ = system.spawn(TestActor).unwrap();
        let _ = system.spawn(TestActor).unwrap();
        let _ = system.spawn(TestActor).unwrap();
        let _ = system.spawn(TestActor).unwrap();
        address.send(1337usize).unwrap();
        address.send(666usize).unwrap();
        address.send(1usize).unwrap();
        thread::sleep(Duration::from_millis(100));

        system.shutdown().unwrap();
        thread::sleep(Duration::from_millis(100));
    }

    #[test]
    fn test_ignore_on_full() {
        let mut system = System::new("hi");
        let address = system.prepare(TestActor).with_capacity(1).spawn().unwrap();
        address.send(1337usize).unwrap();
        assert!(address.send(666usize).is_err());
        address.send(666usize).ignore_on_full().unwrap();

        thread::sleep(Duration::from_millis(100));

        system.shutdown().unwrap();
        thread::sleep(Duration::from_millis(100));
    }

    #[test]
    fn send_constraints() {
        #[derive(Default)]
        struct LocalActor(Rc<()>);
        impl Actor for LocalActor {
            type Error = ();
            type Message = ();

            fn name() -> &'static str {
                "LocalActor"
            }

            fn handle(&mut self, _: &mut Context<Self>, _: ()) -> Result<(), ()> {
                Ok(())
            }

            /// We just need this test to compile, not run.
            fn started(&mut self, ctx: &mut Context<Self>) {
                std::thread::sleep(std::time::Duration::from_millis(100));
                ctx.system_handle.shutdown().unwrap();
            }
        }

        let mut system = System::new("main");

        // Allowable, as the struct will be created on the new thread.
        let _ = system.prepare_fn(LocalActor::default).spawn().unwrap();

        // Allowable, as the struct will be run on the current thread.
        let _ = system.prepare(LocalActor::default()).run_and_block().unwrap();

        system.shutdown().unwrap();
    }
}
