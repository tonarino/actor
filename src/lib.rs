#![warn(clippy::all, clippy::clone_on_ref_ptr)]

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
//!     type Context = Context<Self::Message>;
//!     type Error = ();
//!     type Message = usize;
//!
//!     fn name() -> &'static str {
//!         "TestActor"
//!     }
//!
//!     fn handle(&mut self, _context: &mut Self::Context, message: Self::Message) -> Result<(), ()> {
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
//! `tonari-actor` also provides some extensions on top of the basic actor functionality:
//!
//! # Timing Message Delivery
//!
//! On top of [`Context::set_deadline()`] and [`Actor::deadline_passed()`] building blocks there
//! is a higher level abstraction for delayed and recurring messages in the [`timed`] module.
//!
//! # Publisher/subscriber Event System
//!
//! For cases where you want a global propagation of "events",
//! you can implement the [`Event`] trait for your event type and then use [`Context::subscribe()`]
//! and [`SystemHandle::publish()`] methods.
//!
//! Keep in mind that the event system has an additional requirement that the event type needs to be
//! [`Clone`] and is not intended to be high-throughput. Run the `pub_sub` benchmark to get an idea.

use flume::{select::SelectError, Receiver, RecvError, Selector, Sender};
use log::*;
use parking_lot::{Mutex, RwLock};
use std::{
    any::TypeId,
    collections::HashMap,
    fmt,
    ops::Deref,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

pub mod timed;

#[cfg(test)]
pub mod testing;

/// Capacity of the control channel (used to deliver [Control] messages).
const CONTROL_CHANNEL_CAPACITY: usize = 5;

#[derive(Debug)]
pub enum ActorError {
    /// The system has stopped, and a new actor can not be started.
    SystemStopped { actor_name: &'static str },
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

/// Failures that can occur when sending a message to an actor.
#[derive(Debug, Clone, Copy)]
pub struct SendError {
    /// The name of the intended recipient.
    pub recipient_name: &'static str,
    /// The priority assigned to the message that could not be sent.
    pub priority: Priority,
    /// The reason why sending has failed.
    pub reason: SendErrorReason,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let recipient_name = self.recipient_name;
        let priority = self.priority;
        match self.reason {
            SendErrorReason::Full => {
                write!(
                    f,
                    "The capacity of {}'s {:?}-priority channel is full.",
                    recipient_name, priority
                )
            },
            SendErrorReason::Disconnected => DisconnectedError { recipient_name, priority }.fmt(f),
        }
    }
}

impl std::error::Error for SendError {}

/// Error publishing an event.
#[derive(Debug)]
pub struct PublishError(pub Vec<SendError>);

impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let error_strings: Vec<String> = self.0.iter().map(ToString::to_string).collect();
        write!(
            f,
            "Failed to deliver an event to {} subscribers: {}.",
            self.0.len(),
            error_strings.join(", ")
        )
    }
}

impl std::error::Error for PublishError {}

/// The actor message channel is disconnected.
#[derive(Debug, Clone, Copy)]
pub struct DisconnectedError {
    /// The name of the intended recipient.
    pub recipient_name: &'static str,
    /// The priority assigned to the message that could not be sent.
    pub priority: Priority,
}

impl fmt::Display for DisconnectedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "The recipient of the message ({}) no longer exists.", self.recipient_name)
    }
}

impl std::error::Error for DisconnectedError {}

/// Reasons why sending a message to an actor can fail.
#[derive(Debug, Clone, Copy)]
pub enum SendErrorReason {
    /// The channel's capacity is full.
    Full,
    /// The recipient of the message no longer exists.
    Disconnected,
}

impl<M> From<flume::TrySendError<M>> for SendErrorReason {
    fn from(orig: flume::TrySendError<M>) -> Self {
        match orig {
            flume::TrySendError::Full(_) => Self::Full,
            flume::TrySendError::Disconnected(_) => Self::Disconnected,
        }
    }
}

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
type EventCallback = Box<dyn Fn(&dyn std::any::Any) -> Result<(), SendError> + Send + Sync>;

#[derive(Default)]
pub struct SystemCallbacks {
    pub preshutdown: Option<SystemCallback>,
    pub postshutdown: Option<SystemCallback>,
}

#[derive(Debug, Default, PartialEq)]
enum SystemState {
    /// The system is running and able to spawn new actors, or be asked to shut down
    #[default]
    Running,

    /// The system is in the process of shutting down, actors cannot be spawned
    /// or request for the system to shut down again
    ShuttingDown,

    /// The system has finished shutting down and is no longer running.
    /// All actors have stopped and their threads have been joined. No actors
    /// may be spawned at this point.
    Stopped,
}

/// A marker trait for types which participate in the publish-subscribe system
/// of the actor framework.
pub trait Event: Clone + std::any::Any + 'static {}

#[derive(Default)]
struct EventSubscribers {
    events: HashMap<TypeId, Vec<EventCallback>>,
}

/// Contains the "metadata" of the system, including information about the registry
/// of actors currently existing within the system.
#[derive(Default, Clone)]
pub struct SystemHandle {
    name: String,
    registry: Arc<Mutex<Vec<RegistryEntry>>>,
    system_state: Arc<RwLock<SystemState>>,
    callbacks: Arc<SystemCallbacks>,

    event_subscribers: Arc<RwLock<EventSubscribers>>,
}

/// An execution context for a specific actor. Specifically, this is useful for managing
/// the lifecycle of itself (through the `myself` field) and other actors via the `SystemHandle`
/// provided. A time-based deadline for receiving a message can be set using
/// [`Self::set_deadline()`] and friends.
pub struct Context<M> {
    pub system_handle: SystemHandle,
    pub myself: Recipient<M>,
    receive_deadline: Option<Instant>,
}

impl<M> Context<M> {
    fn new(system_handle: SystemHandle, myself: Recipient<M>) -> Self {
        Self { system_handle, myself, receive_deadline: None }
    }

    /// Get the deadline previously set using [`Self::set_deadline()`] or [`Self::set_timeout()`].
    /// The deadline is cleared just before [`Actor::deadline_passed()`] is called.
    pub fn deadline(&self) -> &Option<Instant> {
        &self.receive_deadline
    }

    /// Schedule a future one-shot call to [`Actor::deadline_passed()`], or cancel the schedule.
    /// A deadline in the past is considered to expire right in the next iteration (possibly after
    /// receiving new messages).
    pub fn set_deadline(&mut self, deadline: Option<Instant>) {
        self.receive_deadline = deadline;
    }

    /// Schedule or cancel a call to [`Actor::deadline_passed()`] after `timeout` from now.
    /// Convenience variant of [`Self::set_deadline()`].
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.set_deadline(timeout.map(|t| Instant::now() + t));
    }

    /// Subscribe current actor to event of type `E`. This is part of the event system. You don't
    /// need to call this method to receive direct messages sent using [`Addr`] and [`Recipient`].
    ///
    /// Note that subscribing twice to the same event would result in duplicate events -- no
    /// de-duplication of subscriptions is performed.
    pub fn subscribe<E: Event + Into<M>>(&self)
    where
        M: 'static,
    {
        self.system_handle.subscribe_recipient::<M, E>(self.myself.clone());
    }
}

/// Capacity of actor's normal- and high-priority inboxes.
/// For each inbox type, `None` signifies default capacity of given actor. Converts from [`usize`].
#[derive(Clone, Copy, Debug, Default)]
pub struct Capacity {
    pub normal: Option<usize>,
    pub high: Option<usize>,
}

impl Capacity {
    /// Set capacity of the normal priority channel, and use default for the high priority one.
    pub fn of_normal_priority(capacity: usize) -> Self {
        Self { normal: Some(capacity), ..Default::default() }
    }

    /// Set capacity of the high priority channel, and use default for the normal priority one.
    pub fn of_high_priority(capacity: usize) -> Self {
        Self { high: Some(capacity), ..Default::default() }
    }
}

/// Set capacity of both normal and high priority channels to the same amount of messages.
impl From<usize> for Capacity {
    fn from(capacity: usize) -> Self {
        Self { normal: Some(capacity), high: Some(capacity) }
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
    capacity: Capacity,
    addr: Option<Addr<A>>,
    factory: F,
}

impl<'a, A: 'static + Actor<Context = Context<<A as Actor>::Message>>, F: FnOnce() -> A>
    SpawnBuilder<'a, A, F>
{
    /// Specify a capacity for the actor's receiving channel. Accepts [`Capacity`] or [`usize`].
    ///
    /// Ignored when `.with_addr()` is used at the same time.
    pub fn with_capacity(self, capacity: impl Into<Capacity>) -> Self {
        Self { capacity: capacity.into(), ..self }
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
        let capacity = self.capacity;
        let addr = self.addr.unwrap_or_else(|| Addr::with_capacity(capacity));

        self.system.block_on(factory(), addr)
    }
}

impl<
        'a,
        A: 'static + Actor<Context = Context<<A as Actor>::Message>>,
        F: FnOnce() -> A + Send + 'static,
    > SpawnBuilder<'a, A, F>
{
    /// Spawn this Actor into a new thread managed by the [`System`].
    pub fn spawn(self) -> Result<Addr<A>, ActorError> {
        let factory = self.factory;
        let capacity = self.capacity;
        let addr = self.addr.unwrap_or_else(|| Addr::with_capacity(capacity));

        self.system.spawn_fn_with_addr(factory, addr.clone()).map(move |_| addr)
    }
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

    /// Prepare an actor to be spawned. Returns a [`SpawnBuilder`]
    /// which can be used to customize the spawning of the actor.
    pub fn prepare<A>(&mut self, actor: A) -> SpawnBuilder<A, impl FnOnce() -> A>
    where
        A: Actor + 'static,
    {
        SpawnBuilder {
            system: self,
            capacity: Default::default(),
            addr: None,
            factory: move || actor,
        }
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
        SpawnBuilder { system: self, capacity: Default::default(), addr: None, factory }
    }

    /// Spawn a normal [`Actor`] in the system, returning its address when successful.
    pub fn spawn<A>(&mut self, actor: A) -> Result<Addr<A>, ActorError>
    where
        A: Actor<Context = Context<<A as Actor>::Message>> + Send + 'static,
    {
        self.prepare(actor).spawn()
    }

    /// Spawn a normal Actor in the system, using a factory that produces an [`Actor`],
    /// and an address that will be assigned to the Actor.
    ///
    /// This method is useful if you need to model circular dependencies between `Actor`s.
    fn spawn_fn_with_addr<F, A>(&mut self, factory: F, addr: Addr<A>) -> Result<(), ActorError>
    where
        F: FnOnce() -> A + Send + 'static,
        A: Actor<Context = Context<<A as Actor>::Message>> + 'static,
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
        let mut context = Context::new(system_handle.clone(), addr.recipient.clone());
        let control_addr = addr.control_tx.clone();

        let thread_handle = thread::Builder::new()
            .name(A::name().into())
            .spawn(move || {
                let mut actor = factory();

                actor.started(&mut context);
                debug!("[{}] started actor: {}", system_handle.name, A::name());

                Self::run_actor_select_loop(actor, addr, &mut context, &system_handle);
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
        A: Actor<Context = Context<<A as Actor>::Message>>,
    {
        // Prevent race condition of spawn and shutdown.
        if !self.is_running() {
            return Err(ActorError::SystemStopped { actor_name: A::name() });
        }

        let system_handle = &self.handle;
        let mut context = Context::new(system_handle.clone(), addr.recipient.clone());

        self.handle.registry.lock().push(RegistryEntry::CurrentThread(addr.control_tx.clone()));

        actor.started(&mut context);
        debug!("[{}] started actor: {}", system_handle.name, A::name());
        Self::run_actor_select_loop(actor, addr, &mut context, system_handle);

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
        context: &mut Context<A::Message>,
        system_handle: &SystemHandle,
    ) where
        A: Actor<Context = Context<<A as Actor>::Message>>,
    {
        /// What can be received during one actor event loop.
        enum Received<M> {
            Control(Control),
            Message(M),
            Timeout,
        }

        loop {
            // We don't handle the messages (control and actor's) directly in .recv(), that would
            // lead to mutably borrowing actor multiple times. Read into intermediate enum instead.
            // The order of .recv() calls is significant and determines priority.
            let selector = Selector::new()
                .recv(&addr.control_rx, |msg| match msg {
                    Ok(control) => Received::Control(control),
                    Err(RecvError::Disconnected) => {
                        panic!("We keep control_tx alive through addr, should not happen.")
                    },
                })
                .recv(&addr.priority_rx, |msg| match msg {
                    Ok(msg) => Received::Message(msg),
                    Err(RecvError::Disconnected) => {
                        panic!("We keep message_tx alive through addr, should not happen.")
                    },
                })
                .recv(&addr.message_rx, |msg| match msg {
                    Ok(msg) => Received::Message(msg),
                    Err(RecvError::Disconnected) => {
                        panic!("We keep message_tx alive through addr, should not happen.")
                    },
                });

            // Wait for some event to happen, with a timeout if set.
            let received = if let Some(deadline) = context.receive_deadline {
                match selector.wait_deadline(deadline) {
                    Ok(received) => received,
                    Err(SelectError::Timeout) => Received::Timeout,
                }
            } else {
                selector.wait()
            };

            // Process the event. Returning ends actor loop, the normal operation is to fall through.
            match received {
                Received::Control(Control::Stop) => {
                    actor.stopped(context);
                    debug!("[{}] stopped actor: {}", system_handle.name, A::name());
                    return;
                },
                Received::Message(msg) => {
                    trace!("[{}] message received by {}", system_handle.name, A::name());
                    if let Err(err) = actor.handle(context, msg) {
                        error!(
                            "[{}] {} handle error: {:?}, shutting down.",
                            system_handle.name,
                            A::name(),
                            err
                        );
                        let _ = system_handle.shutdown();

                        return;
                    }
                },
                Received::Timeout => {
                    let deadline = context.receive_deadline.take().expect("implied by timeout");
                    if let Err(err) = actor.deadline_passed(context, deadline) {
                        error!(
                            "[{}] {} deadline_passed error: {:?}, shutting down.",
                            system_handle.name,
                            A::name(),
                            err
                        );
                        let _ = system_handle.shutdown();

                        return;
                    }
                },
            }
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

                            let join_result = thread_handle.join().map_err(|e| {
                                error!("a panic inside actor thread {}: {:?}", actor_name, e)
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

    /// Subscribe given `recipient` to events of type `E`. See [`Context::subscribe()`].
    pub fn subscribe_recipient<M: 'static, E: Event + Into<M>>(&self, recipient: Recipient<M>) {
        let mut event_subscribers = self.event_subscribers.write();

        let subs = event_subscribers.events.entry(TypeId::of::<E>()).or_default();

        subs.push(Box::new(move |e| {
            if let Some(event) = e.downcast_ref::<E>() {
                let msg = event.clone();
                recipient.send(msg.into())?;
            }

            Ok(())
        }));
    }

    /// Publish an event. All actors that have previously subscribed to the type will receive it.
    ///
    /// When sending to some subscriber fails, others are still tried and vec of errors is returned.
    /// For direct, non-[`Clone`] or high-throughput messages please use [`Addr`] or [`Recipient`].
    pub fn publish<E: Event>(&self, event: E) -> Result<(), PublishError> {
        let event_subscribers = self.event_subscribers.read();
        if let Some(subs) = event_subscribers.events.get(&TypeId::of::<E>()) {
            let errors: Vec<SendError> = subs
                .iter()
                .filter_map(|subscriber_callback| subscriber_callback(&event).err())
                .collect();
            if !errors.is_empty() {
                return Err(PublishError(errors));
            }
        }

        Ok(())
    }

    pub fn is_running(&self) -> bool {
        *self.system_state.read() == SystemState::Running
    }
}

enum RegistryEntry {
    CurrentThread(Sender<Control>),
    BackgroundThread(Sender<Control>, thread::JoinHandle<()>),
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
    /// What kind of context this actor accepts. Usually [`Context<Self::Message>`].
    type Context;

    /// Default capacity of actor's normal-priority inbox unless overridden by `.with_capacity()`.
    const DEFAULT_CAPACITY_NORMAL: usize = 5;
    /// Default capacity of actor's high-priority inbox unless overridden by `.with_capacity()`.
    const DEFAULT_CAPACITY_HIGH: usize = 5;

    /// The primary function of this trait, allowing an actor to handle incoming messages of a certain type.
    fn handle(
        &mut self,
        context: &mut Self::Context,
        message: Self::Message,
    ) -> Result<(), Self::Error>;

    /// The name of the Actor - used only for logging/debugging.
    fn name() -> &'static str;

    /// Determine priority of a `message` before it is sent to this actor.
    /// Default implementation returns [`Priority::Normal`].
    fn priority(_message: &Self::Message) -> Priority {
        Priority::Normal
    }

    /// An optional callback when the Actor has been started.
    fn started(&mut self, _context: &mut Self::Context) {}

    /// An optional callback when the Actor has been stopped.
    fn stopped(&mut self, _context: &mut Self::Context) {}

    /// An optional callback when a deadline has passed.
    ///
    /// The deadline has to be set via [`Context::set_deadline()`] or [`Context::set_timeout()`]
    /// first. The instant to which the deadline was originally set is passed via the `deadline`
    /// argument; it is normally close to [`Instant::now()`], but can be later if the actor was busy.
    ///
    /// # Periodic tick example
    /// ```
    /// # use {std::{cmp::max, time::{Duration, Instant}}, tonari_actor::{Actor, Context}};
    /// # struct TickingActor;
    /// impl Actor for TickingActor {
    /// #    type Context = Context<Self::Message>;
    /// #    type Error = ();
    /// #    type Message = ();
    /// #    fn name() -> &'static str { "TickingActor" }
    /// #    fn handle(&mut self, _: &mut Self::Context, _: ()) -> Result<(), ()> { Ok(()) }
    ///     // ...
    ///
    ///     fn deadline_passed(&mut self, context: &mut Self::Context, deadline: Instant) -> Result<(), ()> {
    ///         // do_periodic_housekeeping();
    ///
    ///         // A: Schedule one second from now (even if delayed); drifting tick.
    ///         context.set_timeout(Some(Duration::from_secs(1)));
    ///
    ///         // B: Schedule one second from deadline; non-drifting tick.
    ///         context.set_deadline(Some(deadline + Duration::from_secs(1)));
    ///
    ///         // C: Schedule one second from deadline, but don't fire multiple times if delayed.
    ///         context.set_deadline(Some(max(deadline + Duration::from_secs(1), Instant::now())));
    ///
    ///         Ok(())
    ///     }
    /// }
    /// ```
    fn deadline_passed(
        &mut self,
        _context: &mut Self::Context,
        _deadline: Instant,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub struct Addr<A: Actor + ?Sized> {
    recipient: Recipient<A::Message>,
    priority_rx: Receiver<A::Message>,
    message_rx: Receiver<A::Message>,
    control_rx: Receiver<Control>,
}

impl<A: Actor> Default for Addr<A> {
    fn default() -> Self {
        Self::with_capacity(Capacity::default())
    }
}

impl<A: Actor> Clone for Addr<A> {
    fn clone(&self) -> Self {
        Self {
            recipient: self.recipient.clone(),
            priority_rx: self.priority_rx.clone(),
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
    /// Create address for an actor, specifying its inbox size. Accepts [`Capacity`] or [`usize`].
    pub fn with_capacity(capacity: impl Into<Capacity>) -> Self {
        let capacity: Capacity = capacity.into();
        let prio_capacity = capacity.high.unwrap_or(A::DEFAULT_CAPACITY_HIGH);
        let normal_capacity = capacity.normal.unwrap_or(A::DEFAULT_CAPACITY_NORMAL);

        let (priority_tx, priority_rx) = flume::bounded::<A::Message>(prio_capacity);
        let (message_tx, message_rx) = flume::bounded::<A::Message>(normal_capacity);
        let (control_tx, control_rx) = flume::bounded(CONTROL_CHANNEL_CAPACITY);

        let name = A::name();
        let message_tx = Arc::new(MessageSender {
            high: priority_tx,
            normal: message_tx,
            get_priority: A::priority,
            name,
        });
        Self {
            recipient: Recipient { message_tx, control_tx },
            priority_rx,
            message_rx,
            control_rx,
        }
    }
}

/// Urgency of a given message. All high-priority messages are delivered before normal priority.
#[derive(Clone, Copy, Debug)]
pub enum Priority {
    Normal,
    High,
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
        Self { message_tx: Arc::clone(&self.message_tx), control_tx: self.control_tx.clone() }
    }
}

impl<M> Recipient<M> {
    /// Send a message to an actor. Returns [`SendError`] if the channel is full; does not block.
    /// See [`SendResultExt`] trait for convenient handling of errors.
    pub fn send(&self, message: M) -> Result<(), SendError> {
        self.message_tx.try_send(message)
    }
}

impl<M: 'static> Recipient<M> {
    /// Convert a [`Recipient<M>`] (or [`Addr<A>`] through [`Deref`], where `A::Message = M`) into
    /// [`Recipient<N>`], where message `N` can be converted into `M`.
    ///
    /// In case of converting from [`Addr`], this erases the type of the actor and only preserves
    /// type of the message, allowing you to make actors more independent of each other.
    pub fn recipient<N: Into<M>>(&self) -> Recipient<N> {
        Recipient {
            // Each level of boxing adds one .into() call, so box here to convert A::Message to M.
            message_tx: Arc::new(Arc::clone(&self.message_tx)),
            control_tx: self.control_tx.clone(),
        }
    }
}

pub trait SendResultExt {
    /// Don't return an `Err` when the recipient is at full capacity, run `func(receiver_name)`
    /// in such a case instead. `receiver_name` is the name of the intended recipient.
    fn on_full<F: FnOnce(&'static str, Priority)>(self, func: F) -> Result<(), DisconnectedError>;

    /// Don't return an `Err` when the recipient is at full capacity.
    fn ignore_on_full(self) -> Result<(), DisconnectedError>;
}

impl SendResultExt for Result<(), SendError> {
    fn on_full<F: FnOnce(&'static str, Priority)>(
        self,
        callback: F,
    ) -> Result<(), DisconnectedError> {
        self.or_else(|e| match e {
            SendError { recipient_name, priority, reason: SendErrorReason::Full } => {
                callback(recipient_name, priority);
                Ok(())
            },
            SendError { recipient_name, priority, reason: SendErrorReason::Disconnected } => {
                Err(DisconnectedError { recipient_name, priority })
            },
        })
    }

    fn ignore_on_full(self) -> Result<(), DisconnectedError> {
        self.on_full(|_, _| ())
    }
}

/// Internal struct to encapsulate ability to send message with priority to an actor.
struct MessageSender<M> {
    high: Sender<M>,
    normal: Sender<M>,
    get_priority: fn(&M) -> Priority,
    /// Name of the actor we're sending to.
    name: &'static str,
}

/// Internal trait to generalize over [`Sender`].
trait SenderTrait<M>: Send + Sync {
    fn try_send(&self, message: M) -> Result<(), SendError>;
}

/// [`SenderTrait`] is implemented for our [`MessageSender`].
impl<M: Send> SenderTrait<M> for MessageSender<M> {
    fn try_send(&self, message: M) -> Result<(), SendError> {
        let priority = (self.get_priority)(&message);
        let sender = match priority {
            Priority::Normal => &self.normal,
            Priority::High => &self.high,
        };
        sender.try_send(message).map_err(|e| SendError {
            reason: e.into(),
            recipient_name: self.name,
            priority,
        })
    }
}

/// [`SenderTrait`] is also implemented for boxed version of itself, including M -> N conversion.
impl<M: Into<N>, N> SenderTrait<M> for Arc<dyn SenderTrait<N>> {
    fn try_send(&self, message: M) -> Result<(), SendError> {
        self.deref().try_send(message.into())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        rc::Rc,
        sync::atomic::{AtomicU32, Ordering},
        time::Duration,
    };

    use super::*;

    struct TestActor;
    impl Actor for TestActor {
        type Context = Context<Self::Message>;
        type Error = ();
        type Message = usize;

        fn name() -> &'static str {
            "TestActor"
        }

        fn handle(&mut self, _: &mut Self::Context, message: usize) -> Result<(), ()> {
            println!("message: {}", message);

            Ok(())
        }

        fn started(&mut self, _: &mut Self::Context) {
            println!("started");
        }

        fn stopped(&mut self, _: &mut Self::Context) {
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
            type Context = Context<Self::Message>;
            type Error = ();
            type Message = ();

            fn name() -> &'static str {
                "LocalActor"
            }

            fn handle(&mut self, _: &mut Self::Context, _: ()) -> Result<(), ()> {
                Ok(())
            }

            /// We just need this test to compile, not run.
            fn started(&mut self, ctx: &mut Self::Context) {
                ctx.system_handle.shutdown().unwrap();
            }
        }

        let mut system = System::new("main");

        // Allowable, as the struct will be created on the new thread.
        let _ = system.prepare_fn(LocalActor::default).spawn().unwrap();

        // Allowable, as the struct will be run on the current thread.
        system.prepare(LocalActor::default()).run_and_block().unwrap();

        system.shutdown().unwrap();
    }

    #[test]
    fn timeouts() {
        struct TimeoutActor {
            handle_count: Arc<AtomicU32>,
            timeout_count: Arc<AtomicU32>,
        }

        impl Actor for TimeoutActor {
            type Context = Context<Self::Message>;
            type Error = ();
            type Message = Option<Instant>;

            fn name() -> &'static str {
                "TimeoutActor"
            }

            fn handle(&mut self, ctx: &mut Self::Context, msg: Self::Message) -> Result<(), ()> {
                self.handle_count.fetch_add(1, Ordering::SeqCst);
                if msg.is_some() {
                    ctx.receive_deadline = msg;
                }
                Ok(())
            }

            fn deadline_passed(&mut self, _: &mut Self::Context, _: Instant) -> Result<(), ()> {
                self.timeout_count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let mut system = System::new("timeouts");
        let (handle_count, timeout_count) = (Default::default(), Default::default());
        let actor = TimeoutActor {
            handle_count: Arc::clone(&handle_count),
            timeout_count: Arc::clone(&timeout_count),
        };
        let addr = system.spawn(actor).unwrap();

        // Test that setting deadline to past triggers the deadline immediately.
        addr.send(Some(Instant::now().checked_sub(Duration::from_secs(1)).unwrap())).unwrap();
        thread::sleep(Duration::from_millis(10));
        assert_eq!(handle_count.load(Ordering::SeqCst), 1);
        assert_eq!(timeout_count.load(Ordering::SeqCst), 1);

        // Test the normal case.
        addr.send(Some(Instant::now() + Duration::from_millis(20))).unwrap();
        thread::sleep(Duration::from_millis(10));
        assert_eq!(handle_count.load(Ordering::SeqCst), 2);
        assert_eq!(timeout_count.load(Ordering::SeqCst), 1);
        thread::sleep(Duration::from_millis(20));
        assert_eq!(handle_count.load(Ordering::SeqCst), 2);
        assert_eq!(timeout_count.load(Ordering::SeqCst), 2);

        // Test that receiving a message doesn't reset the deadline.
        addr.send(Some(Instant::now() + Duration::from_millis(40))).unwrap();
        thread::sleep(Duration::from_millis(20));
        assert_eq!(handle_count.load(Ordering::SeqCst), 3);
        assert_eq!(timeout_count.load(Ordering::SeqCst), 2);
        addr.send(None).unwrap();
        thread::sleep(Duration::from_millis(30));
        assert_eq!(handle_count.load(Ordering::SeqCst), 4);
        assert_eq!(timeout_count.load(Ordering::SeqCst), 3);

        system.shutdown().unwrap();
    }

    #[test]
    fn errors() {
        let mut system = System::new("hi");
        let low_capacity_actor: Addr<TestActor> = Addr::with_capacity(1);
        // Convert to `Recipient` so that we don't keep the receiving side of `Addr` alive.
        let stopped_actor = system.spawn(TestActor).unwrap().recipient();

        low_capacity_actor.send(9).expect("one message should fit");
        let error = low_capacity_actor.send(123).unwrap_err();
        assert_eq!(
            error.to_string(),
            "The capacity of TestActor's Normal-priority channel is full."
        );
        assert_eq!(
            format!("{:?}", error),
            r#"SendError { recipient_name: "TestActor", priority: Normal, reason: Full }"#
        );

        system.shutdown().unwrap();

        let error = stopped_actor.send(456usize).unwrap_err();
        assert_eq!(error.to_string(), "The recipient of the message (TestActor) no longer exists.");
        assert_eq!(
            format!("{:?}", error),
            r#"SendError { recipient_name: "TestActor", priority: Normal, reason: Disconnected }"#
        );
    }

    #[test]
    fn message_priorities() {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();

        struct PriorityActor {
            received: Arc<Mutex<Vec<usize>>>,
        }

        impl Actor for PriorityActor {
            type Context = Context<Self::Message>;
            type Error = ();
            type Message = usize;

            fn handle(
                &mut self,
                context: &mut Self::Context,
                message: Self::Message,
            ) -> Result<(), Self::Error> {
                let mut received = self.received.lock();
                received.push(message);
                if received.len() >= 20 {
                    context.system_handle.shutdown().unwrap();
                }
                Ok(())
            }

            fn name() -> &'static str {
                "PriorityActor"
            }

            fn priority(message: &Self::Message) -> Priority {
                if *message >= 10 {
                    Priority::High
                } else {
                    Priority::Normal
                }
            }
        }

        let addr = Addr::with_capacity(10);
        let received = Arc::new(Mutex::new(Vec::<usize>::new()));

        // Send messages before even actor starts.
        for message in 0..20usize {
            addr.send(message).unwrap();
        }

        let mut system = System::new("priorities");
        system
            .prepare(PriorityActor { received: Arc::clone(&received) })
            .with_addr(addr)
            .run_and_block()
            .unwrap();

        assert_eq!(
            *received.lock(),
            [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        );
    }
}
