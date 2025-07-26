use crate::Priority;
use std::{any::type_name, fmt};

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
}
