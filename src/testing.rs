use std::{fmt, thread, time::Duration};

use crate::System;

/// A helper wrapper to run test actors for a certain period.
///
/// # Example
///
/// ```ignore
/// use actor::testing::SystemThread;
///
/// SystemThread::run_for(Duration::from_millis(100), |system| {
///     system.spawn(SomeActor::new())?;
/// });
/// ```
pub struct SystemThread {
    inner: Option<thread::JoinHandle<()>>,
}

impl SystemThread {
    pub fn run_for<F, E: fmt::Debug>(duration: Duration, func: F) -> Self
    where
        F: FnOnce(&mut System) -> Result<(), E> + Send + 'static,
    {
        Self {
            inner: Some(thread::spawn(move || {
                let mut system = System::new("test");
                func(&mut system).unwrap();
                thread::sleep(duration);
                system.shutdown().unwrap();
            })),
        }
    }
}

impl Drop for SystemThread {
    fn drop(&mut self) {
        self.inner.take().unwrap().join().unwrap();
    }
}
