use anyhow::Error;

use super::broker::Broker;
use super::App;

pub struct Worker<'a, B: Broker> {
    app: &'a App<'a, B>,
}

impl<'a, B: Broker + 'static> Worker<'a, B> {
    /// Create a new worker instance.
    ///
    /// Note that since this takes a reference to the `App` instance and registering a task
    /// requires mutating the `App` the borrow checker prevents any more tasks from being
    /// registered. All tasks must be registered by the time the worker is initialized.
    pub fn new(app: &'a App<'a, B>) -> Self {
        Self { app }
    }

    pub fn take_first_task_in_queue(&self) -> Result<(), Error> {
        let message = self.app.broker.pop_message()?;
        match message {
            Some(m) => self.app.handle_message(&m),
            None => Err(anyhow::anyhow!("No messages in queue")),
        }
    }
}
