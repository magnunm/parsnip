use anyhow::Result;
use std::{ops::Drop, thread, time};
use ulid::Ulid;

use super::broker::{Broker, WorkerInfo, WorkerState};
use super::messages::Command;
use super::App;

const SLEEP_TIME: time::Duration = time::Duration::from_millis(500);

pub struct Worker<'a, B: Broker + 'static> {
    app: &'a App<'a, B>,
    pub id: String,
}

impl<'a, B: Broker + 'static> Worker<'a, B> {
    /// Create a new worker instance.
    ///
    /// Note that since this takes a reference to the `App` instance and registering a task
    /// requires mutating the `App` the borrow checker prevents any more tasks from being
    /// registered. All tasks must be registered by the time the worker is initialized.
    pub fn new(app: &'a App<'a, B>) -> Result<Self> {
        let id = Ulid::new().to_string();
        app.update_worker_info(WorkerInfo {
            state: WorkerState::Pending,
            id: id.clone(),
        })?;

        Ok(Self { app, id })
    }

    /// Listen for and process queued task messages.
    ///
    /// Continually check the queue for new messages and run the corresponding
    /// task. Checks the command queue before each new message and obeys the
    /// commands there. Keeps processing until receiving a stop command.
    pub fn listen_for_messages(&self) -> Result<()> {
        self.app.update_worker_info(WorkerInfo {
            id: self.id.clone(),
            state: WorkerState::Running,
        })?;

        loop {
            match self.app.broker.pop_command(&self.id)? {
                None => (),
                Some(Command::StopWorker) => {
                    break;
                }
            };

            match self.app.broker.pop_message()? {
                None => {
                    // If there are no messages on the queue, wait a little
                    // instead of immediately re-checking for messages.
                    thread::sleep(SLEEP_TIME);
                }
                Some(m) => self.app.handle_message(&m)?,
            }
        }

        self.app.update_worker_info(WorkerInfo {
            id: self.id.clone(),
            state: WorkerState::Stopped,
        })?;
        Ok(())
    }

    pub fn take_first_task_in_queue(&self) -> Result<()> {
        let message = self.app.broker.pop_message()?;
        match message {
            Some(m) => self.app.handle_message(&m),
            None => Err(anyhow::anyhow!("No messages in queue")),
        }
    }
}

impl<'a, B: Broker + 'static> Drop for Worker<'a, B> {
    fn drop(&mut self) {
        // Remove the worker from the worker register.
        // We don't want to panic here, so instead we just inform about the
        // failed cleanup. This should preferably be handled by a logging
        // solution.
        self.app.remove_worker_info(&self.id.clone()).unwrap_or_else(|_| {
            println!("Unable to remove worker info, worker ID {} will linger in worker register even though it is being dropped.", &self.id);
        });
    }
}
