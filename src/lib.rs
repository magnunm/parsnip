pub mod broker;
pub mod messages;
mod runner;
pub mod task;
pub mod worker;

use broker::Broker;
use messages::{Message, ResultMessage};
use runner::TaskRunnerBuilder;
use task::{Signature, Task};

use anyhow::{Context, Error};
use serde_json;
use std::collections::HashMap;
use ulid::Ulid;

pub struct App<'a, B: Broker> {
    task_runner_builders: HashMap<String, TaskRunnerBuilder<B>>,
    broker: &'a B,
}

impl<'a, B: Broker + 'static> App<'a, B> {
    pub fn new(broker: &'a B) -> Self {
        Self {
            task_runner_builders: HashMap::new(),
            broker,
        }
    }

    pub fn register_task<T: Task + 'static>(&mut self) {
        self.task_runner_builders
            .insert(T::ID.into(), Box::new(runner::build_task_runner::<T, B>));
    }

    pub fn queue_task<T: Task + 'static>(&self, arg: T::ArgumentType) -> Result<(), Error> {
        if !self.task_runner_builders.contains_key(T::ID.into()) {
            anyhow::bail!(
                "Can not queue task with ID '{}' as it is not registered.",
                T::ID
            );
        }

        let signature = Signature::<T> {
            arg,
            id: Ulid::new().to_string(),
        };
        self.broker
            .push_message(&Message {
                task_id: T::ID.into(),
                signature: serde_json::to_string(&signature)?,
            })
            .context("Failed to put task invocation on the queue.")?;
        Ok(())
    }

    fn handle_message(&self, message: &Message) -> Result<(), Error> {
        let task_runner = match self.task_runner_builders.get(&message.task_id) {
            Some(task_runner_builder) => Ok(task_runner_builder(&message.signature)?),
            None => Err(anyhow::anyhow!(
                "Received message for unknown task ID '{}'.",
                &message.task_id
            )),
        }?;

        task_runner.run_task(self)?;

        Ok(())
    }

    fn store_task_result(&self, result: ResultMessage) -> Result<(), Error> {
        self.broker.store_result(result)?;
        Ok(())
    }
}
