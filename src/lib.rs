pub mod broker;
pub mod brokers;
pub mod messages;
mod runner;
pub mod task;
pub mod worker;

use broker::{Broker, WorkerInfo};
use messages::{Command, Message, ResultMessage};
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

    /// Queue a task for pickup by a worker.
    ///
    /// Returns the signature_id for the task invocation, which can be used to
    /// lookup the result of running the task.
    pub fn queue_task<T: Task + 'static>(&self, arg: T::ArgumentType) -> Result<String, Error> {
        if !self.task_runner_builders.contains_key(T::ID.into()) {
            anyhow::bail!(
                "Can not queue task with ID '{}' as it is not registered.",
                T::ID
            );
        }

        let signature_id = Ulid::new().to_string();
        let signature = Signature::<T> {
            arg,
            id: signature_id.clone(),
        };
        self.broker
            .push_message(&Message {
                task_id: T::ID.into(),
                signature: serde_json::to_string(&signature)?,
            })
            .context("Failed to put task invocation on the queue.")?;
        Ok(signature_id)
    }

    pub fn get_task_result(&self, signatrue_id: &str) -> Result<Option<ResultMessage>, Error> {
        self.broker.get_result(signatrue_id)
    }

    pub fn queue_command(&self, command: &Command, worker_id: &str) -> Result<(), Error> {
        self.broker.push_command(command, worker_id)
    }

    pub fn get_worker_info(&self, worker_id: &str) -> Result<Option<WorkerInfo>, Error> {
        self.broker.get_worker_info(worker_id)
    }

    pub fn list_workers(&self) -> Result<Option<Vec<WorkerInfo>>, Error> {
        self.broker.all_workers()
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
        self.broker.store_result(result)
    }

    fn update_worker_info(&self, info: WorkerInfo) -> Result<(), Error> {
        self.broker.update_worker_info(info)
    }

    fn remove_worker_info(&self, worker_id: &str) -> Result<(), Error> {
        self.broker.remove_worker_info(worker_id)
    }
}
