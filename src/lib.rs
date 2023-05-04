pub mod broker;

use broker::Broker;

use anyhow::Error;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone)]
pub struct Message {
    pub task_id: String,
    pub signature: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ResultMessage {
    pub signature_id: String,
    pub result: String,
}

#[derive(Serialize, Deserialize)]
pub struct Signature<T>
where
    T: Task,
{
    pub arg: T::ArgumentType,
    pub id: String,
}

impl<T> Signature<T>
where
    T: Task,
{
    pub fn from_serialized(signature: String) -> Result<Self, Error> {
        Ok(serde_json::from_str(&signature)?)
    }
}

pub trait Task: Sized
where
    Self::ArgumentType: Serialize,
    Self::ArgumentType: for<'a> Deserialize<'a>,
    Self::ReturnType: Serialize,
    Self::ReturnType: for<'a> Deserialize<'a>,
{
    type ArgumentType;
    type ReturnType;

    const ID: &'static str;

    fn from_signature(signature: Signature<Self>) -> Self;

    fn run(arg: &Self::ArgumentType) -> Self::ReturnType;

    /// Get the signature used to created the task instance
    fn signature(&self) -> &Signature<Self>;
}

pub trait TaskRunnerTrait<B: Broker> {
    fn run_task(&self, app: &App<B>) -> Result<(), Error>;
}

pub struct TaskRunner<T>
where
    T: Task,
{
    task: T,
}

impl<T> TaskRunner<T>
where
    T: Task,
{
    pub fn new(task: T) -> Self {
        Self { task }
    }
}

impl<T, B: Broker + 'static> TaskRunnerTrait<B> for TaskRunner<T>
where
    T: Task,
{
    fn run_task(&self, app: &App<B>) -> Result<(), Error> {
        let result = T::run(&self.task.signature().arg);
        app.store_task_result(ResultMessage {
            result: serde_json::to_string(&result)?,
            signature_id: self.task.signature().id.clone(),
        })?;
        Ok(())
    }
}

pub type TaskRunnerBuilderResult<B> = Result<Box<dyn TaskRunnerTrait<B>>, Error>;
pub type TaskRunnerBuilder<B> = Box<dyn Fn(String) -> TaskRunnerBuilderResult<B>>;

pub fn build_task_runner<T: Task + 'static, B: Broker + 'static>(
    serialized_signature: String,
) -> TaskRunnerBuilderResult<B> {
    let signature = Signature::<T>::from_serialized(serialized_signature)?;
    let task = T::from_signature(signature);
    Ok(Box::new(TaskRunner::<T>::new(task)))
}

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
            .insert(T::ID.into(), Box::new(build_task_runner::<T, B>));
    }

    pub fn handle_message(&self, message: &str) -> Result<(), Error> {
        let deserialized_message = serde_json::from_str::<Message>(message)?;
        let task_id = deserialized_message.task_id;

        let task_runner = match self.task_runner_builders.get(&task_id) {
            Some(task_runner_builder) => Ok(task_runner_builder(deserialized_message.signature)?),
            None => Err(anyhow::anyhow!(
                "Received message for unknown task {}",
                &task_id
            )),
        }?;

        task_runner.run_task(self)?;

        Ok(())
    }

    pub fn store_task_result(&self, result: ResultMessage) -> Result<(), Error> {
        self.broker.store_result(result)?;
        Ok(())
    }
}
