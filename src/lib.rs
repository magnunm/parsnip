use anyhow::Error;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub task_id: String,
    pub signature: String,
    pub id: String,
}

#[derive(Serialize, Deserialize)]
pub struct Signature<T>
where
    T: Task,
{
    pub arg: T::ArgumentType,
}

impl<T> Signature<T>
where
    T: Task,
{
    pub fn from_serialized(signature: String) -> Result<Self, Error> {
        let result: Self = serde_json::from_str(&signature)?;
        Ok(result)
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

pub trait TaskRunnerTrait {
    fn run_task(&self, app: &App) -> Result<(), Error>;
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

impl<T> TaskRunnerTrait for TaskRunner<T>
where
    T: Task,
{
    fn run_task(&self, app: &App) -> Result<(), Error> {
        // TODO: Do something with task result
        T::run(&self.task.signature().arg);
        Ok(())
    }
}

pub type TaskRunnerBuilderResult = Result<Box<dyn TaskRunnerTrait>, Error>;
pub type TaskRunnerBuilder = Box<dyn Fn(String) -> TaskRunnerBuilderResult>;

pub fn build_task_runner<T: Task + 'static>(
    serialized_signature: String,
) -> TaskRunnerBuilderResult {
    let signature = Signature::<T>::from_serialized(serialized_signature)?;
    let task = T::from_signature(signature);
    Ok(Box::new(TaskRunner::<T>::new(task)))
}

pub struct App {
    task_runner_builders: HashMap<String, TaskRunnerBuilder>,
}

impl App {
    pub fn new() -> Self {
        Self {
            task_runner_builders: HashMap::new(),
        }
    }

    pub fn register_task<T: Task + 'static>(&mut self) {
        self.task_runner_builders
            .insert(T::ID.into(), Box::new(build_task_runner::<T>));
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

        task_runner.run_task(&self)?;

        Ok(())
    }
}
