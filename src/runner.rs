use anyhow::Error;

use super::broker::Broker;
use super::messages::ResultMessage;
use super::task::{Signature, Task};
use super::App;

pub trait TaskRunnerTrait<B: Broker> {
    fn run_task(&self, app: &App<B>) -> Result<(), Error>;
}

pub type TaskRunnerBuilderResult<B> = Result<Box<dyn TaskRunnerTrait<B>>, Error>;
pub type TaskRunnerBuilder<B> = Box<dyn Fn(&str) -> TaskRunnerBuilderResult<B>>;

pub fn build_task_runner<T: Task + 'static, B: Broker + 'static>(
    serialized_signature: &str,
) -> TaskRunnerBuilderResult<B> {
    let signature = Signature::<T>::from_serialized(serialized_signature)?;
    let task = T::from_signature(signature);
    Ok(Box::new(TaskRunner::<T>::new(task)))
}

struct TaskRunner<T>
where
    T: Task,
{
    task: T,
}

impl<T> TaskRunner<T>
where
    T: Task,
{
    fn new(task: T) -> Self {
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
