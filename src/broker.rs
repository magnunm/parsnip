use super::messages::{Command, Message, ResultMessage};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub enum WorkerState {
    Pending,
    Running,
    Stopped,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct WorkerInfo {
    pub state: WorkerState,
    pub id: String,
}

pub trait Broker {
    fn push_message(&self, message: &Message) -> Result<()>;

    fn pop_message(&self) -> Result<Option<Message>>;

    fn push_command(&self, command: &Command, worker_id: &str) -> Result<()>;

    fn pop_command(&self, worker_id: &str) -> Result<Option<Command>>;

    fn store_result(&self, result_message: ResultMessage) -> Result<()>;

    fn get_result(&self, signature_id: &str) -> Result<Option<ResultMessage>>;

    fn update_worker_info(&self, info: WorkerInfo) -> Result<()>;

    fn remove_worker_info(&self, worker_id: &str) -> Result<()>;

    fn get_worker_info(&self, worker_id: &str) -> Result<Option<WorkerInfo>>;

    fn all_workers(&self) -> Result<Option<Vec<WorkerInfo>>>;
}
