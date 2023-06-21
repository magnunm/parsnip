use anyhow;
use parsnip::{
    self,
    broker::{Broker, WorkerInfo},
    messages::ResultMessage,
    messages::{Command, Message},
    task::Signature,
    task::Task,
    worker::Worker,
    App,
};
use serde_json;
use std::collections::{HashMap, LinkedList};
use std::sync::RwLock;

struct InMemoryTestBroker {
    pub task_results: RwLock<HashMap<String, ResultMessage>>,
    pub queue: RwLock<LinkedList<Message>>,
    pub command_queues: RwLock<HashMap<String, LinkedList<Command>>>,
    pub worker_register: RwLock<HashMap<String, WorkerInfo>>,
}

impl InMemoryTestBroker {
    fn new() -> Self {
        Self {
            task_results: RwLock::new(HashMap::new()),
            queue: RwLock::new(LinkedList::new()),
            command_queues: RwLock::new(HashMap::new()),
            worker_register: RwLock::new(HashMap::new()),
        }
    }
}

impl Broker for InMemoryTestBroker {
    fn push_message(&self, message: &Message) -> anyhow::Result<()> {
        self.queue
            .write()
            .expect("Failed to aquire lock")
            .push_back(message.clone());
        Ok(())
    }

    fn pop_message(&self) -> anyhow::Result<Option<Message>> {
        match self
            .queue
            .write()
            .expect("Failed to aquire lock")
            .pop_front()
        {
            Some(message) => Ok(Some(message)),
            None => Ok(None),
        }
    }

    fn push_command(
        &self,
        command: &parsnip::messages::Command,
        worker_id: &str,
    ) -> anyhow::Result<()> {
        if !self
            .command_queues
            .read()
            .expect("Failed to aquire lock")
            .contains_key(worker_id)
        {
            self.command_queues
                .write()
                .expect("Failed to aquire lock")
                .insert(worker_id.to_string(), LinkedList::new());
        }

        self.command_queues
            .write()
            .expect("Failed to aquire lock")
            .get_mut(worker_id)
            .expect("Linked list was not initialized when pushing a new command")
            .push_back(command.clone());
        Ok(())
    }

    fn pop_command(&self, worker_id: &str) -> anyhow::Result<Option<parsnip::messages::Command>> {
        match self
            .command_queues
            .write()
            .expect("Failed to aquire lock")
            .get_mut(worker_id)
        {
            None => Ok(None),
            Some(ll) => Ok(ll.pop_front()),
        }
    }

    fn store_result(&self, result_message: ResultMessage) -> anyhow::Result<()> {
        self.task_results
            .write()
            .expect("Failed to aquire lock")
            .insert(result_message.signature_id.clone(), result_message);
        Ok(())
    }

    fn get_result(&self, signature_id: &str) -> anyhow::Result<Option<ResultMessage>> {
        Ok(self
            .task_results
            .read()
            .expect("Failed to aquire lock")
            .get(signature_id)
            .map(ResultMessage::clone))
    }

    fn update_worker_info(&self, info: parsnip::broker::WorkerInfo) -> anyhow::Result<()> {
        self.worker_register
            .write()
            .expect("Failed to aquire lock")
            .insert(info.id.clone(), info);
        Ok(())
    }

    fn remove_worker_info(&self, worker_id: &str) -> anyhow::Result<()> {
        self.worker_register
            .write()
            .expect("Failed to aquire lock")
            .remove(worker_id);
        Ok(())
    }

    fn get_worker_info(
        &self,
        worker_id: &str,
    ) -> anyhow::Result<Option<parsnip::broker::WorkerInfo>> {
        Ok(self
            .worker_register
            .read()
            .expect("Failed to aquire lock")
            .get(worker_id)
            .map(WorkerInfo::clone))
    }

    fn all_workers(&self) -> anyhow::Result<Option<Vec<parsnip::broker::WorkerInfo>>> {
        Ok(Some(
            self.worker_register
                .read()
                .expect("Failed to aquire lock")
                .values()
                .map(|v| v.clone())
                .collect(),
        ))
    }
}

struct SummationTask {
    called_with_signature: Signature<Self>,
}

impl Task for SummationTask {
    type ArgumentType = Vec<usize>;
    type ReturnType = usize;

    const ID: &'static str = "SummationTask";

    fn from_signature(signature: Signature<Self>) -> Self {
        Self {
            called_with_signature: signature,
        }
    }

    fn run(arg: &Self::ArgumentType) -> Self::ReturnType {
        arg.iter().sum()
    }

    fn signature(&self) -> &Signature<Self> {
        &self.called_with_signature
    }
}

#[test]
fn test_running_task_from_message() -> anyhow::Result<()> {
    let mut broker = InMemoryTestBroker::new();
    let mut app = App::new(&mut broker);

    app.register_task::<SummationTask>();

    app.queue_task::<SummationTask>(vec![1, 2, 3])?;

    // Scope because the worker uses the mutable borrow of the broker above
    // when it is dropped.
    {
        let worker = Worker::new(&app)?;
        worker.take_first_task_in_queue()?;
    }

    assert_eq!(
        broker
            .task_results
            .read()
            .expect("Failed to aquire lock")
            .len(),
        1
    );
    let task_results = broker.task_results.read().expect("Failed to aquire lock");
    let first_task_result = task_results.values().next().unwrap();
    let return_value = serde_json::from_str::<usize>(&first_task_result.result)?;

    assert_eq!(return_value, 6); // = 1 + 2 + 3

    Ok(())
}
