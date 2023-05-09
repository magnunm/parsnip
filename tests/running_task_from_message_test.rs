use anyhow;
use parsnip::{
    self, broker::Broker, messages::Message, messages::ResultMessage, task::Signature, task::Task,
    worker::Worker, App,
};
use serde_json;
use std::collections::LinkedList;
use std::sync::RwLock;

struct InMemoryTestBroker {
    pub task_results: RwLock<Vec<ResultMessage>>,
    pub queue: RwLock<LinkedList<Message>>,
}

impl InMemoryTestBroker {
    fn new() -> Self {
        Self {
            task_results: RwLock::new(Vec::new()),
            queue: RwLock::new(LinkedList::new()),
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

    fn pop_message(&self) -> anyhow::Result<Message> {
        match self
            .queue
            .write()
            .expect("Failed to aquire lock")
            .pop_front()
        {
            Some(message) => Ok(message),
            None => Err(anyhow::anyhow!("No messages in queue!")),
        }
    }

    fn store_result(&self, result_message: ResultMessage) -> anyhow::Result<()> {
        self.task_results
            .write()
            .expect("Failed to aquire lock")
            .push(result_message.clone());
        Ok(())
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

    let worker = Worker::new(&app);
    worker.take_first_task_in_queue()?;

    assert_eq!(
        broker
            .task_results
            .read()
            .expect("Failed to aquire lock")
            .len(),
        1
    );
    let task_results = broker.task_results.read().expect("Failed to aquire lock");
    let first_task_result = task_results.first().unwrap();
    let return_value = serde_json::from_str::<usize>(&first_task_result.result)?;

    assert_eq!(return_value, 6); // = 1 + 2 + 3

    Ok(())
}
