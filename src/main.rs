use anyhow;
use parsnip::{
    self, broker::Broker, messages::Message, messages::ResultMessage, task::Signature, task::Task,
    worker::Worker,
};
use std::collections::LinkedList;
use std::sync::RwLock;

struct InMemoryTestBroker {
    task_results: RwLock<Vec<ResultMessage>>,
    queue: RwLock<LinkedList<Message>>,
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

    fn store_result(&self, result_message: ResultMessage) -> anyhow::Result<()> {
        self.task_results
            .write()
            .expect("Failed to aquire lock")
            .push(result_message.clone());
        Ok(())
    }
}

struct HelloWorldTask {
    called_with_signature: Signature<Self>,
}

impl Task for HelloWorldTask {
    type ArgumentType = ();
    type ReturnType = ();

    const ID: &'static str = "HelloWorldTask";

    fn from_signature(signature: Signature<Self>) -> Self {
        Self {
            called_with_signature: signature,
        }
    }

    fn run(_: &Self::ArgumentType) -> Self::ReturnType {
        println!("Hello, World!");
    }

    fn signature(&self) -> &Signature<Self> {
        &self.called_with_signature
    }
}

fn main() -> Result<(), anyhow::Error> {
    let mut broker = InMemoryTestBroker::new();
    let mut app = parsnip::App::new(&mut broker);
    app.register_task::<HelloWorldTask>();

    let worker = Worker::new(&app);

    app.queue_task::<HelloWorldTask>(())?;
    worker.take_first_task_in_queue()?;

    Ok(())
}
