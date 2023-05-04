use anyhow;
use parsnip::{self, broker::Broker, Message, ResultMessage, Signature, Task};
use serde_json;
use std::collections::LinkedList;
use std::sync::RwLock;
use ulid::Ulid;

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

    let signature = Signature::<HelloWorldTask> {
        arg: (),
        id: Ulid::new().to_string(),
    };
    let message = Message {
        task_id: HelloWorldTask::ID.into(),
        signature: serde_json::to_string(&signature)?,
    };

    app.handle_message(&serde_json::to_string(&message)?)?;

    Ok(())
}
