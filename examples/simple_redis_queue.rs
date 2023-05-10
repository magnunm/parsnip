use anyhow;
use parsnip::{self, brokers::redis::RedisBroker, task::Signature, task::Task, worker::Worker};
use std::env;

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
    let connect_url: String = env::args()
        .nth(1)
        .expect("No Redis connect URL passed the first an argument");

    let mut broker = RedisBroker::new(&connect_url)?;

    let mut app = parsnip::App::new(&mut broker);
    app.register_task::<HelloWorldTask>();

    let worker = Worker::new(&app);

    app.queue_task::<HelloWorldTask>(())?;
    worker.take_first_task_in_queue()?;

    Ok(())
}
