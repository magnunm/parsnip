use anyhow;
use parsnip::{self, Message, Signature, Task};
use serde_json;
use ulid::Ulid;

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
    let mut app = parsnip::App::new();

    app.register_task::<HelloWorldTask>();

    let signature = Signature::<HelloWorldTask> { arg: () };
    let message = Message {
        task_id: HelloWorldTask::ID.into(),
        signature: serde_json::to_string(&signature)?,
        id: Ulid::new().to_string(),
    };

    app.handle_message(&serde_json::to_string(&message)?)?;

    Ok(())
}
