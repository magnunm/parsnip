use anyhow;
use parsnip::{
    self, brokers::redis::RedisBroker, messages::Command, task::Signature, task::Task,
    worker::Worker,
};
use std::{env, thread, time};

struct HelloWorldTask {
    called_with_signature: Signature<Self>,
}

impl Task for HelloWorldTask {
    type ArgumentType = ();
    type ReturnType = usize;

    const ID: &'static str = "HelloWorldTask";

    fn from_signature(signature: Signature<Self>) -> Self {
        Self {
            called_with_signature: signature,
        }
    }

    fn run(_: &Self::ArgumentType) -> Self::ReturnType {
        println!("Hello, World!");
        42
    }

    fn signature(&self) -> &Signature<Self> {
        &self.called_with_signature
    }
}

fn controller_main(connect_url: String) -> () {
    let mut broker = RedisBroker::new(&connect_url).expect("Can not connect to Redis");
    let mut app = parsnip::App::new(&mut broker);
    app.register_task::<HelloWorldTask>();

    println!("Controller thread: Queueing task");
    let signature_id = app.queue_task::<HelloWorldTask>(()).unwrap();

    println!("Controller thread: Polling for task run result");
    let mut result = app.get_task_result(&signature_id).unwrap();
    while result.is_none() {
        thread::sleep(time::Duration::from_millis(500));
        println!("Controller thread: Polling for task run result");
        result = app.get_task_result(&signature_id).unwrap();
    }

    println!(
        "Controller thread: Got task run result, {}",
        result.unwrap().result
    );

    println!("Controller thread: Looking up all registered workers");
    match app.list_workers().unwrap() {
        None => panic!(
            "The task was picked up and run by a worker, but none are registered. This is a bug."
        ),
        Some(v) => v.into_iter().for_each(|worker| {
            println!(
                "Controller thread: Sending command to stop worker {}",
                &worker.id
            );
            app.queue_command(&Command::StopWorker, &worker.id).unwrap();
        }),
    };
    println!("Controller thread: Done");
}

fn worker_main(connect_url: String) -> () {
    let mut broker = RedisBroker::new(&connect_url).expect("Can not connect to Redis");
    let mut app = parsnip::App::new(&mut broker);
    app.register_task::<HelloWorldTask>();

    let worker = Worker::new(&app).expect("Worker initalization failed");
    println!("Worker thread: Registered worker with ID {}", worker.id);
    println!("Worker thread: Listening for messages...");
    worker
        .listen_for_messages()
        .expect("Malfunction while handling messages.");
    println!("Worker thread: Done")
}

fn main() -> Result<(), anyhow::Error> {
    let connect_url: String = env::args()
        .nth(1)
        .expect("No Redis connect URL passed the first an argument");

    let connect_url_controller = connect_url.clone();
    let handle_controller_thread = thread::spawn(|| controller_main(connect_url_controller));

    let connect_url_worker = connect_url.clone();
    let handler_worker_thread = thread::spawn(|| worker_main(connect_url_worker));

    handle_controller_thread.join().unwrap();
    handler_worker_thread.join().unwrap();
    Ok(())
}
