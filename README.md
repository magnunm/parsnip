# Parsnip

An experiment creating a task queue for distributed, asynchronous computing in Rust.
Inspired by [Celery](https://docs.celeryq.dev/en/stable/).

## Examples

### `simple_redis_queue`

Spin up a Redis instance and pass the connection URL as the first argument.

```sh
cargo r --example simple_redis_queue 'redis://localhost:6379/1'
```

This example uses two threads: a "worker thread" and a "controller thread". The
controller queues tasks and commands that the worker picks up and executes. In
the example code the controller queues a single tasks to print "Hello, World!"
and return 42 and then waits for the worker to finish executing it. It then
commands the worker to stop before terminating. The worker thread listens for
messages and commands and executes them until it gets a stop command. It then
terminates. A typical run looks something like this:

```
Controller thread: Queueing task
Controller thread: Polling for task run result
Worker thread: Registered worker with ID 01H3FV14GHSWEB48PPDB4Q506S
Worker thread: Listening for messages...
Hello, World!
Controller thread: Polling for task run result
Controller thread: Got task run result, 42
Controller thread: Looking up all registered workers
Controller thread: Sending command to stop worker 01H3FV14GHSWEB48PPDB4Q506S
Controller thread: Done
Worker thread: Done
```
