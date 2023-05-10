# Parsnip

A Rust library for distributed, asynchronous computing using a task queue.
Inspired by [Celery](https://docs.celeryq.dev/en/stable/).

## Examples

### `simple_redis_queue`

Spin up a Redis instance and pass the connection URL as the first argument.

```sh
cargo r --example simple_redis_queue 'redis://localhost:6379/1'
```
