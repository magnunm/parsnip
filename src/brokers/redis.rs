use crate::broker::Broker;

use anyhow::Result;
use redis::{self, Commands};
use serde_json;

pub struct RedisBroker {
    redis_client: redis::Client,
    queue: String,
    result_hash_map: String,
}

impl RedisBroker {
    pub fn new(connect_url: &str) -> Result<Self> {
        let redis_client = redis::Client::open(connect_url)?;

        Ok(Self {
            redis_client,
            queue: "parsnip_queue".to_string(),
            result_hash_map: "parsnip_task_results".to_string(),
        })
    }
}

impl Broker for RedisBroker {
    fn push_message(&self, message: &crate::messages::Message) -> Result<()> {
        let mut con = self.redis_client.get_connection()?;
        con.lpush(&self.queue, serde_json::to_string(&message)?)?;
        Ok(())
    }

    fn pop_message(&self) -> Result<crate::messages::Message> {
        let mut con = self.redis_client.get_connection()?;
        let serialized_message: Option<String> = con.rpop(&self.queue, None)?;
        match serialized_message {
            Some(v) => Ok(serde_json::from_str(&v)?),
            None => Err(anyhow::anyhow!("Task queue is empty")),
        }
    }

    fn store_result(&self, result_message: crate::messages::ResultMessage) -> Result<()> {
        let mut con = self.redis_client.get_connection()?;
        con.hset(
            &self.result_hash_map,
            &result_message.signature_id,
            serde_json::to_string(&result_message)?,
        )?;
        Ok(())
    }
}
