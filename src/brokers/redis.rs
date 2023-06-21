use crate::broker::{Broker, WorkerInfo};

use anyhow::Result;
use redis::{self, Commands};
use serde_json;

pub struct RedisBroker {
    redis_client: redis::Client,
    queue: String,
    command_queue_prefix: String,
    result_hash_map: String,
    worker_register: String,
}

impl RedisBroker {
    pub fn new(connect_url: &str) -> Result<Self> {
        let redis_client = redis::Client::open(connect_url)?;

        Ok(Self {
            redis_client,
            queue: "parsnip_queue".to_string(),
            command_queue_prefix: "parsnip_command_queue".to_string(),
            result_hash_map: "parsnip_task_results".to_string(),
            worker_register: "worker_register".to_string(),
        })
    }
}

impl Broker for RedisBroker {
    fn push_message(&self, message: &crate::messages::Message) -> Result<()> {
        let mut con = self.redis_client.get_connection()?;
        con.lpush(&self.queue, serde_json::to_string(&message)?)?;
        Ok(())
    }

    fn pop_message(&self) -> Result<Option<crate::messages::Message>> {
        let mut con = self.redis_client.get_connection()?;
        let serialized_message: Option<String> = con.rpop(&self.queue, None)?;
        match serialized_message {
            Some(v) => Ok(Some(serde_json::from_str(&v)?)),
            None => Ok(None),
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

    fn get_result(&self, signature_id: &str) -> Result<Option<crate::messages::ResultMessage>> {
        let mut con = self.redis_client.get_connection()?;
        let serialized_result: Option<String> = con.hget(&self.result_hash_map, signature_id)?;
        serialized_result.map_or(Ok(None), |v| {
            serde_json::from_str(&v).map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    fn push_command(&self, command: &crate::messages::Command, worker_id: &str) -> Result<()> {
        let mut con = self.redis_client.get_connection()?;
        con.lpush(
            &format!("{}_{}", self.command_queue_prefix, worker_id),
            serde_json::to_string(&command)?,
        )?;
        Ok(())
    }

    fn pop_command(&self, worker_id: &str) -> Result<Option<crate::messages::Command>> {
        let mut con = self.redis_client.get_connection()?;
        let serialized_command: Option<String> = con.rpop(
            &format!("{}_{}", self.command_queue_prefix, worker_id),
            None,
        )?;
        match serialized_command {
            Some(v) => Ok(Some(serde_json::from_str(&v)?)),
            None => Ok(None),
        }
    }

    fn update_worker_info(&self, info: WorkerInfo) -> Result<()> {
        let mut con = self.redis_client.get_connection()?;
        con.hset(
            &self.worker_register,
            &info.id,
            serde_json::to_string(&info)?,
        )?;
        Ok(())
    }

    fn get_worker_info(&self, worker_id: &str) -> Result<Option<WorkerInfo>> {
        let mut con = self.redis_client.get_connection()?;
        let serialized_info: Option<String> = con.hget(&self.worker_register, worker_id)?;
        serialized_info.map_or(Ok(None), |v| {
            serde_json::from_str(&v).map_err(|e| anyhow::anyhow!("{}", e))
        })
    }

    fn remove_worker_info(&self, worker_id: &str) -> Result<()> {
        let mut con = self.redis_client.get_connection()?;
        con.hdel(&self.worker_register, worker_id)
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn all_workers(&self) -> Result<Option<Vec<WorkerInfo>>> {
        let mut con = self.redis_client.get_connection()?;

        let serialized_info: Option<Vec<String>> = con.hvals(&self.worker_register)?;
        serialized_info.map_or(Ok(None), |info_vec| {
            info_vec
                .iter()
                .map(|v| serde_json::from_str(v).map_err(|e| anyhow::anyhow!("{}", e)))
                .collect()
        })
    }
}
