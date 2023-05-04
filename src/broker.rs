use super::{Message, ResultMessage};
use anyhow::Result;

pub trait Broker {
    fn push_message(&self, message: &Message) -> Result<()>;

    fn pop_message(&self) -> Result<Message>;

    fn store_result(&self, result_message: ResultMessage) -> Result<()>;
}
