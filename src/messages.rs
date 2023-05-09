use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct Message {
    pub task_id: String,
    pub signature: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ResultMessage {
    pub signature_id: String,
    pub result: String,
}
