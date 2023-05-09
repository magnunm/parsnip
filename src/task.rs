use anyhow::Error;
use serde::{Deserialize, Serialize};

pub trait Task: Sized
where
    Self::ArgumentType: Serialize,
    Self::ArgumentType: for<'a> Deserialize<'a>,
    Self::ReturnType: Serialize,
    Self::ReturnType: for<'a> Deserialize<'a>,
{
    type ArgumentType;
    type ReturnType;

    const ID: &'static str;

    fn from_signature(signature: Signature<Self>) -> Self;

    fn run(arg: &Self::ArgumentType) -> Self::ReturnType;

    /// Get the signature used to created the task instance
    fn signature(&self) -> &Signature<Self>;
}

#[derive(Serialize, Deserialize)]
pub struct Signature<T>
where
    T: Task,
{
    pub arg: T::ArgumentType,
    pub id: String,
}

impl<T> Signature<T>
where
    T: Task,
{
    pub fn from_serialized(signature: &str) -> Result<Self, Error> {
        Ok(serde_json::from_str(signature)?)
    }
}
