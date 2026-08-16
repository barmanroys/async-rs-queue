// This module implements the broker which interfaces with redis
use log::info;
use redis::AsyncTypedCommands;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Bytes(Vec<u8>),
}
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskInputMessage {
    pub task_id: Uuid,
    pub task_name: String,
    pub kwargs: HashMap<String, Value>,
}

impl TaskInputMessage {
    pub fn new(task_id: Uuid, task_name: String, kwargs: HashMap<String, Value>) -> Self {
        Self {
            task_id,
            task_name,
            kwargs,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskOutputMessage {
    pub task_id: Uuid,
    pub result: Value,
}

impl TaskOutputMessage {
    pub fn new(task_id: Uuid, result: Value) -> Self {
        Self { task_id, result }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum BrokerMessage {
    Input(TaskInputMessage),
    Output(TaskOutputMessage),
}

pub struct SerdeAdapter;

pub trait SerialiserInterface {
    fn serialise(&self, message: &BrokerMessage) -> Vec<u8>;

    fn parse(&self, content: &[u8]) -> BrokerMessage;
}

impl SerialiserInterface for SerdeAdapter {
    fn serialise(&self, message: &BrokerMessage) -> Vec<u8> {
        serde_json::to_vec(message).expect("Failed to serialise message.") // Equivalent of json.dumps
    }

    fn parse(&self, content: &[u8]) -> BrokerMessage {
        serde_json::from_slice(content).expect("Failed to parse message") // Equivalent of json.loads
    }
}

pub trait BrokerInterface {
    async fn receive_task(&mut self) -> TaskInputMessage; // Pick up the task via this method

    async fn write_result(&mut self, out: TaskOutputMessage) -> (); // Use this to write back the results
}

pub struct RedisAdapter<T: SerialiserInterface> {
    pub redis_client: MultiplexedConnection,
    pub queue_name: String,     // Where to pick up the task?
    pub result_backend: String, // Where to write back the result?
    pub serialiser: T,
}

impl<T: SerialiserInterface> BrokerInterface for RedisAdapter<T> {
    async fn receive_task(&mut self) -> TaskInputMessage {
        self.redis_client
            .set_response_timeout(Duration::from_hours(5));
        let qn: &str = &self.queue_name;
        info!("Waiting for next task on queue {qn}");
        let results: Option<(String, Vec<u8>)> =
            redis::AsyncCommands::brpop(&mut self.redis_client, qn, 0.0)
                .await
                .expect("Failed to get next task on queue.");
        let tasks: (String, Vec<u8>) = results.unwrap();
        assert_eq!(tasks.0, qn); // The first element returns the queue name.
        // The serialiser returns a broker message, make sure it is an input message
        match self.serialiser.parse(&tasks.1) {
            BrokerMessage::Input(input) => {
                info!("Successfully parsed input from queue name {qn}");
                input
            }
            _ => {
                panic!("Queue contains invalid message."); // Exit any other case
            }
        }
    }

    async fn write_result(&mut self, out: TaskOutputMessage) -> () {
        let task_id: Uuid = out.task_id;
        let content: Vec<u8> = self.serialiser.serialise(&BrokerMessage::Output(out));
        let key: String = format!("{}:{task_id}", self.result_backend);
        info!("Writing result to: {key}");
        self.redis_client
            .set(&key, content)
            .await
            .expect("Failed to set result.");
        info!("Finished writing result to {key}");
    }
}
