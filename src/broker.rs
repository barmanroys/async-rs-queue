use log::info;
use redis::AsyncCommands;
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

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskOutputMessage {
    pub task_id: Uuid,
    pub result: Value,
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
    async fn write_task(&mut self, input: TaskInputMessage) -> (); // Producer pushes task into the queue
    async fn receive_task(&mut self) -> TaskInputMessage; // Consumer claims task

    async fn write_result(&mut self, out: TaskOutputMessage) -> (); // Consumer write back results

    async fn query_result(&mut self, task_id: Uuid) -> Option<TaskOutputMessage>; // Producer queries the result
}

pub struct RedisAdapter<T: SerialiserInterface> {
    pub redis_client: MultiplexedConnection,
    pub queue_name: String,     // Where to pick up the task?
    pub result_backend: String, // Where to write back the result?
    pub serialiser: T,
}

impl<T: SerialiserInterface> RedisAdapter<T> {
    fn result_key(&self, task_id: &Uuid) -> String {
        // Follow TaskIQ convention of key formatting
        info!("Generating key for task {task_id}");
        format!("{}:{task_id}", self.result_backend)
    }
}

impl<T: SerialiserInterface> BrokerInterface for RedisAdapter<T> {
    // Producer push task
    async fn write_task(&mut self, input: TaskInputMessage) -> () {
        // Producer task insertion
        let task_id: Uuid = input.task_id;
        info!("Inserting {task_id} inputs into the broker.");
        let content: Vec<u8> = self.serialiser.serialise(&BrokerMessage::Input(input));
        info!("Writing task content: {content:?}");
        let length: usize = AsyncCommands::lpush(&mut self.redis_client, &self.queue_name, content)
            .await
            .expect("Failed to insert into redis queue.");

        info!("Finished writing {task_id} to queue, and the new queue size is {length}");
    }
    // Consumer claims task
    async fn receive_task(&mut self) -> TaskInputMessage {
        self.redis_client
            .set_response_timeout(Duration::from_hours(5));
        let qn: &str = &self.queue_name;
        info!("Waiting for next task on queue {qn}");
        let results: Option<(String, Vec<u8>)> =
            AsyncCommands::brpop(&mut self.redis_client, qn, 0.0)
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
    // Consumer writes result
    async fn write_result(&mut self, out: TaskOutputMessage) -> () {
        let task_id: Uuid = out.task_id;
        let content: Vec<u8> = self.serialiser.serialise(&BrokerMessage::Output(out));
        let key: String = self.result_key(&task_id);
        info!("Writing result to: {key}");
        AsyncCommands::set(&mut self.redis_client, &key, content)
            .await
            .expect("Failed to set result.")
    }

    // Producer reads result
    async fn query_result(&mut self, task_id: Uuid) -> Option<TaskOutputMessage> {
        let key: String = self.result_key(&task_id);
        info!("Querying result for {key}");
        let content: Vec<u8> = self
            .redis_client
            .get(key)
            .await
            .expect("Failed to get result.");
        match self.serialiser.parse(&content) {
            BrokerMessage::Output(output) => Option::from(output),
            // This indicates absence of result
            _ => None,
        }
    }
}
