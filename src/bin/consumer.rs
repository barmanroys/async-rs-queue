// This module implements the consumer binary
use async_rs_queue::broker::{
    BrokerInterface, RedisAdapter, SerdeAdapter, TaskInputMessage, TaskOutputMessage, Value,
};
use std::collections::HashMap;

use log::info;
use redis::Client;
use redis::aio::MultiplexedConnection;
use std::env;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

async fn add(a: i64, b: i64) -> i64 {
    info!("Adding {a} and {b}");
    sleep(Duration::from_secs(30)).await; // Emulate a long-running function
    a + b
}

async fn multiply(a: i64, b: i64) -> i64 {
    info!("Multiplying {a} and {b}");
    sleep(Duration::from_secs(30)).await; // Emulate a long-running function
    a * b
}

pub struct Orchestrator<T: BrokerInterface> {
    pub bi: T,
}

impl<T: BrokerInterface> Orchestrator<T> {
    async fn compute(&self, ti: &TaskInputMessage) -> TaskOutputMessage {
        let task_id: Uuid = ti.task_id;
        let task_name: &String = &ti.task_name;
        let kwargs: &HashMap<String, Value> = &ti.kwargs;
        info!("Computing {task_name}");
        // This match acts as the task registry.
        // ToDo replace it with a Hashmap and some more dynamic behaviour to parse the arguments
        match task_name.as_str() {
            "add" => {
                let a: i64 = match kwargs.get("a").unwrap() {
                    Value::Integer(i) => *i,
                    _ => panic!("Expected an integer"),
                };
                let b: i64 = match kwargs.get("b").unwrap() {
                    Value::Integer(i) => *i,
                    _ => panic!("Expected an integer"),
                };
                let result: i64 = add(a, b).await;
                info!("Computed result {result} after adding {a} and {b}");
                TaskOutputMessage {
                    task_id,
                    result: Value::Integer(result),
                }
            }
            "multiply" => {
                let a: i64 = match kwargs.get("a").unwrap() {
                    Value::Integer(i) => *i,
                    _ => panic!("Expected an integer"),
                };
                let b: i64 = match kwargs.get("b").unwrap() {
                    Value::Integer(i) => *i,
                    _ => panic!("Expected an integer"),
                };
                let result: i64 = multiply(a, b).await;
                info!("Computed result {result} after multiplying {a} and {b}");
                TaskOutputMessage {
                    task_id,
                    result: Value::Integer(result),
                }
            }
            // The default case when correct task identification fails
            _ => panic!("Unknown task name {}", &ti.task_name),
        }
    }

    async fn write_back(&mut self, output: TaskOutputMessage) -> () {
        self.bi.write_result(output).await;
    }

    pub async fn start_running(&mut self) {
        loop {
            info!("Waiting for task.");
            // Blocking call waiting for the BRPOP
            let ti: TaskInputMessage = self.bi.receive_task().await;
            let task_id: Uuid = ti.task_id;
            info!("Starting task {task_id}");
            let out: TaskOutputMessage = self.compute(&ti).await;
            self.write_back(out).await;
            info!("Task {task_id} completed and result persisted on Broker.");
            sleep(Duration::from_secs(5)).await;
        }
    }
}

#[tokio::main]
async fn main() -> () {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();
    dotenvy::dotenv().ok();

    let redis_url: &str = &env::var("REDIS_URL").expect("REDIS_URL must be set");
    let queue_name: &str = &env::var("QUEUE_NAME").expect("QUEUE_NAME must be set");
    let result_backend: &str = &env::var("RESULT_BACKEND").expect("RESULT_BACKEND must be set");
    let conn: Client = Client::open(redis_url).expect("Failed to open redis connection");
    let mut redis_client: MultiplexedConnection = conn
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect to redis");
    redis_client.set_response_timeout(Duration::from_hours(5)); // Make sure the brpop waits long enough
    info!("Client opened with redis url at {redis_url}");
    let serialiser: SerdeAdapter = SerdeAdapter {};

    let bi: RedisAdapter<SerdeAdapter> = RedisAdapter {
        redis_client,
        queue_name: queue_name.into(),
        result_backend: result_backend.into(),
        serialiser,
    };
    let mut orchestrator: Orchestrator<RedisAdapter<SerdeAdapter>> = Orchestrator { bi };
    orchestrator.start_running().await;
}
