use crate::broker::{BrokerInterface, TaskInputMessage, TaskOutputMessage, Value};
use log::info;
use std::collections::HashMap;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

async fn add(a: i64, b: i64) -> i64 {
    info!("Adding {a} and {b}");
    sleep(Duration::from_secs(30)).await; // Emulate a long-running function
    a + b
}

async fn multiply(a: i64, b: i64) -> i64 {
    info!("Adding {a} and {b}");
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
