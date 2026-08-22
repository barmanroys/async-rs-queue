use async_rs_queue::broker::{
    BrokerInterface, RedisAdapter, SerdeAdapter, TaskInputMessage, TaskOutputMessage, Value,
};
use log::info;
use rand::RngExt;
use rand::rngs::ThreadRng;
use redis::Client;
use redis::aio::MultiplexedConnection;
use std::collections::HashMap;
use std::env;
use std::process::exit;
use std::time::Duration;
use uuid::Uuid;
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

    let mut bi: RedisAdapter<SerdeAdapter> = RedisAdapter {
        redis_client,
        queue_name: queue_name.into(),
        result_backend: result_backend.into(),
        serialiser,
    };
    let task_id: Uuid = Uuid::new_v4();
    let task_name: &str = "add";
    let mut rng: ThreadRng = rand::rng();
    // Pick random inputs for testing
    let a: i64 = rng.random_range(100..=1000);
    let b: i64 = rng.random_range(100..=1000);
    let input_message: TaskInputMessage = TaskInputMessage {
        task_id: task_id.clone(),
        task_name: task_name.into(),
        kwargs: HashMap::from([
            ("a".to_string(), Value::Integer(a)),
            ("b".to_string(), Value::Integer(b)),
        ]),
    };
    bi.write_task(input_message).await;

    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        info!("Checking for result of task id {task_id}");
        let provisional_result: Option<TaskOutputMessage> = bi.query_result(task_id).await;
        match provisional_result {
            Some(task_output) => {
                let task_id: Uuid = task_output.task_id;
                info!("Results read for {task_id}");
                let result: Value = task_output.result;
                match result {
                    Value::Integer(computed_result) => {
                        assert_eq!(a + b, computed_result);
                        info!("Successfully read result of task id {task_id} as {computed_result}");
                    }
                    _ => panic!("Incorrect result {:#?} for task id {task_id}", result),
                }
                exit(0);
            }
            None => info!("Result for {task_id} not found, will poll again after a while."),
        }
    }
}
