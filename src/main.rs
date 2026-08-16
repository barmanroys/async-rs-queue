mod broker;
mod consumer;

use crate::broker::{RedisAdapter, SerdeAdapter};
use crate::consumer::Orchestrator;
use log::info;
use redis::Client;
use redis::aio::MultiplexedConnection;
use std::env;
use std::time::Duration;

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
