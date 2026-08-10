use log::info;
use redis::Client;
use redis::aio::MultiplexedConnection;
use std::env;
use std::time::Duration;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();
    dotenvy::dotenv().ok();

    let redis_url: &str = &env::var("REDIS_URL")?;
    let queue_name: &str = &env::var("QUEUE_NAME")?;
    let client: Client = Client::open(redis_url)?;
    let mut conn: MultiplexedConnection = client.get_multiplexed_async_connection().await?;
    conn.set_response_timeout(Duration::from_hours(5)); // Make sure the brpop waits long enough
    info!("Client opened with redis url at {redis_url}");

    loop {
        info!("Waiting for next task on queue {queue_name}");
        // brpop can come from multiple traits, so the need to disambiguate
        let results: Option<(String, Vec<u8>)> =
            redis::AsyncCommands::brpop(&mut conn, queue_name, 0.0).await?;
        let tasks: (String, Vec<u8>) = results.unwrap();
        info!("Item found in Queue {}", tasks.0);
        for task in tasks.1 {
            info!("Task found in redis: {}", task);
        }
    }
}
