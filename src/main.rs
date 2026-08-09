use log::info;
use redis::aio::MultiplexedConnection;
use redis::{AsyncTypedCommands, Client};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();
    let redis_url: &str = "redis://localhost:6379/0";
    let key: &str = "test_key";
    let value: &str = "test_value";
    let client: Client = Client::open(redis_url)?;
    let mut conn: MultiplexedConnection = client.get_multiplexed_async_connection().await?;
    info!("Client opened with redis url at {redis_url}");
    conn.set(key, value).await?;
    info!("Setting {key} to {value}");
    // The compiler trusts the returned type. In case of runtime mismatch, it throws error
    assert_eq!(value, conn.get(key).await?.unwrap());
    info!("Fetched {key} from {redis_url}");
    conn.del(key).await?;
    info!("Deleted {key} from {redis_url}");
    assert!(conn.get(key).await?.is_none());
    info!("Deletion of {key} from {redis_url} tested.");
    Ok(())
}
