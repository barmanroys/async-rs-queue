use redis::aio::MultiplexedConnection;
use redis::{AsyncTypedCommands, Client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url: &str = "redis://localhost:6379/0";
    let key: &str = "test_key";
    let value: &str = "test_value";
    let client: Client = Client::open(redis_url)?;
    let mut conn: MultiplexedConnection = client.get_multiplexed_async_connection().await?;
    conn.set(key, value).await?;
    // The compiler trusts the answer type. In case of runtime mismatch, it throws error
    let answer: String = conn.get(key).await?.unwrap();
    assert_eq!(value, answer);
    Ok(())
}
