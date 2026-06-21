use anyhow::Result;

mod config;
mod db;
mod executor;
mod models;
mod worker;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let config = config::Config::from_env()?;
    let pool = db::create_pool(&config.database_url).await?;

    println!("Starting worker...");
    worker::run_worker(pool, config).await;

    Ok(())
}
