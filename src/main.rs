use anyhow::Result;

mod config;
mod db;
mod executor;
mod models;
mod routes;
mod worker;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let config = config::Config::from_env()?;
    let pool = db::create_pool(&config.database_url).await?;

    let (tx, _rx) = tokio::sync::broadcast::channel(100);

    let worker_pool = pool.clone();
    let worker_config = config.clone();
    let worker_tx = tx.clone();
    tokio::spawn(async move {
        worker::run_worker(worker_pool, worker_config, worker_tx).await;
    });

    let app = routes::create_router(pool, tx);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port)).await?;

    println!("Server running on port: {}", config.port);
    axum::serve(listener, app).await?;

    Ok(())
}
