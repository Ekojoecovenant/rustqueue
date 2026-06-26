use std::sync::Arc;

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
    let registry = Arc::new(executor::HandlerRegistry::new(&config)?);

    let (tx, _rx) = tokio::sync::broadcast::channel(100);

    // Worker loop
    let worker_pool = pool.clone();
    let worker_registry = registry.clone();
    let worker_tx = tx.clone();
    tokio::spawn(async move {
        worker::run_worker(worker_pool, worker_registry, worker_tx).await;
    });

    // Stale-job reaper
    let reaper_pool = pool.clone();
    tokio::spawn(async move {
        worker::run_stale_job_reaper(&reaper_pool).await;
    });

    // HTTP server
    let app = routes::create_router(pool, tx);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port)).await?;

    println!("Server running on port: {}", config.port);
    axum::serve(listener, app).await?;

    Ok(())
}
