use axum::{
    Router,
    routing::{get, post},
};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::broadcast;

mod events;
mod jobs;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub tx: broadcast::Sender<String>,
}

pub fn create_router(pool: PgPool, tx: broadcast::Sender<String>) -> Router {
    let state = Arc::new(AppState { pool, tx });

    Router::new()
        .route("/jobs", post(jobs::create_job).get(jobs::list_jobs))
        .route("/jobs/:id", get(jobs::get_job))
        .route("/events", get(events::sse_handler))
        .with_state(state)
}
