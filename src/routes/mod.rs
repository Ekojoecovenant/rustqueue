use axum::{
    Router,
    routing::{get, post},
};
use sqlx::PgPool;
use std::sync::Arc;

mod jobs;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

pub fn create_router(pool: PgPool) -> Router {
    let state = Arc::new(AppState { pool });

    Router::new()
        .route("/jobs", post(jobs::create_job).get(jobs::list_jobs))
        .route("/jobs/:id", get(jobs::get_job))
        .with_state(state)
}
