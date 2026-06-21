use std::sync::Arc;

use anyhow::Result;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use uuid::Uuid;

use crate::models::job::{CreateJobRequest, Job};

use super::AppState;

pub async fn create_job(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateJobRequest>,
) -> Result<Json<Job>, StatusCode> {
    let job = sqlx::query_as::<_, Job>(
        r#"
      INSERT INTO jobs (job_type, payload)
      VALUES ($1, $2)
      RETURNING *
    "#,
    )
    .bind(payload.job_type)
    .bind(payload.payload)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(job))
}

pub async fn get_job(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Job>, StatusCode> {
    let job = sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(job))
}

pub async fn list_jobs(State(state): State<Arc<AppState>>) -> Result<Json<Vec<Job>>, StatusCode> {
    let jobs = sqlx::query_as::<_, Job>("SELECT * FROM jobs ORDER BY created_at DESC")
        .fetch_all(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(jobs))
}
