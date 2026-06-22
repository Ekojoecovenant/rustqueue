use std::time::Duration;

use sqlx::PgPool;
use tokio::{sync::broadcast, time::sleep};
use uuid::Uuid;

use crate::{config::Config, executor, models::job::Job};

pub async fn run_worker(pool: PgPool, config: Config, tx: broadcast::Sender<String>) {
    loop {
        match claim_next_job(&pool).await {
            Ok(Some(job)) => {
                println!("Claimed job: {} ({})", job.id, job.job_type);
                let _ = tx.send(format!("Job {} - processing", job.id));
                process_job(&pool, &config, &tx, job).await;
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("Error claiming job: {:?}", e);
            }
        }

        sleep(Duration::from_secs(2)).await;
    }
}

async fn claim_next_job(pool: &PgPool) -> anyhow::Result<Option<Job>> {
    let result = sqlx::query_as::<_, Job>(
        r#"
      UPDATE jobs
      SET status = 'processing', updated_at = now()
      WHERE id = (
        SELECT id FROM jobs
        WHERE status = 'pending' AND scheduled_at <= now()
        ORDER BY scheduled_at
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      )
      RETURNING *
    "#,
    )
    .fetch_optional(pool)
    .await?;

    Ok(result)
}

async fn process_job(pool: &PgPool, config: &Config, tx: &broadcast::Sender<String>, job: Job) {
    let handler = match executor::get_handler(&job.job_type, config) {
        Ok(h) => h,
        Err(e) => {
            mark_failed(pool, tx, job.id, &e.to_string()).await;
            return;
        }
    };

    match handler.execute(&job.payload).await {
        Ok(()) => mark_done(pool, tx, job.id).await,
        Err(e) => {
            let next_attempt = job.attempts + 1;

            if next_attempt < job.max_attempts {
                schedule_retry(pool, tx, job.id, next_attempt, &e.to_string()).await;
            } else {
                mark_failed(pool, tx, job.id, &e.to_string()).await;
            }
        }
    }
}

async fn schedule_retry(
    pool: &PgPool,
    tx: &broadcast::Sender<String>,
    job_id: Uuid,
    next_attempt: i32,
    error: &str,
) {
    // exponential backoff: 1s, 5s, 25s... (5^attempt seconds, roughly matching your Dilamme pattern)
    let backoff_secs = 5i64.pow(next_attempt as u32 - 1).min(300); // cap at 5 minutes

    let result = sqlx::query(
        r#"
        UPDATE jobs
        SET status = 'pending',
            attempts = $1,
            last_error = $2,
            scheduled_at = now() + ($3 || ' seconds')::interval,
            updated_at = now()
        WHERE id = $4
        "#,
    )
    .bind(next_attempt)
    .bind(error)
    .bind(backoff_secs.to_string())
    .bind(job_id)
    .execute(pool)
    .await;

    if result.is_ok() {
        let _ = tx.send(format!(
            "Job {} - retry {} scheduled in {}s",
            job_id, next_attempt, backoff_secs
        ));
    }
}

async fn mark_done(pool: &PgPool, tx: &broadcast::Sender<String>, job_id: Uuid) {
    let result = sqlx::query("UPDATE jobs SET status = 'done', updated_at = now() WHERE id = $1")
        .bind(job_id)
        .execute(pool)
        .await;

    if result.is_ok() {
        let _ = tx.send(format!("Job {} - done", job_id));
    }
}

async fn mark_failed(pool: &PgPool, tx: &broadcast::Sender<String>, job_id: Uuid, error: &str) {
    let result = sqlx::query(
        "UPDATE jobs SET status = 'failed', last_error = $1, updated_at = now() WHERE id = $2",
    )
    .bind(error)
    .bind(job_id)
    .execute(pool)
    .await;

    if result.is_ok() {
        let _ = tx.send(format!("Job {} - failed: {}", job_id, error));
    }
}
