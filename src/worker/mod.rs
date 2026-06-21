use std::time::Duration;

use sqlx::PgPool;
use tokio::time::sleep;
use uuid::Uuid;

use crate::{config::Config, executor, models::job::Job};

pub async fn run_worker(pool: PgPool, config: Config) {
    loop {
        match claim_next_job(&pool).await {
            Ok(Some(job)) => {
                println!("Claimed job: {} ({})", job.id, job.job_type);
                process_job(&pool, &config, job).await;
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

async fn process_job(pool: &PgPool, config: &Config, job: Job) {
    let handler = match executor::get_handler(&job.job_type, config) {
        Ok(h) => h,
        Err(e) => {
            mark_failed(pool, job.id, &e.to_string()).await;
            return;
        }
    };

    match handler.execute(&job.payload).await {
        Ok(()) => {
            mark_done(pool, job.id).await;
        }
        Err(e) => {
            mark_failed(pool, job.id, &e.to_string()).await;
        }
    }
}

async fn mark_done(pool: &PgPool, job_id: Uuid) {
    let _ = sqlx::query("UPDATE jobs SET status = 'done', updated_at = now() WHERE id = $1")
        .bind(job_id)
        .execute(pool)
        .await;
}

async fn mark_failed(pool: &PgPool, job_id: Uuid, error: &str) {
    let _ = sqlx::query(
        "UPDATE jobs SET status = 'failed', last_error = $1, updated_at = now() WHERE id = $2",
    )
    .bind(error)
    .bind(job_id)
    .execute(pool)
    .await;
}
