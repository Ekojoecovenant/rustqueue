use std::sync::Arc;
use std::time::Duration;

use sqlx::{PgPool, postgres::PgListener};
use tokio::{
    sync::{Semaphore, broadcast},
    time::sleep,
};
use uuid::Uuid;

use crate::{executor::HandlerRegistry, models::job::Job};

const MAX_CONCURRENT_JOBS: usize = 10;

pub async fn run_worker(
    pool: PgPool,
    registry: Arc<HandlerRegistry>,
    tx: broadcast::Sender<String>,
) {
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_JOBS));

    let mut listener = PgListener::connect_with(&pool)
        .await
        .expect("failed to create PgListener");
    listener
        .listen("new_job")
        .await
        .expect("failed to LISTEN on new_job");

    loop {
        while let Ok(Some(job)) = claim_next_job(&pool).await {
            println!("Claimed job: {} ({})", job.id, job.job_type);
            let _ = tx.send(format!("Job {} - processing", job.id));

            // Wait for an available slot, then process this job concurrently
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let pool2 = pool.clone();
            let registry2 = registry.clone();
            let tx2 = tx.clone();

            tokio::spawn(async move {
                process_job(&pool2, &registry2, &tx2, job).await;
                drop(permit);
            });
        }

        // 30 secs fallback
        let _ = tokio::time::timeout(Duration::from_secs(2), listener.recv()).await;
    }
}

pub async fn run_stale_job_reaper(pool: &PgPool) {
    loop {
        match reclaim_stale_jobs(&pool).await {
            Ok(count) if count > 0 => {
                println!("Reclaimed {} stale job(s)", count);
            }
            Ok(_) => {}
            Err(e) => eprintln!("Error reclaiming statle jobs: {:?}", e),
        }

        sleep(Duration::from_secs(60)).await;
    }
}

async fn claim_next_job(pool: &PgPool) -> anyhow::Result<Option<Job>> {
    let result = sqlx::query_as::<_, Job>(
        r#"
      UPDATE jobs
      SET status = 'processing', updated_at = now(), processing_started_at = now()
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

async fn process_job(
    pool: &PgPool,
    registry: &HandlerRegistry,
    tx: &broadcast::Sender<String>,
    job: Job,
) {
    let handler = match registry.get(&job.job_type) {
        Ok(h) => h,
        Err(e) => {
            let current_attempt = job.attempts + 1;
            mark_failed(pool, tx, job.id, current_attempt, &e.to_string()).await;
            return;
        }
    };

    let current_attempt = job.attempts + 1;

    match handler.execute(&job.payload).await {
        Ok(()) => mark_done(pool, tx, job.id, current_attempt).await,
        Err(e) => {
            if current_attempt < job.max_attempts {
                schedule_retry(pool, tx, job.id, current_attempt, &e.to_string()).await;
            } else {
                mark_failed(pool, tx, job.id, current_attempt, &e.to_string()).await;
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
    let backoff_secs = 5i64.pow(next_attempt as u32 - 1).min(300); // cap at 5 minutes

    let result = sqlx::query(
        r#"
        UPDATE jobs
        SET status = 'pending',
            attempts = $1,
            last_error = $2,
            scheduled_at = now() + ($3 || ' seconds')::interval,
            processing_started_at = NULL,
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

async fn mark_done(pool: &PgPool, tx: &broadcast::Sender<String>, job_id: Uuid, attempt: i32) {
    let result = sqlx::query(
        "UPDATE jobs SET status = 'done', attempts = $1, processing_started_at = NULL, updated_at = now() WHERE id = $2",
    )
    .bind(attempt)
    .bind(job_id)
    .execute(pool)
    .await;

    if result.is_ok() {
        let _ = tx.send(format!("Job {} - done", job_id));
    }
}

async fn mark_failed(
    pool: &PgPool,
    tx: &broadcast::Sender<String>,
    job_id: Uuid,
    attempt: i32,
    error: &str,
) {
    let result = sqlx::query(
        "UPDATE jobs SET status = 'failed', attempts = $1, last_error = $2, processing_started_at = NULL, updated_at = now() WHERE id = $3",
    )
    .bind(attempt)
    .bind(error)
    .bind(job_id)
    .execute(pool)
    .await;

    if result.is_ok() {
        let _ = tx.send(format!("Job {} - failed: {}", job_id, error));
    }
}

async fn reclaim_stale_jobs(pool: &PgPool) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r#"
      UPDATE jobs
      SET status = 'pending', updated_at = now(), processing_started_at = NULL
      WHERE status = 'processing'
        AND processing_started_at < now() - interval '5 minutes'
    "#,
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}
