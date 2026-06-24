# ADR-001: rustqueue architecture review

**Status:** Proposed  
**Date:** 2026-06-22  
**Deciders:** Solo author (team of 1)

---

## Context

`rustqueue` is a PostgreSQL-backed job queue with an HTTP API, a polling worker, a stale-job reaper, and real-time SSE push. Currently the only executor is an email handler via Amazon SES/SMTP. This document reviews the existing design, surfaces risks, and proposes concrete improvements.

---

## Current architecture

```plain
HTTP clients
    │  POST /jobs   GET /jobs   GET /jobs/:id   GET /events
    ▼
  Axum router  ──── Arc<AppState> (PgPool + broadcast::Sender) ────┐
                                                                    │
              ┌─────────────────────────────────────────────────────┤
              │  Worker loop (tokio::spawn)                         │
              │    poll every 2 s                                   │
              │    SELECT … FOR UPDATE SKIP LOCKED                  │
              │    → executor::get_handler → handler.execute()      │
              │    → mark done / schedule retry / mark failed       │
              │                                                     │
              │  Stale-job reaper (tokio::spawn)                    │
              │    poll every 60 s                                  │
              │    reclaim jobs stuck in "processing" > 5 min       │
              └─────────────────────────────────────────────────────┘
              │
         broadcast::Sender<String>  →  SSE /events stream
              │
         PostgreSQL  (jobs table)
```

---

## Strengths

**Solid concurrency primitive for claiming jobs.** The `SELECT … FOR UPDATE SKIP LOCKED` pattern is the right way to avoid double-processing in a polling queue. No external lock manager needed.

**Stale-job reaper.** Covering the "worker crashed mid-job" case with a reaper is necessary and correctly implemented.

**SSE for real-time updates.** Using a `broadcast::Sender` threaded through `AppState` is a clean, low-overhead way to push status to connected clients without a separate pub-sub system.

**Exponential backoff.** `5^(attempt-1)` capped at 300 s is a reasonable retry curve.

**Good dependency choices.** `axum` + `sqlx` + `tokio` is a well-maintained, production-proven async stack for Rust.

---

## Issues and risks

### 1. Single-threaded worker (high impact)

The worker processes one job at a time inside a `loop`. Even if the job handler is async and non-blocking (e.g. network I/O), the worker sits idle during the 2 s sleep between polls and only picks up the next job after the previous one finishes.

**Risk:** throughput is capped at ~1 job per handler-duration + 2 s. For email, a slow SMTP relay could back up the queue.

**Fix:** spawn each job into its own task, and run N concurrent workers.

```rust
// instead of process_job(...).await inside the loop:
let pool2 = pool.clone();
let cfg2 = config.clone();
let tx2 = tx.clone();
tokio::spawn(async move {
    process_job(&pool2, &cfg2, &tx2, job).await;
});
```

Optionally add a semaphore to cap concurrency:

```rust
let sem = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_JOBS));
// acquire a permit before spawning
```

---

### 2. Polling wastes cycles; misses bursts (medium impact)

The 2 s sleep means jobs wait up to 2 s before being picked up, and the database receives a query every 2 s regardless of queue depth.

**Fix:** Use PostgreSQL `LISTEN / NOTIFY`. Send a `NOTIFY` on job insert and have the worker wake immediately.

```sql
-- trigger on INSERT
NOTIFY new_job;
```

```rust
// worker uses sqlx::PgListener
let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
listener.listen("new_job").await?;
// fall back to 30 s timeout poll so the reaper still runs
listener.recv().await?;
```

This reduces average latency from ~1 s to near-zero and cuts idle DB load.

---

### 3. SMTP connection rebuilt per job (medium impact)

`EmailHandler::execute` creates a new `AsyncSmtpTransport` on every invocation — TLS handshake, TCP connect, auth — for every single email.

**Fix:** Build the transport once and store it in `EmailHandler`, or wrap it in an `Arc` so it can be shared across concurrent executions.

```rust
pub struct EmailHandler {
    transport: Arc<AsyncSmtpTransport<Tokio1Executor>>,
    from_email: String,
}
```

---

### 4. `list_jobs` has no pagination (medium impact)

```rust
"SELECT * FROM jobs ORDER BY created_at DESC"
```

This will return every row in the table. At 10 000+ jobs this becomes a memory and latency problem.

**Fix:** Add `LIMIT` / `OFFSET` or cursor-based pagination, and a status filter.

```plain
GET /jobs?status=failed&limit=50&after=<uuid>
```

---

### 5. Dropped broadcast receiver silently loses SSE messages on startup (low–medium impact)

In `main`:

```rust
let (tx, _rx) = tokio::sync::broadcast::channel(100);
```

`_rx` is immediately dropped. The channel has capacity 100, but if no SSE client is connected when jobs complete, those messages are dropped silently. The next client to connect sees nothing about past jobs.

**Fix:** either keep one permanent `_rx` alive for the lifetime of the process to prevent the channel from closing prematurely, or (better) store terminal job state in the DB and serve historical events on SSE connect via a catch-up query.

---

### 6. Job status is an untyped string (low impact)

`status: String` in `Job` means typos compile fine. `"procesing"` would silently never match any query.

**Fix:** Define an enum and implement `sqlx::Type` for it.

```rust
#[derive(Debug, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Processing,
    Done,
    Failed,
}
```

---

### 7. Config requires SMTP even when no email jobs are enqueued (low impact)

`Config::from_env()` panics at startup if `SMTP_USERNAME` is missing, regardless of whether any email jobs will be run.

**Fix:** Make SMTP config optional (`Option<SmtpConfig>`) and return an error only when an email job is dispatched without SMTP configured.

---

### 8. Route error handling swallows context (low impact)

```rust
.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
```

The actual sqlx error is discarded. Operators get a 500 with no log line.

**Fix:** log the error before mapping it away.

```rust
.map_err(|e| {
    tracing::error!("DB error: {:?}", e);
    StatusCode::INTERNAL_SERVER_ERROR
})?;
```

---

## Recommended priority order

| # | Issue | Effort | Impact |
| - | ----- | ------ | ------ |
| 1 | Concurrent worker + semaphore | Low | High |
| 2 | LISTEN/NOTIFY instead of 2 s poll | Medium | Medium |
| 3 | SMTP transport reuse | Low | Medium |
| 4 | Paginate `list_jobs` | Low | Medium |
| 5 | Keep a live broadcast receiver | Trivial | Low–Medium |
| 6 | Typed `JobStatus` enum | Low | Low |
| 7 | Optional SMTP config | Low | Low |
| 8 | Log DB errors in routes | Trivial | Low |

---

## Action items

- [ ] Spawn job processing into individual tasks; add `Semaphore` with configurable limit
- [ ] Add `PgListener` for `NOTIFY`-based wakeup; fall back to 30 s poll
- [ ] Refactor `EmailHandler` to hold an `Arc<AsyncSmtpTransport>`
- [ ] Add `?limit=&status=&after=` to `GET /jobs`
- [ ] Hold one live `_rx` in `AppState` or add catch-up query on SSE connect
- [ ] Replace `status: String` with `JobStatus` enum + sqlx type mapping
- [ ] Make SMTP config `Option<SmtpConfig>` in `Config`
- [ ] Add `tracing::error!` before all `.map_err(|_| ...)` discards
