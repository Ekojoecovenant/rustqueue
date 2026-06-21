pub mod email;

use anyhow::{Result, bail};
use axum::async_trait;
use serde_json::Value;

use self::email::EmailHandler;
use crate::config::Config;

#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn execute(&self, payload: &Value) -> Result<()>;
}

pub fn get_handler(job_type: &str, config: &Config) -> Result<Box<dyn JobHandler>> {
    match job_type {
        "email" => Ok(Box::new(EmailHandler {
            smtp_username: config.smtp_username.clone(),
            smtp_password: config.smtp_password.clone(),
            smtp_host: config.smtp_host.clone(),
            smtp_port: config.smtp_port,
            from_email: config.from_email.clone(),
        })),
        other => bail!("unknown job type: {}", other),
    }
}
