pub mod email;

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use axum::async_trait;
use serde_json::Value;

use self::email::EmailHandler;
use crate::config::Config;

#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn execute(&self, payload: &Value) -> Result<()>;
}

pub struct HandlerRegistry {
    handlers: HashMap<String, Arc<dyn JobHandler>>,
}

impl HandlerRegistry {
    pub fn new(config: &Config) -> Result<Self> {
        let mut handlers: HashMap<String, Arc<dyn JobHandler>> = HashMap::new();

        let email_handler = EmailHandler::new(
            config.smtp_username.clone(),
            config.smtp_password.clone(),
            config.smtp_host.clone(),
            config.smtp_port,
            config.from_email.clone(),
        )?;
        handlers.insert("email".to_string(), Arc::new(email_handler));

        Ok(Self { handlers })
    }

    pub fn get(&self, job_type: &str) -> Result<Arc<dyn JobHandler>> {
        self.handlers
            .get(job_type)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown job type: {}", job_type))
    }
}
