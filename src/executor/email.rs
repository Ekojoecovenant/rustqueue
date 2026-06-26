use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::async_trait;
use lettre::{
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
    message::Mailbox,
    transport::smtp::{
        authentication::Credentials,
        client::{Tls, TlsParameters},
    },
};
use serde_json::Value;

use crate::executor::JobHandler;

pub struct EmailHandler {
    transport: Arc<AsyncSmtpTransport<Tokio1Executor>>,
    pub from_email: String,
}

impl EmailHandler {
    pub fn new(
        smtp_username: String,
        smtp_password: String,
        smtp_host: String,
        smtp_port: u16,
        from_email: String,
    ) -> Result<Self> {
        let creds = Credentials::new(smtp_username, smtp_password);
        let tls_params = TlsParameters::builder(smtp_host.clone()).build()?;

        let transport = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&smtp_host)?
            .port(smtp_port)
            .tls(Tls::Required(tls_params))
            .credentials(creds)
            .timeout(Some(Duration::from_secs(30)))
            .build();

        println!("🔧 SMTP transport built"); // TODO: TEMPORARY — remove after testing

        Ok(Self {
            transport: Arc::new(transport),
            from_email,
        })
    }
}

#[async_trait]
impl JobHandler for EmailHandler {
    async fn execute(&self, payload: &Value) -> Result<()> {
        let to = payload["to"]
            .as_str()
            .context("payload missing 'to' field")?;

        let from_mailbox: Mailbox = self.from_email.parse().context("invalid from email")?;
        let to_mailbox: Mailbox = to.parse().context("invalid to email")?;

        let email = Message::builder()
            .from(from_mailbox)
            .to(to_mailbox)
            .subject("RustQueue Notification")
            .body(format!(
                "This email was processed by RustQueue. Job payload: {}",
                payload
            ))
            .context("failed to build email")?;

        self.transport
            .send(email)
            .await
            .context("failed to send email")?;

        Ok(())
    }
}
