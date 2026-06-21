use std::time::Duration;

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
    pub smtp_username: String,
    pub smtp_password: String,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub from_email: String,
}

#[async_trait]
impl JobHandler for EmailHandler {
    async fn execute(&self, payload: &Value) -> Result<()> {
        let to = payload["to"]
            .as_str()
            .context("payload missing 'to' field")?;

        let creds = Credentials::new(self.smtp_username.clone(), self.smtp_password.clone());
        let tls_params = TlsParameters::builder(self.smtp_host.clone()).build()?;

        let transport = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&self.smtp_host)?
            .port(self.smtp_port)
            .tls(Tls::Required(tls_params))
            .credentials(creds)
            .timeout(Some(Duration::from_secs(30)))
            .build();

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

        transport
            .send(email)
            .await
            .context("failed to send email")?;

        Ok(())
    }
}
