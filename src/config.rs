use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub port: u16,
    pub smtp_username: String,
    pub smtp_password: String,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub from_email: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            database_url: std::env::var("DATABASE_URL").context("DATABASE_URL not set")?,
            port: std::env::var("PORT")
                .unwrap_or_else(|_| "3000".to_string())
                .parse()
                .context("PORT must be a valid number")?,
            smtp_username: std::env::var("SMTP_USERNAME").context("SMTP_USERNAME not set")?,
            smtp_password: std::env::var("SMTP_PASSWORD").context("SMTP_PASSWORD not set")?,
            smtp_host: std::env::var("SMTP_HOST")
                .unwrap_or_else(|_| "email-smtp.eu-north-1.amazonaws.com".to_string()),
            smtp_port: std::env::var("SMTP_PORT")
                .unwrap_or_else(|_| "587".to_string())
                .parse()
                .context("SMTP_PORT must be a valid number")?,
            from_email: std::env::var("FROM_EMAIL").context("FROM_EMAIL not set")?,
        })
    }
}
