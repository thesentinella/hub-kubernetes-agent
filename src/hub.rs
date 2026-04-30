//! HTTP client to the Sentinella Hub.

use crate::config::Config;
use crate::model::*;
use anyhow::Result;
use reqwest::{Client, StatusCode};
use std::time::Duration;
use tracing::{error, warn};

pub struct HubClient {
    http: Client,
    cfg: Config,
}

impl HubClient {
    pub fn new(cfg: Config) -> Result<Self> {
        if let Some(key) = &cfg.api_key {
            if !key.starts_with("shub_") {
                error!(
                    "HUB_API_KEY does not have the expected 'shub_' prefix; requests will likely be rejected"
                );
            }
        } else {
            warn!("HUB_API_KEY is not set; requests will be sent unauthenticated");
        }

        let http = Client::builder()
            .gzip(true)
            .timeout(cfg.http_timeout)
            .pool_idle_timeout(Duration::from_secs(120))
            .pool_max_idle_per_host(2)
            .user_agent(concat!(
                "sentinella-hub-k8s-agent/",
                env!("CARGO_PKG_VERSION")
            ))
            .build()?;
        Ok(Self { http, cfg })
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.cfg.api_key {
            Some(key) => req.bearer_auth(key),
            None => req,
        }
    }

    /// Ship the inventory snapshot. Bounded retries: 0s, 2s, 5s.
    pub async fn send_snapshot(&self, snap: &InventorySnapshot) -> Result<()> {
        let url = format!(
            "{}/v1/clusters/{}/inventory",
            self.cfg.hub_url, self.cfg.cluster_id
        );
        let backoffs = [
            Duration::ZERO,
            Duration::from_secs(2),
            Duration::from_secs(5),
        ];
        let mut last_err: Option<anyhow::Error> = None;

        for (i, delay) in backoffs.iter().enumerate() {
            if !delay.is_zero() {
                tokio::time::sleep(*delay).await;
            }
            let req = self.auth(self.http.post(&url).json(snap));
            match req.send().await {
                Ok(r) if r.status().is_success() => return Ok(()),
                Ok(r) => {
                    let s = r.status();
                    if s.is_client_error()
                        && s != StatusCode::REQUEST_TIMEOUT
                        && s != StatusCode::TOO_MANY_REQUESTS
                    {
                        return Err(anyhow::anyhow!("hub rejected snapshot: {}", s));
                    }
                    last_err = Some(anyhow::anyhow!("attempt {} status {}", i + 1, s));
                }
                Err(e) => last_err = Some(anyhow::anyhow!("attempt {} error: {}", i + 1, e)),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("snapshot retries exhausted")))
    }

    /// Long-poll for commands. Returns an empty batch on timeout (normal).
    /// Uses a per-call timeout slightly larger than the server-side wait window.
    pub async fn poll_commands(&self) -> Result<CommandBatch> {
        let url = format!(
            "{}/v1/clusters/{}/commands/poll?wait={}s",
            self.cfg.hub_url,
            self.cfg.cluster_id,
            self.cfg.poll_wait.as_secs()
        );
        let timeout = self.cfg.poll_wait + Duration::from_secs(10);
        let req = self.auth(self.http.get(&url).timeout(timeout));

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) if e.is_timeout() => return Ok(CommandBatch { commands: vec![] }),
            Err(e) => return Err(e.into()),
        };

        let status = resp.status();
        if status == StatusCode::NO_CONTENT {
            return Ok(CommandBatch { commands: vec![] });
        }
        if !status.is_success() {
            return Err(anyhow::anyhow!("poll status {}", status));
        }
        Ok(resp.json().await?)
    }

    /// Acknowledge a command result.
    pub async fn ack_command(&self, result: &CommandResult) -> Result<()> {
        let url = format!(
            "{}/v1/clusters/{}/commands/{}/ack",
            self.cfg.hub_url, self.cfg.cluster_id, result.command_id
        );
        let req = self.auth(self.http.post(&url).json(result));
        let resp = req.send().await?;
        if !resp.status().is_success() {
            warn!("ack returned {}", resp.status());
        }
        Ok(())
    }
}
