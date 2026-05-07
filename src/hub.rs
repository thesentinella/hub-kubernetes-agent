//! HTTP client to the Sentinella Hub.

use crate::config::Config;
use crate::model::*;
use anyhow::Result;
use reqwest::{Client, StatusCode};
use std::time::Duration;
use tracing::{debug, error, warn};

pub struct HubClient {
    http: Client,
    cfg: Config,
    routes: HubRoutes,
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
        let routes = HubRoutes::new(&cfg);
        Ok(Self { http, cfg, routes })
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.cfg.api_key {
            Some(key) => req.bearer_auth(key),
            None => req,
        }
    }

    /// Ship the inventory snapshot. Bounded retries: 0s, 2s, 5s.
    pub async fn send_snapshot(&self, snap: &InventorySnapshot) -> Result<()> {
        let urls = self.routes.inventory_urls(&self.cfg.cluster_id);
        if self.cfg.http_debug {
            let request_body = if self.cfg.http_debug_bodies {
                Some(serde_json::to_string(snap).unwrap_or_else(|_| "<serialization error>".into()))
            } else {
                None
            };
            debug!(
                method = "POST",
                urls = ?urls,
                request_preview_enabled = self.cfg.http_debug_bodies,
                request_body_preview = %body_preview_opt(request_body.as_deref()),
                schema_version = snap.schema_version,
                namespaces = snap.namespaces.len(),
                deployments = snap.workloads.deployments.len(),
                statefulsets = snap.workloads.statefulsets.len(),
                daemonsets = snap.workloads.daemonsets.len(),
                pods = snap.pods.len(),
                "sending inventory snapshot"
            );
        }

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
            for (url_index, url) in urls.iter().enumerate() {
                let start = std::time::Instant::now();
                let req = self.auth(self.http.post(url).json(snap));
                match req.send().await {
                    Ok(r) => {
                        let s = r.status();
                        let content_type = response_content_type(&r);
                        let content_length = r.content_length();
                        let body = if self.cfg.http_debug_bodies {
                            Some(r.text().await.unwrap_or_default())
                        } else {
                            None
                        };

                        if self.cfg.http_debug {
                            debug!(
                                method = "POST",
                                url = %url,
                                status = %s,
                                content_type = %content_type,
                                content_length = ?content_length,
                                elapsed_ms = start.elapsed().as_millis(),
                                body_preview = %body_preview_opt(if self.cfg.http_debug_bodies {
                                    body.as_deref()
                                } else {
                                    None
                                }),
                                "inventory response"
                            );
                        }

                        if s.is_success() {
                            return Ok(());
                        }

                        if s == StatusCode::NOT_FOUND && url_index + 1 < urls.len() {
                            if self.cfg.http_debug {
                                debug!(
                                    method = "POST",
                                    url = %url,
                                    status = %s,
                                    "inventory route returned 404; trying fallback route"
                                );
                            }
                            continue;
                        }

                        if s.is_client_error()
                            && s != StatusCode::REQUEST_TIMEOUT
                            && s != StatusCode::TOO_MANY_REQUESTS
                        {
                            return Err(anyhow::anyhow!(
                                "hub rejected snapshot: {} (content_type={}, body_preview={})",
                                s,
                                content_type,
                                error_body_preview_opt(body.as_deref(), self.cfg.http_debug_bodies)
                            ));
                        }
                        last_err = Some(anyhow::anyhow!("attempt {} status {}", i + 1, s));
                    }
                    Err(e) => {
                        last_err = Some(anyhow::anyhow!("attempt {} error: {}", i + 1, e));
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("snapshot retries exhausted")))
    }

    /// Long-poll for commands. Returns an empty batch on timeout (normal).
    /// Uses a per-call timeout slightly larger than the server-side wait window.
    pub async fn poll_commands(&self) -> Result<CommandBatch> {
        let urls = self
            .routes
            .poll_urls(&self.cfg.cluster_id, self.cfg.poll_wait.as_secs());
        let timeout = self.cfg.poll_wait + Duration::from_secs(10);
        if self.cfg.http_debug {
            debug!(
                method = "GET",
                urls = ?urls,
                timeout_secs = timeout.as_secs(),
                "polling commands"
            );
        }

        for (url_index, url) in urls.iter().enumerate() {
            let req = self.auth(self.http.get(url).timeout(timeout));
            let start = std::time::Instant::now();

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) if e.is_timeout() => return Ok(CommandBatch { commands: vec![] }),
                Err(e) => return Err(e.into()),
            };

            let status = resp.status();
            let content_type = response_content_type(&resp);
            let content_length = resp.content_length();
            if status == StatusCode::NO_CONTENT {
                if self.cfg.http_debug {
                    debug!(
                        method = "GET",
                        url = %url,
                        status = %status,
                        content_type = %content_type,
                        content_length = ?content_length,
                        elapsed_ms = start.elapsed().as_millis(),
                        "poll returned no content"
                    );
                }
                return Ok(CommandBatch { commands: vec![] });
            }

            let body = resp.text().await.unwrap_or_default();
            if self.cfg.http_debug {
                debug!(
                    method = "GET",
                    url = %url,
                    status = %status,
                    content_type = %content_type,
                    content_length = ?content_length,
                    elapsed_ms = start.elapsed().as_millis(),
                    body_preview = %body_preview_opt(self.cfg.http_debug_bodies.then_some(body.as_str())),
                    "poll response"
                );
            }

            if status == StatusCode::NOT_FOUND && url_index + 1 < urls.len() {
                if self.cfg.http_debug {
                    debug!(
                        method = "GET",
                        url = %url,
                        status = %status,
                        "poll route returned 404; trying fallback route"
                    );
                }
                continue;
            }

            if !status.is_success() {
                return Err(anyhow::anyhow!(
                    "poll status {} (content_type={}, body_preview={})",
                    status,
                    content_type,
                    error_body_preview(Some(body.as_str()), self.cfg.http_debug_bodies)
                ));
            }

            if body.trim().is_empty() {
                return Ok(CommandBatch { commands: vec![] });
            }

            return serde_json::from_str(&body).map_err(|e| {
                anyhow::anyhow!(
                    "error decoding response body: {} (status={}, content_type={}, body_preview={})",
                    e,
                    status,
                    content_type,
                    error_body_preview(Some(body.as_str()), self.cfg.http_debug_bodies)
                )
            });
        }

        Err(anyhow::anyhow!("poll status 404 Not Found"))
    }

    /// Acknowledge a command result.
    pub async fn ack_command(&self, result: &CommandResult) -> Result<()> {
        let urls = self
            .routes
            .ack_urls(&self.cfg.cluster_id, &result.command_id);
        if self.cfg.http_debug {
            let request_body = if self.cfg.http_debug_bodies {
                Some(
                    serde_json::to_string(result)
                        .unwrap_or_else(|_| "<serialization error>".into()),
                )
            } else {
                None
            };
            debug!(
                method = "POST",
                urls = ?urls,
                request_preview_enabled = self.cfg.http_debug_bodies,
                request_body_preview = %body_preview_opt(request_body.as_deref()),
                command_id = %result.command_id,
                status = result.status,
                dry_run = ?result.dry_run,
                "sending command ack"
            );
        }

        for (url_index, url) in urls.iter().enumerate() {
            let start = std::time::Instant::now();
            let req = self.auth(self.http.post(url).json(result));
            let resp = req.send().await?;
            let status = resp.status();
            let content_type = response_content_type(&resp);
            let content_length = resp.content_length();
            let body = if self.cfg.http_debug_bodies {
                Some(resp.text().await.unwrap_or_default())
            } else {
                None
            };

            if self.cfg.http_debug {
                debug!(
                    method = "POST",
                    url = %url,
                    status = %status,
                    content_type = %content_type,
                    content_length = ?content_length,
                    elapsed_ms = start.elapsed().as_millis(),
                    body_preview = %body_preview_opt(if self.cfg.http_debug_bodies {
                        body.as_deref()
                    } else {
                        None
                    }),
                    "ack response"
                );
            }

            if status == StatusCode::NOT_FOUND && url_index + 1 < urls.len() {
                if self.cfg.http_debug {
                    debug!(
                        method = "POST",
                        url = %url,
                        status = %status,
                        "ack route returned 404; trying fallback route"
                    );
                }
                continue;
            }

            if !status.is_success() {
                warn!(
                    "ack returned {} (content_type={}, body_preview={})",
                    status,
                    content_type,
                    error_body_preview_opt(body.as_deref(), self.cfg.http_debug_bodies)
                );
            }
            break;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct HubRoutes {
    legacy_base: String,
    api_base: String,
    prefer_api: bool,
}

impl HubRoutes {
    fn new(cfg: &Config) -> Self {
        let prefer_api = cfg.hub_url.ends_with("/api");
        let (legacy_base, api_base) = if prefer_api {
            (
                cfg.hub_url.trim_end_matches("/api").to_string(),
                cfg.hub_url.clone(),
            )
        } else {
            (cfg.hub_url.clone(), format!("{}/api", cfg.hub_url))
        };

        Self {
            legacy_base,
            api_base,
            prefer_api,
        }
    }

    fn inventory_urls(&self, cluster_id: &str) -> Vec<String> {
        let legacy = format!("{}/v1/clusters/{}/inventory", self.legacy_base, cluster_id);
        let api = format!("{}/v1/agent/ingest", self.api_base);
        self.ordered(legacy, api)
    }

    fn poll_urls(&self, cluster_id: &str, wait_secs: u64) -> Vec<String> {
        let legacy = format!(
            "{}/v1/clusters/{}/commands/poll?wait={}s",
            self.legacy_base, cluster_id, wait_secs
        );
        let api = format!(
            "{}/v1/clusters/{}/commands/poll?wait={}s",
            self.api_base, cluster_id, wait_secs
        );
        self.ordered(legacy, api)
    }

    fn ack_urls(&self, cluster_id: &str, command_id: &str) -> Vec<String> {
        let legacy = format!(
            "{}/v1/clusters/{}/commands/{}/ack",
            self.legacy_base, cluster_id, command_id
        );
        let api = format!(
            "{}/v1/clusters/{}/commands/{}/ack",
            self.api_base, cluster_id, command_id
        );
        self.ordered(legacy, api)
    }

    fn ordered(&self, legacy: String, api: String) -> Vec<String> {
        if self.prefer_api {
            dedupe_urls([api, legacy])
        } else {
            dedupe_urls([legacy, api])
        }
    }
}

fn dedupe_urls<const N: usize>(urls: [String; N]) -> Vec<String> {
    let mut out = Vec::with_capacity(N);
    for url in urls {
        if out.iter().all(|u| u != &url) {
            out.push(url);
        }
    }
    out
}

fn response_content_type(resp: &reqwest::Response) -> String {
    resp.headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("<none>")
        .to_string()
}

fn body_preview_opt(body: Option<&str>) -> String {
    match body {
        Some(b) => body_preview(b),
        None => "<disabled>".into(),
    }
}

fn body_preview(body: &str) -> String {
    const MAX_PREVIEW_CHARS: usize = 200;
    let sanitized = sanitize_for_log(body);
    let trimmed = sanitized.trim();
    let mut preview = trimmed.chars().take(MAX_PREVIEW_CHARS).collect::<String>();
    if trimmed.chars().count() > MAX_PREVIEW_CHARS {
        preview.push_str("...");
    }
    preview
}

fn sanitize_for_log(input: &str) -> String {
    input
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

fn error_body_preview(body: Option<&str>, bodies_enabled: bool) -> String {
    if !bodies_enabled {
        return "<body logging disabled>".into();
    }
    body_preview_opt(body)
}

fn error_body_preview_opt(body: Option<&str>, bodies_enabled: bool) -> String {
    if !bodies_enabled {
        return "<body logging disabled>".into();
    }
    body_preview_opt(body)
}
