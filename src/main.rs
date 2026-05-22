mod collector;
mod config;
mod executor;
mod health;
mod hub;
mod leader;
mod model;
mod tech;

use crate::config::Config;
use crate::executor::Executor;
use crate::hub::HubClient;
use crate::leader::LeaderState;
use crate::model::*;
use anyhow::Result;
use kube::Client as KubeClient;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::signal;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, error, info, warn};

const AGENT_NAME: &str = "Sentinella Hub Kubernetes Agent";
const WARN_SUPPRESSION_WINDOW: Duration = Duration::from_secs(60);

static WARN_SUPPRESSION: Lazy<Mutex<HashMap<String, SuppressionState>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Debug)]
struct SuppressionState {
    last_emitted_at: Instant,
    suppressed_count: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cfg = Config::from_env()?;
    info!(
        agent = AGENT_NAME,
        version = env!("CARGO_PKG_VERSION"),
        cluster_id = %cfg.cluster_id,
        hub_url = %cfg.hub_url,
        node = %cfg.node_name,
        actions_enabled = cfg.actions_enabled,
        "starting"
    );

    let kube_client = KubeClient::try_default().await?;
    let hub = Arc::new(HubClient::new(cfg.clone())?);
    let executor = Arc::new(Executor::new(cfg.clone(), kube_client.clone()));
    let leader_state = LeaderState::new();

    // Health/metrics server
    tokio::spawn(async {
        if let Err(e) = health::run().await {
            error!("health server crashed: {}", e);
        }
    });

    // Leader election loop. Holder identity = node name, so the elected pod
    // is the one running on the node that won. The lease lives in the agent's
    // own namespace.
    let leader_handle = tokio::spawn(leader::run_leader_loop(
        kube_client.clone(),
        cfg.pod_namespace.clone(),
        cfg.lease_name.clone(),
        cfg.node_name.clone(),
        cfg.lease_ttl,
        leader_state.clone(),
    ));

    // Collector loop — only sends when this pod is the leader.
    let collect_handle = tokio::spawn(collector_loop(
        cfg.clone(),
        kube_client.clone(),
        hub.clone(),
        leader_state.clone(),
    ));

    // Command poll loop — runs on every pod so node-targeted commands can be
    // picked up by the right pod when actions are enabled.
    let poll_handle = tokio::spawn(command_loop(hub.clone(), executor.clone()));

    wait_for_shutdown().await;
    info!("shutdown signal received; aborting workers");
    leader_handle.abort();
    collect_handle.abort();
    poll_handle.abort();
    Ok(())
}

fn init_tracing() {
    let filter = std::env::var("AGENT_LOG")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| {
            std::env::var("RUST_LOG")
                .ok()
                .filter(|v| !v.trim().is_empty())
        })
        .unwrap_or_else(|| "info".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .json()
        .init();
}

async fn collector_loop(cfg: Config, kube: KubeClient, hub: Arc<HubClient>, leader: LeaderState) {
    let mut ticker = interval(cfg.collect_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        if !leader.is_leader() {
            debug!("skipping snapshot send: not leader");
            health::SNAPSHOTS_SENT
                .with_label_values(&["skipped_not_leader"])
                .inc();
            continue;
        }
        match build_and_send(&cfg, &kube, &hub).await {
            Ok(()) => {
                health::SNAPSHOTS_SENT.with_label_values(&["ok"]).inc();
                info!("snapshot sent");
            }
            Err(e) => {
                health::SNAPSHOTS_SENT.with_label_values(&["error"]).inc();
                warn!("snapshot failed: {:#}", e);
            }
        }
    }
}

async fn build_and_send(cfg: &Config, kube: &KubeClient, hub: &HubClient) -> Result<()> {
    let (
        cluster,
        namespaces,
        workloads,
        pods,
        network,
        dependencies,
        configuration,
        storage,
        events,
        pod_logs,
    ) = collector::collect(
        kube,
        cfg.collect_secrets,
        cfg.collect_dependencies_tetragon,
        &cfg.tetragon_log_path,
    )
    .await?;
    let snap = InventorySnapshot {
        schema_version: 1,
        agent: AgentInfo {
            name: AGENT_NAME,
            version: reported_agent_version(cfg),
            pod_name: cfg.pod_name.clone(),
            pod_namespace: cfg.pod_namespace.clone(),
            node_name: cfg.node_name.clone(),
            actions_enabled: cfg.actions_enabled,
        },
        cluster_id: cfg.cluster_id.clone(),
        timestamp_ms: now_ms(),
        cluster,
        namespaces,
        workloads,
        pods,
        network,
        dependencies,
        configuration,
        storage,
        events,
        pod_logs,
    };
    hub.send_snapshot(&snap).await
}

fn reported_agent_version(cfg: &Config) -> String {
    cfg.agent_version_override
        .clone()
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
}

async fn command_loop(hub: Arc<HubClient>, executor: Arc<Executor>) {
    loop {
        match hub.poll_commands().await {
            Ok(batch) if batch.commands.is_empty() => {
                // Normal: long-poll timed out with no work.
            }
            Ok(batch) => {
                for cmd in batch.commands {
                    health::COMMANDS_RECEIVED.inc();
                    let result = executor.execute(&cmd).await;
                    let restart_requested = should_restart_after_ack(&result);
                    health::COMMANDS_EXECUTED
                        .with_label_values(&[result.status])
                        .inc();
                    if let Err(e) = hub.ack_command(&result).await {
                        warn!("ack failed: {:#}", e);
                    }
                    if restart_requested {
                        warn!(
                            command_id = %result.command_id,
                            "self-update requested; exiting process for immediate restart"
                        );
                        std::process::exit(0);
                    }
                }
            }
            Err(e) => {
                warn_with_suppression(
                    &format!("command_poll::{e}"),
                    &format!("command poll failed: {:#}", e),
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

fn should_restart_after_ack(result: &CommandResult) -> bool {
    result.status == "ok" && result.restart_requested.unwrap_or(false)
}

fn warn_with_suppression(key: &str, message: &str) {
    let now = Instant::now();
    let mut map = WARN_SUPPRESSION
        .lock()
        .expect("warn suppression mutex poisoned");

    match map.get_mut(key) {
        None => {
            warn!("{}", message);
            map.insert(
                key.to_string(),
                SuppressionState {
                    last_emitted_at: now,
                    suppressed_count: 0,
                },
            );
        }
        Some(state) => {
            if now.duration_since(state.last_emitted_at) >= WARN_SUPPRESSION_WINDOW {
                if state.suppressed_count > 0 {
                    warn!(
                        warning_key = %key,
                        suppressed_count = state.suppressed_count,
                        window_secs = WARN_SUPPRESSION_WINDOW.as_secs(),
                        "suppressed similar warnings"
                    );
                }
                warn!("{}", message);
                state.last_emitted_at = now;
                state.suppressed_count = 0;
            } else {
                state.suppressed_count += 1;
            }
        }
    }
}

async fn wait_for_shutdown() {
    let ctrl_c = signal::ctrl_c();
    #[cfg(unix)]
    let term = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("SIGINT"),
        _ = term => info!("SIGTERM"),
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(version_override: Option<&str>) -> Config {
        Config {
            hub_url: "https://api.hub.sentinel.la".into(),
            cluster_id: "cluster-dev".into(),
            api_key: None,
            agent_version_override: version_override.map(ToString::to_string),
            collect_interval: Duration::from_secs(60),
            poll_wait: Duration::from_secs(30),
            actions_enabled: false,
            collect_secrets: false,
            collect_dependencies_tetragon: false,
            tetragon_log_path: "/var/run/tetragon/tetragon-events.jsonl".into(),
            http_timeout: Duration::from_secs(20),
            http_debug: false,
            http_debug_bodies: false,
            pod_name: "pod-1".into(),
            pod_namespace: "sentinella".into(),
            node_name: "node-1".into(),
            lease_name: "lease".into(),
            lease_ttl: Duration::from_secs(30),
        }
    }

    #[test]
    fn reported_agent_version_uses_override_when_set() {
        let cfg = test_config(Some("dev"));
        assert_eq!(reported_agent_version(&cfg), "dev");
    }

    #[test]
    fn reported_agent_version_falls_back_to_package_version() {
        let cfg = test_config(None);
        assert_eq!(reported_agent_version(&cfg), env!("CARGO_PKG_VERSION"));
    }
}
