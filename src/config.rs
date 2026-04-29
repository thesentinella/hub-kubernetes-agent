use anyhow::{Context, Result};
use std::env;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    /// Base URL of the Sentinella Hub (e.g. https://hub.sentinel.la).
    pub hub_url: String,

    /// Identifier this cluster registers as in the Hub.
    pub cluster_id: String,

    /// Bearer token for Hub authentication.
    pub bearer_token: Option<String>,

    /// How often the leader collects and ships cluster inventory.
    pub collect_interval: Duration,

    /// Long-poll wait window for the command channel (server-side hold time).
    pub poll_wait: Duration,

    /// Master switch for action execution. Read-only when false.
    pub actions_enabled: bool,

    /// HTTP request timeout to the Hub.
    pub http_timeout: Duration,

    /// Pod name (downward API).
    pub pod_name: String,

    /// Pod namespace (downward API).
    pub pod_namespace: String,

    /// Node name this pod is running on (downward API). Used as the holder
    /// identity for leader election and to route node-targeted commands.
    pub node_name: String,

    /// Name of the Lease object used for leader election. Stored in
    /// `pod_namespace`.
    pub lease_name: String,

    /// Lease validity window. The leader renews at `lease_ttl / 3`.
    pub lease_ttl: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let hub_url = env::var("HUB_URL").context("HUB_URL not set")?;
        let cluster_id = env::var("CLUSTER_ID").context("CLUSTER_ID not set")?;

        let bearer_token = env::var("HUB_BEARER_TOKEN").ok().filter(|s| !s.is_empty());

        let collect_interval = parse_secs("COLLECT_INTERVAL_SECS", 60);
        let poll_wait = parse_secs("POLL_WAIT_SECS", 30);
        let http_timeout = parse_secs("HTTP_TIMEOUT_SECS", 20);
        let lease_ttl = parse_secs("LEASE_TTL_SECS", 30);

        let actions_enabled = env::var("ACTIONS_ENABLED")
            .ok()
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let pod_name = env::var("POD_NAME").unwrap_or_else(|_| "unknown".to_string());
        let pod_namespace = env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let node_name = env::var("NODE_NAME").unwrap_or_else(|_| "unknown-node".to_string());

        let lease_name = env::var("LEASE_NAME")
            .unwrap_or_else(|_| "sentinella-hub-k8s-agent-leader".to_string());

        Ok(Self {
            hub_url: hub_url.trim_end_matches('/').to_string(),
            cluster_id,
            bearer_token,
            collect_interval,
            poll_wait,
            actions_enabled,
            http_timeout,
            pod_name,
            pod_namespace,
            node_name,
            lease_name,
            lease_ttl,
        })
    }
}

fn parse_secs(var: &str, default: u64) -> Duration {
    Duration::from_secs(
        env::var(var)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn set_required(hub_url: &str, cluster_id: &str) {
        env::set_var("HUB_URL", hub_url);
        env::set_var("CLUSTER_ID", cluster_id);
    }

    fn clear_required() {
        env::remove_var("HUB_URL");
        env::remove_var("CLUSTER_ID");
    }

    #[test]
    fn from_env_ok() {
        set_required("https://hub.example.com", "cluster-1");
        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.hub_url, "https://hub.example.com");
        assert_eq!(cfg.cluster_id, "cluster-1");
        clear_required();
    }

    #[test]
    fn from_env_missing_hub_url() {
        env::remove_var("HUB_URL");
        env::set_var("CLUSTER_ID", "cluster-1");
        assert!(Config::from_env().is_err());
        clear_required();
    }

    #[test]
    fn from_env_missing_cluster_id() {
        env::set_var("HUB_URL", "https://hub.example.com");
        env::remove_var("CLUSTER_ID");
        assert!(Config::from_env().is_err());
        clear_required();
    }

    #[test]
    fn hub_url_trailing_slash_stripped() {
        set_required("https://hub.example.com///", "cluster-1");
        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.hub_url, "https://hub.example.com");
        clear_required();
    }

    #[test]
    fn bearer_token_empty_string_becomes_none() {
        set_required("https://hub.example.com", "cluster-1");
        env::set_var("HUB_BEARER_TOKEN", "");
        let cfg = Config::from_env().unwrap();
        assert!(cfg.bearer_token.is_none());
        env::remove_var("HUB_BEARER_TOKEN");
        clear_required();
    }

    #[test]
    fn bearer_token_set() {
        set_required("https://hub.example.com", "cluster-1");
        env::set_var("HUB_BEARER_TOKEN", "secret");
        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.bearer_token.as_deref(), Some("secret"));
        env::remove_var("HUB_BEARER_TOKEN");
        clear_required();
    }

    #[test]
    fn actions_enabled_true_values() {
        set_required("https://hub.example.com", "cluster-1");
        for val in ["true", "1"] {
            env::set_var("ACTIONS_ENABLED", val);
            let cfg = Config::from_env().unwrap();
            assert!(
                cfg.actions_enabled,
                "expected true for ACTIONS_ENABLED={val}"
            );
        }
        env::remove_var("ACTIONS_ENABLED");
        clear_required();
    }

    #[test]
    fn actions_enabled_defaults_false() {
        set_required("https://hub.example.com", "cluster-1");
        env::remove_var("ACTIONS_ENABLED");
        let cfg = Config::from_env().unwrap();
        assert!(!cfg.actions_enabled);
        clear_required();
    }

    #[test]
    fn parse_secs_default_on_invalid() {
        env::set_var("__TEST_SECS", "not_a_number");
        let d = parse_secs("__TEST_SECS", 42);
        assert_eq!(d, Duration::from_secs(42));
        env::remove_var("__TEST_SECS");
    }

    #[test]
    fn parse_secs_reads_value() {
        env::set_var("__TEST_SECS", "99");
        let d = parse_secs("__TEST_SECS", 42);
        assert_eq!(d, Duration::from_secs(99));
        env::remove_var("__TEST_SECS");
    }

    #[test]
    fn pod_defaults() {
        set_required("https://hub.example.com", "cluster-1");
        env::remove_var("POD_NAME");
        env::remove_var("POD_NAMESPACE");
        env::remove_var("NODE_NAME");
        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.pod_name, "unknown");
        assert_eq!(cfg.pod_namespace, "default");
        assert_eq!(cfg.node_name, "unknown-node");
        clear_required();
    }
}
