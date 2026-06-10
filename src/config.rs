use anyhow::{Context, Result};
use std::env;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    /// Base URL of the Sentinella Hub (e.g. https://api.hub.sentinel.la).
    pub hub_url: String,

    /// Identifier this cluster registers as in the Hub.
    pub cluster_id: String,

    /// API key for Hub authentication. Must have the `shub_` prefix.
    pub api_key: Option<String>,

    /// Optional override for the reported agent version in snapshots.
    pub agent_version_override: Option<String>,

    /// How often the leader collects and ships cluster inventory.
    pub collect_interval: Duration,

    /// Long-poll wait window for the command channel (server-side hold time).
    pub poll_wait: Duration,

    /// Master switch for action execution. Read-only when false.
    pub actions_enabled: bool,

    /// Enables cluster-wide secret metadata/key-name collection.
    pub collect_secrets: bool,

    /// Enables Tetragon-based dependency collection from the local sidecar.
    pub collect_dependencies_tetragon: bool,

    /// Local sidecar HTTP endpoint that serves recent Tetragon events.
    pub tetragon_sidecar_url: String,

    /// HTTP request timeout to the Hub.
    pub http_timeout: Duration,

    /// Enables debug logging for Hub HTTP requests/responses.
    pub http_debug: bool,

    /// Enables bounded response/request body previews in Hub HTTP debug logs.
    pub http_debug_bodies: bool,

    /// Enables full request/response body logging for debugging.
    pub full_debug: bool,

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

        let api_key = env::var("HUB_API_KEY").ok().filter(|s| !s.is_empty());
        let agent_version_override = parse_non_empty_env("AGENT_VERSION_OVERRIDE");

        let collect_interval = parse_secs("COLLECT_INTERVAL_SECS", 60);
        let poll_wait = parse_secs("POLL_WAIT_SECS", 30);
        let http_timeout = parse_secs("HTTP_TIMEOUT_SECS", 20);
        let lease_ttl = parse_secs("LEASE_TTL_SECS", 30);

        let full_debug = env_flag("FULL_DEBUG");
        let http_debug = full_debug || env_flag("AGENT_HTTP_DEBUG");
        let http_debug_bodies = full_debug || env_flag("AGENT_HTTP_DEBUG_BODIES");

        let actions_enabled = env::var("ACTIONS_ENABLED")
            .ok()
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);
        let collect_secrets = env_flag("COLLECT_SECRETS");
        let collect_dependencies_tetragon = env_flag("COLLECT_DEPENDENCIES_TETRAGON");
        let tetragon_sidecar_url = env::var("TETRAGON_SIDECAR_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:9801/events".to_string());

        let pod_name = env::var("POD_NAME").unwrap_or_else(|_| "unknown".to_string());
        let pod_namespace = env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let node_name = env::var("NODE_NAME").unwrap_or_else(|_| "unknown-node".to_string());

        let lease_name = env::var("LEASE_NAME")
            .unwrap_or_else(|_| "sentinella-hub-k8s-agent-leader".to_string());

        Ok(Self {
            hub_url: hub_url.trim_end_matches('/').to_string(),
            cluster_id,
            api_key,
            agent_version_override,
            collect_interval,
            poll_wait,
            actions_enabled,
            collect_secrets,
            collect_dependencies_tetragon,
            tetragon_sidecar_url,
            http_timeout,
            http_debug,
            http_debug_bodies,
            full_debug,
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

fn env_flag(var: &str) -> bool {
    env::var(var)
        .ok()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}

fn parse_non_empty_env(var: &str) -> Option<String> {
    env::var(var)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .filter(|value| value.len() <= 128)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    unsafe fn reset_env() {
        for var in [
            "HUB_URL",
            "CLUSTER_ID",
            "HUB_API_KEY",
            "AGENT_VERSION_OVERRIDE",
            "COLLECT_INTERVAL_SECS",
            "POLL_WAIT_SECS",
            "HTTP_TIMEOUT_SECS",
            "LEASE_TTL_SECS",
            "FULL_DEBUG",
            "AGENT_HTTP_DEBUG",
            "AGENT_HTTP_DEBUG_BODIES",
            "ACTIONS_ENABLED",
            "COLLECT_SECRETS",
            "COLLECT_DEPENDENCIES_TETRAGON",
            "TETRAGON_SIDECAR_URL",
            "POD_NAME",
            "POD_NAMESPACE",
            "NODE_NAME",
            "LEASE_NAME",
        ] {
            unsafe { env::remove_var(var) };
        }
    }

    unsafe fn set_required(hub_url: &str, cluster_id: &str) {
        unsafe {
            env::set_var("HUB_URL", hub_url);
            env::set_var("CLUSTER_ID", cluster_id);
        }
    }

    unsafe fn clear_required() {
        unsafe { reset_env() };
    }

    #[test]
    fn from_env_ok() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.hub_url, "https://hub.example.com");
            assert_eq!(cfg.cluster_id, "cluster-1");
            assert!(!cfg.collect_dependencies_tetragon);
            assert_eq!(cfg.tetragon_sidecar_url, "http://127.0.0.1:9801/events");
            clear_required();
        }
    }

    #[test]
    fn from_env_missing_hub_url() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            env::set_var("CLUSTER_ID", "cluster-1");
            assert!(Config::from_env().is_err());
            clear_required();
        }
    }

    #[test]
    fn from_env_missing_cluster_id() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            env::set_var("HUB_URL", "https://hub.example.com");
            assert!(Config::from_env().is_err());
            clear_required();
        }
    }

    #[test]
    fn hub_url_trailing_slash_stripped() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com///", "cluster-1");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.hub_url, "https://hub.example.com");
            clear_required();
        }
    }

    #[test]
    fn api_key_empty_string_becomes_none() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            env::set_var("HUB_API_KEY", "");
            let cfg = Config::from_env().unwrap();
            assert!(cfg.api_key.is_none());
            env::remove_var("HUB_API_KEY");
            clear_required();
        }
    }

    #[test]
    fn api_key_set() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            env::set_var("HUB_API_KEY", "shub_test123");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.api_key.as_deref(), Some("shub_test123"));
            env::remove_var("HUB_API_KEY");
            clear_required();
        }
    }

    #[test]
    fn agent_version_override_set() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            env::set_var("AGENT_VERSION_OVERRIDE", "dev");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.agent_version_override.as_deref(), Some("dev"));
            env::remove_var("AGENT_VERSION_OVERRIDE");
            clear_required();
        }
    }

    #[test]
    fn agent_version_override_empty_becomes_none() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            env::set_var("AGENT_VERSION_OVERRIDE", "   ");
            let cfg = Config::from_env().unwrap();
            assert!(cfg.agent_version_override.is_none());
            env::remove_var("AGENT_VERSION_OVERRIDE");
            clear_required();
        }
    }

    #[test]
    fn agent_version_override_too_long_becomes_none() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            env::set_var("AGENT_VERSION_OVERRIDE", "x".repeat(129));
            let cfg = Config::from_env().unwrap();
            assert!(cfg.agent_version_override.is_none());
            env::remove_var("AGENT_VERSION_OVERRIDE");
            clear_required();
        }
    }

    #[test]
    fn actions_enabled_true_values() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            for val in ["true", "1"] {
                env::set_var("ACTIONS_ENABLED", val);
                let cfg = Config::from_env().unwrap();
                assert!(
                    cfg.actions_enabled,
                    "expected true for ACTIONS_ENABLED={val}"
                );
                assert!(!cfg.collect_secrets);
            }
            env::remove_var("ACTIONS_ENABLED");
            clear_required();
        }
    }

    #[test]
    fn actions_enabled_defaults_false() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            let cfg = Config::from_env().unwrap();
            assert!(!cfg.actions_enabled);
            assert!(!cfg.collect_secrets);
            clear_required();
        }
    }

    #[test]
    fn collect_secrets_true_values() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            for val in ["true", "1"] {
                env::set_var("COLLECT_SECRETS", val);
                let cfg = Config::from_env().unwrap();
                assert!(
                    cfg.collect_secrets,
                    "expected true for COLLECT_SECRETS={val}"
                );
            }
            env::remove_var("COLLECT_SECRETS");
            clear_required();
        }
    }

    #[test]
    fn collect_dependencies_tetragon_and_path_values() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            env::set_var("COLLECT_DEPENDENCIES_TETRAGON", "true");
            env::set_var("TETRAGON_SIDECAR_URL", "http://127.0.0.1:7777/events");
            let cfg = Config::from_env().unwrap();
            assert!(cfg.collect_dependencies_tetragon);
            assert_eq!(cfg.tetragon_sidecar_url, "http://127.0.0.1:7777/events");
            clear_required();
        }
    }

    #[test]
    fn http_debug_flags_true_values() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            for val in ["true", "1"] {
                env::set_var("AGENT_HTTP_DEBUG", val);
                env::set_var("AGENT_HTTP_DEBUG_BODIES", val);
                let cfg = Config::from_env().unwrap();
                assert!(cfg.http_debug, "expected true for AGENT_HTTP_DEBUG={val}");
                assert!(
                    cfg.http_debug_bodies,
                    "expected true for AGENT_HTTP_DEBUG_BODIES={val}"
                );
            }
            env::remove_var("AGENT_HTTP_DEBUG");
            env::remove_var("AGENT_HTTP_DEBUG_BODIES");
            clear_required();
        }
    }

    #[test]
    fn full_debug_implies_http_debug_flags() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            env::set_var("FULL_DEBUG", "true");
            let cfg = Config::from_env().unwrap();
            assert!(cfg.full_debug);
            assert!(cfg.http_debug);
            assert!(cfg.http_debug_bodies);
            clear_required();
        }
    }

    #[test]
    fn http_debug_flags_default_false() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            let cfg = Config::from_env().unwrap();
            assert!(!cfg.http_debug);
            assert!(!cfg.http_debug_bodies);
            clear_required();
        }
    }

    #[test]
    fn parse_secs_default_on_invalid() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            env::set_var("__TEST_SECS", "not_a_number");
            let d = parse_secs("__TEST_SECS", 42);
            assert_eq!(d, Duration::from_secs(42));
            env::remove_var("__TEST_SECS");
        }
    }

    #[test]
    fn parse_secs_reads_value() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            env::set_var("__TEST_SECS", "99");
            let d = parse_secs("__TEST_SECS", 42);
            assert_eq!(d, Duration::from_secs(99));
            env::remove_var("__TEST_SECS");
        }
    }

    #[test]
    fn pod_defaults() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.pod_name, "unknown");
            assert_eq!(cfg.pod_namespace, "default");
            assert_eq!(cfg.node_name, "unknown-node");
            clear_required();
        }
    }
}
