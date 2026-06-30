use crate::model::KV;
use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::env;
use std::time::Duration;

pub const AGENT_CONFIG_ENV_ALLOWLIST: &[&str] = &[
    "ACTIONS_ENABLED",
    "AGENT_HTTP_DEBUG",
    "AGENT_HTTP_DEBUG_BODIES",
    "AGENT_LOG",
    "AGENT_VERSION_OVERRIDE",
    "CLUSTER_ID",
    "COLLECT_DEPENDENCIES_TETRAGON",
    "COLLECT_INTERVAL_SECS",
    "COLLECT_SECRETS",
    "FULL_DEBUG",
    "HTTP_TIMEOUT_SECS",
    "HUB_URL",
    "LEASE_NAME",
    "LEASE_TTL_SECS",
    "POLL_WAIT_SECS",
    "READONLY_COMMANDS_ENABLED",
    "POSTGRESQL_MONITORING_DATABASE",
    "POSTGRESQL_MONITORING_ENABLED",
    "POSTGRESQL_MONITORING_HOST",
    "POSTGRESQL_MONITORING_PORT",
    "POSTGRESQL_MONITORING_SECRET_NAME",
    "POSTGRESQL_MONITORING_NAMESPACES",
    "POSTGRESQL_MONITORING_SSLMODE",
    "POSTGRESQL_MONITORING_USER",
    "TETRAGON_ENDPOINT_DISCOVERY_ENABLED",
    "TETRAGON_GRPC_ADDRESS",
    "TETRAGON_GRPC_PORT",
    "TETRAGON_SERVICE_NAME",
    "TETRAGON_SERVICE_NAMESPACE",
    "TECH_DETECT_PROCESS",
    "WORKLOAD_MONITORING_ENABLED",
    "WORKLOAD_MONITORING_NAMESPACES",
    "WORKLOAD_MONITORING_TARGETS",
];

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

    /// Master switch for read-only diagnostic commands.
    pub readonly_commands_enabled: bool,

    /// Enables cluster-wide secret metadata/key-name collection.
    pub collect_secrets: bool,

    /// Enables Tetragon-based dependency collection over gRPC.
    pub collect_dependencies_tetragon: bool,

    /// Enables discovery of Tetragon gRPC endpoints via EndpointSlice.
    pub tetragon_endpoint_discovery_enabled: bool,

    /// Controls whether Tetragon connectivity is required for readiness.
    pub tetragon_required_for_readiness: bool,

    /// Tetragon gRPC server address for dependency collection.
    pub tetragon_grpc_address: String,

    /// Port used when discovering Tetragon gRPC endpoints.
    pub tetragon_grpc_port: u16,

    /// Namespace containing the Tetragon Service used for discovery.
    pub tetragon_service_namespace: String,

    /// Service name used for Tetragon endpoint discovery.
    pub tetragon_service_name: String,

    /// HTTP request timeout to the Hub.
    pub http_timeout: Duration,

    /// Enables debug logging for Hub HTTP requests/responses.
    pub http_debug: bool,

    /// Enables bounded response/request body previews in Hub HTTP debug logs.
    pub http_debug_bodies: bool,

    /// Enables full request/response body logging for debugging.
    pub full_debug: bool,

    /// Base log filter for tracing subscriber initialization.
    pub agent_log: String,

    /// Enables process-level technology detection from container command/args.
    pub tech_detect_process: bool,

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

    /// Master switch for the workload monitoring plugin. When false (default)
    /// the plugin block is omitted from the snapshot payload entirely.
    pub workload_monitoring_enabled: bool,

    /// Allowlist of namespaces to monitor. YAML list env value. Empty means
    /// the plugin is disabled regardless of the `enabled` flag.
    pub workload_monitoring_namespaces: Vec<String>,

    /// Tech detection targets enabled for the plugin. Comma-separated env
    /// value. Defaults to `["angular", "spring_boot", "oracle_database"]`.
    pub workload_monitoring_targets: Vec<String>,

    /// Master switch for the PostgreSQL monitoring plugin. When false (default)
    /// the plugin block is omitted from the snapshot payload entirely.
    pub postgresql_monitoring_enabled: bool,

    /// Allowlist of namespaces to inspect for PostgreSQL workloads.
    pub postgresql_monitoring_namespaces: Vec<String>,

    /// Optional Secret name used to source PostgreSQL probe auth/TLS settings.
    pub postgresql_monitoring_secret_name: Option<String>,

    /// Optional host override for the PostgreSQL probe.
    pub postgresql_monitoring_host: Option<String>,

    /// Optional port override for the PostgreSQL probe.
    pub postgresql_monitoring_port: Option<u16>,

    /// Optional username override for the PostgreSQL probe.
    pub postgresql_monitoring_user: Option<String>,

    /// Optional password override for the PostgreSQL probe.
    pub postgresql_monitoring_password: Option<String>,

    /// Optional database override for the PostgreSQL probe.
    pub postgresql_monitoring_database: Option<String>,

    /// Optional sslmode override for the PostgreSQL probe.
    pub postgresql_monitoring_sslmode: Option<String>,

    /// Optional PEM-encoded root certificate override for the PostgreSQL probe.
    pub postgresql_monitoring_sslrootcert: Option<String>,
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
        let agent_log = env::var("AGENT_LOG")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "info".to_string());

        let actions_enabled = env::var("ACTIONS_ENABLED")
            .ok()
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);
        let readonly_commands_enabled = env_flag("READONLY_COMMANDS_ENABLED");
        let collect_secrets = env_flag("COLLECT_SECRETS");
        let collect_dependencies_tetragon = env_flag("COLLECT_DEPENDENCIES_TETRAGON");
        let tetragon_endpoint_discovery_enabled =
            env_flag_with_default("TETRAGON_ENDPOINT_DISCOVERY_ENABLED", true);
        let tetragon_required_for_readiness =
            env_flag_with_default("TETRAGON_REQUIRED_FOR_READINESS", true);
        let tetragon_grpc_address = env::var("TETRAGON_GRPC_ADDRESS")
            .unwrap_or_else(|_| crate::tetragon::DEFAULT_GRPC_ADDRESS.to_string());
        let tetragon_grpc_port = parse_u16_env("TETRAGON_GRPC_PORT").unwrap_or(54321);
        let tetragon_service_namespace =
            env::var("TETRAGON_SERVICE_NAMESPACE").unwrap_or_else(|_| "tetragon".to_string());
        let tetragon_service_name =
            env::var("TETRAGON_SERVICE_NAME").unwrap_or_else(|_| "tetragon-grpc".to_string());
        let tech_detect_process = env_flag("TECH_DETECT_PROCESS");

        let pod_name = env::var("POD_NAME").unwrap_or_else(|_| "unknown".to_string());
        let pod_namespace = env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let node_name = env::var("NODE_NAME").unwrap_or_else(|_| "unknown-node".to_string());

        let lease_name = env::var("LEASE_NAME")
            .unwrap_or_else(|_| "sentinella-hub-k8s-agent-leader".to_string());

        let workload_monitoring_enabled = env_flag("WORKLOAD_MONITORING_ENABLED");
        let workload_monitoring_namespaces = parse_yaml_list_env("WORKLOAD_MONITORING_NAMESPACES");
        let workload_monitoring_targets = parse_csv_env_or(
            "WORKLOAD_MONITORING_TARGETS",
            vec![
                "angular".to_string(),
                "spring_boot".to_string(),
                "oracle_database".to_string(),
            ],
        );
        let postgresql_monitoring_enabled = env_flag("POSTGRESQL_MONITORING_ENABLED");
        let postgresql_monitoring_namespaces =
            parse_yaml_list_env("POSTGRESQL_MONITORING_NAMESPACES");
        let postgresql_monitoring_secret_name =
            parse_non_empty_env("POSTGRESQL_MONITORING_SECRET_NAME");
        let postgresql_monitoring_host = parse_non_empty_env("POSTGRESQL_MONITORING_HOST");
        let postgresql_monitoring_port = parse_u16_env("POSTGRESQL_MONITORING_PORT");
        let postgresql_monitoring_user = parse_non_empty_env("POSTGRESQL_MONITORING_USER");
        let postgresql_monitoring_password = parse_non_empty_env("POSTGRESQL_MONITORING_PASSWORD");
        let postgresql_monitoring_database = parse_non_empty_env("POSTGRESQL_MONITORING_DATABASE");
        let postgresql_monitoring_sslmode = parse_non_empty_env("POSTGRESQL_MONITORING_SSLMODE");
        let postgresql_monitoring_sslrootcert =
            parse_non_empty_env("POSTGRESQL_MONITORING_SSLROOTCERT");

        Ok(Self {
            hub_url: hub_url.trim_end_matches('/').to_string(),
            cluster_id,
            api_key,
            agent_version_override,
            collect_interval,
            poll_wait,
            actions_enabled,
            readonly_commands_enabled,
            collect_secrets,
            collect_dependencies_tetragon,
            tetragon_endpoint_discovery_enabled,
            tetragon_required_for_readiness,
            tetragon_grpc_address,
            tetragon_grpc_port,
            tetragon_service_namespace,
            tetragon_service_name,
            http_timeout,
            http_debug,
            http_debug_bodies,
            full_debug,
            agent_log,
            tech_detect_process,
            pod_name,
            pod_namespace,
            node_name,
            lease_name,
            lease_ttl,
            workload_monitoring_enabled,
            workload_monitoring_namespaces,
            workload_monitoring_targets,
            postgresql_monitoring_enabled,
            postgresql_monitoring_namespaces,
            postgresql_monitoring_secret_name,
            postgresql_monitoring_host,
            postgresql_monitoring_port,
            postgresql_monitoring_user,
            postgresql_monitoring_password,
            postgresql_monitoring_database,
            postgresql_monitoring_sslmode,
            postgresql_monitoring_sslrootcert,
        })
    }
}

pub fn agent_runtime_env(cfg: &Config) -> Vec<KV> {
    AGENT_CONFIG_ENV_ALLOWLIST
        .iter()
        .filter_map(|key| {
            runtime_env_value(cfg, key).map(|value| KV {
                key: (*key).to_string(),
                value,
            })
        })
        .collect()
}

pub fn agent_configured_env(values: &BTreeMap<String, String>) -> Vec<KV> {
    AGENT_CONFIG_ENV_ALLOWLIST
        .iter()
        .filter_map(|key| {
            values.get(*key).and_then(|value| {
                configured_env_value(key, value).map(|value| KV {
                    key: (*key).to_string(),
                    value,
                })
            })
        })
        .collect()
}

fn runtime_env_value(cfg: &Config, key: &str) -> Option<String> {
    match key {
        "ACTIONS_ENABLED" => Some(bool_string(cfg.actions_enabled)),
        "READONLY_COMMANDS_ENABLED" => Some(bool_string(cfg.readonly_commands_enabled)),
        "AGENT_HTTP_DEBUG" => Some(bool_string(cfg.http_debug)),
        "AGENT_HTTP_DEBUG_BODIES" => Some(bool_string(cfg.http_debug_bodies)),
        "AGENT_LOG" => Some(cfg.agent_log.clone()),
        "AGENT_VERSION_OVERRIDE" => cfg.agent_version_override.clone(),
        "CLUSTER_ID" => Some(cfg.cluster_id.clone()),
        "COLLECT_DEPENDENCIES_TETRAGON" => Some(bool_string(cfg.collect_dependencies_tetragon)),
        "TETRAGON_ENDPOINT_DISCOVERY_ENABLED" => {
            Some(bool_string(cfg.tetragon_endpoint_discovery_enabled))
        }
        "COLLECT_INTERVAL_SECS" => Some(cfg.collect_interval.as_secs().to_string()),
        "COLLECT_SECRETS" => Some(bool_string(cfg.collect_secrets)),
        "FULL_DEBUG" => Some(bool_string(cfg.full_debug)),
        "HTTP_TIMEOUT_SECS" => Some(cfg.http_timeout.as_secs().to_string()),
        "HUB_URL" => Some(cfg.hub_url.clone()),
        "LEASE_NAME" => Some(cfg.lease_name.clone()),
        "LEASE_TTL_SECS" => Some(cfg.lease_ttl.as_secs().to_string()),
        "POLL_WAIT_SECS" => Some(cfg.poll_wait.as_secs().to_string()),
        "POSTGRESQL_MONITORING_DATABASE" => cfg.postgresql_monitoring_database.clone(),
        "TETRAGON_GRPC_ADDRESS" => Some(cfg.tetragon_grpc_address.clone()),
        "TETRAGON_GRPC_PORT" => Some(cfg.tetragon_grpc_port.to_string()),
        "TETRAGON_SERVICE_NAME" => Some(cfg.tetragon_service_name.clone()),
        "TETRAGON_SERVICE_NAMESPACE" => Some(cfg.tetragon_service_namespace.clone()),
        "TECH_DETECT_PROCESS" => Some(bool_string(cfg.tech_detect_process)),
        "POSTGRESQL_MONITORING_HOST" => cfg.postgresql_monitoring_host.clone(),
        "POSTGRESQL_MONITORING_PORT" => cfg
            .postgresql_monitoring_port
            .map(|value| value.to_string()),
        "POSTGRESQL_MONITORING_ENABLED" => Some(bool_string(cfg.postgresql_monitoring_enabled)),
        "POSTGRESQL_MONITORING_SECRET_NAME" => cfg.postgresql_monitoring_secret_name.clone(),
        "POSTGRESQL_MONITORING_NAMESPACES" => {
            Some(yaml_list_string(&cfg.postgresql_monitoring_namespaces))
        }
        "POSTGRESQL_MONITORING_SSLMODE" => cfg.postgresql_monitoring_sslmode.clone(),
        "POSTGRESQL_MONITORING_USER" => cfg.postgresql_monitoring_user.clone(),
        "WORKLOAD_MONITORING_ENABLED" => Some(bool_string(cfg.workload_monitoring_enabled)),
        "WORKLOAD_MONITORING_NAMESPACES" => {
            Some(yaml_list_string(&cfg.workload_monitoring_namespaces))
        }
        "WORKLOAD_MONITORING_TARGETS" => Some(cfg.workload_monitoring_targets.join(",")),
        _ => None,
    }
}

fn configured_env_value(key: &str, value: &str) -> Option<String> {
    let trimmed = value.trim();
    match key {
        "ACTIONS_ENABLED"
        | "AGENT_HTTP_DEBUG"
        | "AGENT_HTTP_DEBUG_BODIES"
        | "COLLECT_DEPENDENCIES_TETRAGON"
        | "TETRAGON_ENDPOINT_DISCOVERY_ENABLED"
        | "COLLECT_SECRETS"
        | "FULL_DEBUG"
        | "READONLY_COMMANDS_ENABLED"
        | "POSTGRESQL_MONITORING_ENABLED"
        | "TECH_DETECT_PROCESS" => Some(bool_string(trimmed == "true" || trimmed == "1")),
        "AGENT_LOG" => Some(if trimmed.is_empty() {
            "info".to_string()
        } else {
            trimmed.to_string()
        }),
        "AGENT_VERSION_OVERRIDE" => parse_non_empty_value(trimmed),
        "COLLECT_INTERVAL_SECS"
        | "HTTP_TIMEOUT_SECS"
        | "LEASE_TTL_SECS"
        | "POLL_WAIT_SECS"
        | "TETRAGON_GRPC_PORT" => trimmed
            .parse::<u16>()
            .map(|value| value.to_string())
            .ok()
            .or_else(|| Some(trimmed.to_string())),
        "POSTGRESQL_MONITORING_PORT" => trimmed
            .parse::<u16>()
            .map(|value| value.to_string())
            .ok()
            .or_else(|| Some(trimmed.to_string())),
        "POSTGRESQL_MONITORING_DATABASE"
        | "POSTGRESQL_MONITORING_HOST"
        | "POSTGRESQL_MONITORING_SECRET_NAME"
        | "POSTGRESQL_MONITORING_SSLMODE"
        | "POSTGRESQL_MONITORING_USER" => parse_non_empty_value(trimmed),
        "HUB_URL" => Some(trimmed.trim_end_matches('/').to_string()),
        _ => Some(trimmed.to_string()),
    }
}

fn bool_string(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

fn parse_non_empty_value(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > 128 {
        None
    } else {
        Some(trimmed.to_string())
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

fn parse_csv_env(var: &str) -> Vec<String> {
    env::var(var)
        .ok()
        .map(|raw| parse_csv_values(&raw))
        .unwrap_or_default()
}

fn parse_yaml_list_env(var: &str) -> Vec<String> {
    let Some(raw) = env::var(var).ok() else {
        return Vec::new();
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    serde_yaml::from_str::<Vec<String>>(trimmed)
        .ok()
        .unwrap_or_else(|| parse_csv_values(trimmed))
}

fn parse_csv_values(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn yaml_list_string(values: &[String]) -> String {
    if values.is_empty() {
        return String::new();
    }

    serde_yaml::to_string(values)
        .map(|yaml| yaml.trim_end().to_string())
        .unwrap_or_else(|_| values.join(","))
}

fn parse_csv_env_or(var: &str, default: Vec<String>) -> Vec<String> {
    let parsed = parse_csv_env(var);
    if parsed.is_empty() { default } else { parsed }
}

fn parse_u16_env(var: &str) -> Option<u16> {
    env::var(var).ok()?.trim().parse::<u16>().ok()
}

fn env_flag_with_default(var: &str, default: bool) -> bool {
    env::var(var)
        .ok()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(default)
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
            "TETRAGON_REQUIRED_FOR_READINESS",
            "TETRAGON_GRPC_ADDRESS",
            "TECH_DETECT_PROCESS",
            "POD_NAME",
            "POD_NAMESPACE",
            "NODE_NAME",
            "LEASE_NAME",
            "POSTGRESQL_MONITORING_DATABASE",
            "POSTGRESQL_MONITORING_HOST",
            "POSTGRESQL_MONITORING_PASSWORD",
            "POSTGRESQL_MONITORING_PORT",
            "POSTGRESQL_MONITORING_SECRET_NAME",
            "POSTGRESQL_MONITORING_SSLMODE",
            "POSTGRESQL_MONITORING_SSLROOTCERT",
            "POSTGRESQL_MONITORING_USER",
            "WORKLOAD_MONITORING_ENABLED",
            "WORKLOAD_MONITORING_NAMESPACES",
            "WORKLOAD_MONITORING_TARGETS",
            "POSTGRESQL_MONITORING_ENABLED",
            "POSTGRESQL_MONITORING_NAMESPACES",
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
            assert!(cfg.tetragon_required_for_readiness);
            assert_eq!(
                cfg.tetragon_grpc_address,
                crate::tetragon::DEFAULT_GRPC_ADDRESS
            );
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
    fn readonly_commands_enabled_true_values() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            for val in ["true", "1"] {
                env::set_var("READONLY_COMMANDS_ENABLED", val);
                let cfg = Config::from_env().unwrap();
                assert!(
                    cfg.readonly_commands_enabled,
                    "expected true for READONLY_COMMANDS_ENABLED={val}"
                );
            }
            env::remove_var("READONLY_COMMANDS_ENABLED");
            clear_required();
        }
    }

    #[test]
    fn readonly_commands_enabled_defaults_false() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            let cfg = Config::from_env().unwrap();
            assert!(!cfg.readonly_commands_enabled);
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
            env::set_var("TETRAGON_REQUIRED_FOR_READINESS", "false");
            env::set_var(
                "TETRAGON_GRPC_ADDRESS",
                "tetragon-grpc.tetragon.svc.cluster.local:54321",
            );
            let cfg = Config::from_env().unwrap();
            assert!(cfg.collect_dependencies_tetragon);
            assert!(cfg.tetragon_endpoint_discovery_enabled);
            assert!(!cfg.tetragon_required_for_readiness);
            assert_eq!(
                cfg.tetragon_grpc_address,
                "tetragon-grpc.tetragon.svc.cluster.local:54321"
            );
            assert_eq!(cfg.tetragon_grpc_port, 54321);
            assert_eq!(cfg.tetragon_service_namespace, "tetragon");
            assert_eq!(cfg.tetragon_service_name, "tetragon-grpc");
            clear_required();
        }
    }

    #[test]
    fn agent_runtime_env_filters_and_normalizes_values() {
        let cfg = Config {
            hub_url: "https://hub.example.com".into(),
            cluster_id: "cluster-1".into(),
            api_key: Some("shub_secret".into()),
            agent_version_override: Some("dev".into()),
            collect_interval: Duration::from_secs(60),
            poll_wait: Duration::from_secs(30),
            actions_enabled: true,
            readonly_commands_enabled: true,
            collect_secrets: false,
            collect_dependencies_tetragon: true,
            tetragon_endpoint_discovery_enabled: true,
            tetragon_required_for_readiness: true,
            tetragon_grpc_address: "tetragon:54321".into(),
            tetragon_grpc_port: 54321,
            tetragon_service_namespace: "tetragon".into(),
            tetragon_service_name: "tetragon-grpc".into(),
            http_timeout: Duration::from_secs(20),
            http_debug: true,
            http_debug_bodies: false,
            full_debug: false,
            agent_log: "debug".into(),
            tech_detect_process: false,
            pod_name: "pod-1".into(),
            pod_namespace: "sentinella".into(),
            node_name: "node-1".into(),
            lease_name: "lease".into(),
            lease_ttl: Duration::from_secs(30),
            workload_monitoring_enabled: false,
            workload_monitoring_namespaces: Vec::new(),
            workload_monitoring_targets: vec!["angular".into(), "spring_boot".into()],
            postgresql_monitoring_enabled: false,
            postgresql_monitoring_namespaces: Vec::new(),
            postgresql_monitoring_secret_name: Some("postgresql-monitoring".into()),
            postgresql_monitoring_host: Some("postgresql.customer-db.svc.cluster.local".into()),
            postgresql_monitoring_port: Some(5432),
            postgresql_monitoring_user: Some("postgres".into()),
            postgresql_monitoring_password: None,
            postgresql_monitoring_database: Some("postgres".into()),
            postgresql_monitoring_sslmode: Some("require".into()),
            postgresql_monitoring_sslrootcert: None,
        };

        let env = agent_runtime_env(&cfg);
        let keys = env
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, AGENT_CONFIG_ENV_ALLOWLIST);
        assert!(!keys.contains(&"HUB_API_KEY"));
        assert!(!keys.contains(&"POD_NAME"));
        assert!(!keys.contains(&"POD_NAMESPACE"));
        assert!(!keys.contains(&"NODE_NAME"));
        assert_eq!(
            env.iter()
                .find(|entry| entry.key == "ACTIONS_ENABLED")
                .unwrap()
                .value,
            "true"
        );
        assert_eq!(
            env.iter()
                .find(|entry| entry.key == "COLLECT_INTERVAL_SECS")
                .unwrap()
                .value,
            "60"
        );
    }

    #[test]
    fn agent_configured_env_filters_allowlisted_keys_only() {
        let values = BTreeMap::from([
            ("HUB_URL".to_string(), "https://hub.example.com".to_string()),
            ("AGENT_LOG".to_string(), "info".to_string()),
            ("HUB_API_KEY".to_string(), "shub_secret".to_string()),
            ("POD_NAME".to_string(), "pod-1".to_string()),
            ("UNRELATED".to_string(), "value".to_string()),
        ]);

        let env = agent_configured_env(&values);
        assert_eq!(
            env,
            vec![
                KV {
                    key: "AGENT_LOG".into(),
                    value: "info".into(),
                },
                KV {
                    key: "HUB_URL".into(),
                    value: "https://hub.example.com".into(),
                },
            ]
        );
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

    #[test]
    fn workload_monitoring_defaults() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            let cfg = Config::from_env().unwrap();
            assert!(!cfg.workload_monitoring_enabled);
            assert!(cfg.workload_monitoring_namespaces.is_empty());
            assert_eq!(
                cfg.workload_monitoring_targets,
                vec!["angular", "spring_boot", "oracle_database"]
            );
            clear_required();
        }
    }

    #[test]
    fn workload_monitoring_parses_yaml_list_values() {
        let _lock = ENV_LOCK.lock().unwrap();
        unsafe {
            reset_env();
            set_required("https://hub.example.com", "cluster-1");
            env::set_var("WORKLOAD_MONITORING_ENABLED", "true");
            env::set_var(
                "WORKLOAD_MONITORING_NAMESPACES",
                "- customer-app\n- customer-db\n",
            );
            env::set_var("WORKLOAD_MONITORING_TARGETS", "angular,spring_boot");
            let cfg = Config::from_env().unwrap();
            assert!(cfg.workload_monitoring_enabled);
            assert_eq!(
                cfg.workload_monitoring_namespaces,
                vec!["customer-app", "customer-db"]
            );
            assert_eq!(
                cfg.workload_monitoring_targets,
                vec!["angular", "spring_boot"]
            );
            clear_required();
        }
    }

    #[test]
    fn workload_monitoring_runtime_env_serializes_yaml_list() {
        let cfg = Config {
            hub_url: "https://hub.example.com".into(),
            cluster_id: "cluster-1".into(),
            api_key: Some("shub_secret".into()),
            agent_version_override: None,
            collect_interval: Duration::from_secs(60),
            poll_wait: Duration::from_secs(30),
            actions_enabled: false,
            readonly_commands_enabled: false,
            collect_secrets: false,
            collect_dependencies_tetragon: false,
            tetragon_endpoint_discovery_enabled: true,
            tetragon_required_for_readiness: true,
            tetragon_grpc_address: "tetragon:54321".into(),
            tetragon_grpc_port: 54321,
            tetragon_service_namespace: "tetragon".into(),
            tetragon_service_name: "tetragon-grpc".into(),
            http_timeout: Duration::from_secs(20),
            http_debug: false,
            http_debug_bodies: false,
            full_debug: false,
            agent_log: "info".into(),
            tech_detect_process: false,
            pod_name: "pod-1".into(),
            pod_namespace: "sentinella".into(),
            node_name: "node-1".into(),
            lease_name: "lease".into(),
            lease_ttl: Duration::from_secs(30),
            workload_monitoring_enabled: true,
            workload_monitoring_namespaces: vec!["customer-app".into(), "customer-db".into()],
            workload_monitoring_targets: vec!["angular".into(), "spring_boot".into()],
            postgresql_monitoring_enabled: false,
            postgresql_monitoring_namespaces: Vec::new(),
            postgresql_monitoring_secret_name: None,
            postgresql_monitoring_host: None,
            postgresql_monitoring_port: None,
            postgresql_monitoring_user: None,
            postgresql_monitoring_password: None,
            postgresql_monitoring_database: None,
            postgresql_monitoring_sslmode: None,
            postgresql_monitoring_sslrootcert: None,
        };

        let env = agent_runtime_env(&cfg);
        let namespaces = env
            .iter()
            .find(|entry| entry.key == "WORKLOAD_MONITORING_NAMESPACES")
            .unwrap();
        assert_eq!(namespaces.value, "- customer-app\n- customer-db");
    }

    #[test]
    fn postgresql_monitoring_runtime_env_serializes_yaml_list() {
        let cfg = Config {
            hub_url: "https://hub.example.com".into(),
            cluster_id: "cluster-1".into(),
            api_key: Some("shub_secret".into()),
            agent_version_override: None,
            collect_interval: Duration::from_secs(60),
            poll_wait: Duration::from_secs(30),
            actions_enabled: false,
            readonly_commands_enabled: false,
            collect_secrets: false,
            collect_dependencies_tetragon: false,
            tetragon_endpoint_discovery_enabled: true,
            tetragon_required_for_readiness: true,
            tetragon_grpc_address: "tetragon:54321".into(),
            tetragon_grpc_port: 54321,
            tetragon_service_namespace: "tetragon".into(),
            tetragon_service_name: "tetragon-grpc".into(),
            http_timeout: Duration::from_secs(20),
            http_debug: false,
            http_debug_bodies: false,
            full_debug: false,
            agent_log: "info".into(),
            tech_detect_process: false,
            pod_name: "pod-1".into(),
            pod_namespace: "sentinella".into(),
            node_name: "node-1".into(),
            lease_name: "lease".into(),
            lease_ttl: Duration::from_secs(30),
            workload_monitoring_enabled: false,
            workload_monitoring_namespaces: Vec::new(),
            workload_monitoring_targets: vec!["angular".into(), "spring_boot".into()],
            postgresql_monitoring_enabled: true,
            postgresql_monitoring_namespaces: vec!["customer-db".into(), "analytics".into()],
            postgresql_monitoring_secret_name: None,
            postgresql_monitoring_host: None,
            postgresql_monitoring_port: None,
            postgresql_monitoring_user: None,
            postgresql_monitoring_password: None,
            postgresql_monitoring_database: None,
            postgresql_monitoring_sslmode: None,
            postgresql_monitoring_sslrootcert: None,
        };

        let env = agent_runtime_env(&cfg);
        let namespaces = env
            .iter()
            .find(|entry| entry.key == "POSTGRESQL_MONITORING_NAMESPACES")
            .unwrap();
        assert_eq!(namespaces.value, "- customer-db\n- analytics");
    }

    #[test]
    fn agent_configured_env_preserves_yaml_list_values() {
        let values = BTreeMap::from([
            (
                "WORKLOAD_MONITORING_NAMESPACES".to_string(),
                "- customer-app\n- customer-db\n".to_string(),
            ),
            (
                "WORKLOAD_MONITORING_ENABLED".to_string(),
                "true".to_string(),
            ),
        ]);

        let env = agent_configured_env(&values);
        assert_eq!(
            env,
            vec![
                KV {
                    key: "WORKLOAD_MONITORING_ENABLED".into(),
                    value: "true".into(),
                },
                KV {
                    key: "WORKLOAD_MONITORING_NAMESPACES".into(),
                    value: "- customer-app\n- customer-db".into(),
                },
            ]
        );
    }
}
