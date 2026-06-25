//! Workload monitoring plugin.
//!
//! Builds a namespace-scoped projection of the cluster-wide inventory for
//! downstream analysis. The plugin is opt-in and disabled by default; when
//! enabled with a non-empty namespace allowlist, the snapshot gains a
//! `plugins.workload_monitoring` block that reuses the existing
//! `WorkloadRef`, `PodInfo`, `ServiceInfo`, `IngressInfo`, `EventInfo`, and
//! `DependencyInventory` DTOs filtered by namespace, plus current pod log
//! tails for the same allowlist.
//!
//! See `docs/adr/0001-workload-monitoring-plugin.md` for the full contract.

use crate::config::Config;
use crate::model::*;

use std::time::{SystemTime, UNIX_EPOCH};

/// Build the workload monitoring plugin block from the cluster-wide inventory.
/// Returns `Some(...)` when the plugin is enabled and the namespace allowlist
/// is non-empty; `None` otherwise. The result is `None` (omitted from JSON)
/// when the plugin is disabled — this matches the ADR decision that the
/// `plugins` field is omitted entirely when the feature is off.
pub fn build_workload_monitoring(
    cfg: &Config,
    workloads: &Workloads,
    pods: &[PodInfo],
    network: &NetworkInventory,
    events: &[EventInfo],
    dependencies: &DependencyInventory,
    logs: WorkloadMonitoringLogs,
) -> Option<WorkloadMonitoringPlugin> {
    if !cfg.workload_monitoring_enabled {
        return None;
    }
    if cfg.workload_monitoring_namespaces.is_empty() {
        return None;
    }

    let allow: Vec<&str> = cfg
        .workload_monitoring_namespaces
        .iter()
        .map(|s| s.as_str())
        .collect();

    let filtered_workloads = Workloads {
        deployments: filter_workloads(&workloads.deployments, &allow),
        statefulsets: filter_workloads(&workloads.statefulsets, &allow),
        daemonsets: filter_workloads(&workloads.daemonsets, &allow),
    };

    let filtered_pods: Vec<PodInfo> = pods
        .iter()
        .filter(|p| allow.iter().any(|ns| *ns == p.namespace))
        .cloned()
        .collect();

    let filtered_services: Vec<ServiceInfo> = network
        .services
        .iter()
        .filter(|s| allow.iter().any(|ns| *ns == s.namespace))
        .cloned()
        .collect();

    let filtered_ingresses: Vec<IngressInfo> = network
        .ingresses
        .iter()
        .filter(|i| allow.iter().any(|ns| *ns == i.namespace))
        .cloned()
        .collect();

    let filtered_events: Vec<EventInfo> = events
        .iter()
        .filter(|e| allow.iter().any(|ns| *ns == e.namespace))
        .cloned()
        .collect();

    let filtered_dependencies = if cfg.collect_dependencies_tetragon {
        Some(DependencyInventory {
            edges: dependencies
                .edges
                .iter()
                .filter(|edge| {
                    edge.from
                        .namespace
                        .as_deref()
                        .map(|ns| allow.contains(&ns))
                        .unwrap_or(false)
                        || edge
                            .to
                            .namespace
                            .as_deref()
                            .map(|ns| allow.contains(&ns))
                            .unwrap_or(false)
                })
                .cloned()
                .collect(),
            source: dependencies.source,
            window_seconds: dependencies.window_seconds,
            truncated: dependencies.truncated,
            dropped_edges: dependencies.dropped_edges,
        })
    } else {
        None
    };

    Some(WorkloadMonitoringPlugin {
        enabled: true,
        namespaces: cfg.workload_monitoring_namespaces.clone(),
        technology_targets: cfg.workload_monitoring_targets.clone(),
        schema_version: 1,
        generated_at_ms: now_ms(),
        signals: WorkloadMonitoringSignals {
            workloads: merge_workload_refs(&filtered_workloads),
            pods: filtered_pods,
            services: filtered_services,
            ingresses: filtered_ingresses,
            events: filtered_events,
            dependencies: filtered_dependencies,
        },
        logs,
    })
}

/// Filter a workload-ref slice by namespace allowlist. Stable sort is
/// preserved by the input (collector's snapshot path already sorts).
fn filter_workloads(items: &[WorkloadRef], allow: &[&str]) -> Vec<WorkloadRef> {
    items
        .iter()
        .filter(|w| allow.iter().any(|ns| *ns == w.namespace))
        .cloned()
        .collect()
}

/// Flatten the three workload kinds (deployments, statefulsets, daemonsets)
/// into a single list. The plugin signals do not preserve the kind
/// distinction on the wire; if a kind tag is needed later, it can be added
/// as a wrapper struct.
fn merge_workload_refs(w: &Workloads) -> Vec<WorkloadRef> {
    let mut out =
        Vec::with_capacity(w.deployments.len() + w.statefulsets.len() + w.daemonsets.len());
    out.extend(w.deployments.iter().cloned());
    out.extend(w.statefulsets.iter().cloned());
    out.extend(w.daemonsets.iter().cloned());
    out
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn base_config(enabled: bool, namespaces: Vec<String>) -> Config {
        Config {
            hub_url: "https://api.hub.sentinel.la".into(),
            cluster_id: "cluster-dev".into(),
            api_key: None,
            agent_version_override: None,
            collect_interval: Duration::from_secs(60),
            poll_wait: Duration::from_secs(30),
            actions_enabled: false,
            collect_secrets: false,
            collect_dependencies_tetragon: false,
            tetragon_required_for_readiness: true,
            tetragon_grpc_address: "tetragon:54321".into(),
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
            workload_monitoring_enabled: enabled,
            workload_monitoring_namespaces: namespaces,
            workload_monitoring_targets: vec!["angular".into(), "spring_boot".into()],
        }
    }

    fn workload(ns: &str, name: &str) -> WorkloadRef {
        WorkloadRef {
            namespace: ns.into(),
            name: name.into(),
            replicas_desired: Some(1),
            replicas_ready: Some(1),
        }
    }

    fn pod(ns: &str, name: &str) -> PodInfo {
        PodInfo {
            namespace: ns.into(),
            name: name.into(),
            age_seconds: Some(60),
            node: Some("node-1".into()),
            phase: Some("Running".into()),
            usage_cpu: None,
            usage_memory: None,
            owner_kind: Some("Deployment".into()),
            owner_name: Some(name.into()),
            containers: vec![],
        }
    }

    #[test]
    fn build_returns_none_when_disabled() {
        let cfg = base_config(false, vec!["customer-app".into()]);
        let result = build_workload_monitoring(
            &cfg,
            &Workloads::default(),
            &[],
            &NetworkInventory::default(),
            &[],
            &DependencyInventory::default(),
            WorkloadMonitoringLogs::default(),
        );
        assert!(result.is_none());
    }

    #[test]
    fn build_returns_none_when_namespace_allowlist_empty() {
        let cfg = base_config(true, vec![]);
        let result = build_workload_monitoring(
            &cfg,
            &Workloads::default(),
            &[],
            &NetworkInventory::default(),
            &[],
            &DependencyInventory::default(),
            WorkloadMonitoringLogs::default(),
        );
        assert!(result.is_none());
    }

    #[test]
    fn build_filters_workloads_pods_events_by_namespace() {
        let cfg = base_config(true, vec!["customer-app".into()]);
        let workloads = Workloads {
            deployments: vec![workload("customer-app", "api"), workload("default", "kube")],
            statefulsets: vec![workload("customer-app", "db")],
            daemonsets: vec![],
        };
        let pods = vec![pod("customer-app", "api-1"), pod("kube-system", "coredns")];
        let events = vec![];
        let result = build_workload_monitoring(
            &cfg,
            &workloads,
            &pods,
            &NetworkInventory::default(),
            &events,
            &DependencyInventory::default(),
            WorkloadMonitoringLogs::default(),
        )
        .unwrap();
        assert_eq!(result.namespaces, vec!["customer-app"]);
        assert_eq!(result.signals.workloads.len(), 2);
        assert_eq!(result.signals.pods.len(), 1);
        assert_eq!(result.signals.pods[0].name, "api-1");
    }

    #[test]
    fn build_preserves_workload_monitoring_logs() {
        let cfg = base_config(true, vec!["customer-app".into()]);
        let logs = WorkloadMonitoringLogs {
            pods: vec![WorkloadMonitoringPodLogs {
                namespace: "customer-app".into(),
                name: "api-1".into(),
                containers: vec![WorkloadMonitoringContainerLogs {
                    name: "api".into(),
                    truncated: false,
                    lines: vec!["ready".into()],
                }],
            }],
        };

        let result = build_workload_monitoring(
            &cfg,
            &Workloads::default(),
            &[],
            &NetworkInventory::default(),
            &[],
            &DependencyInventory::default(),
            logs,
        )
        .unwrap();

        assert_eq!(result.logs.pods.len(), 1);
        assert_eq!(result.logs.pods[0].containers[0].lines, vec!["ready"]);
    }

    #[test]
    fn build_omits_dependencies_when_tetragon_disabled() {
        let cfg = base_config(true, vec!["customer-app".into()]);
        let result = build_workload_monitoring(
            &cfg,
            &Workloads::default(),
            &[],
            &NetworkInventory::default(),
            &[],
            &DependencyInventory::default(),
            WorkloadMonitoringLogs::default(),
        )
        .unwrap();
        assert!(result.signals.dependencies.is_none());
    }

    #[test]
    fn build_includes_dependencies_when_tetragon_enabled() {
        let mut cfg = base_config(true, vec!["customer-app".into()]);
        cfg.collect_dependencies_tetragon = true;
        let deps = DependencyInventory {
            edges: vec![DependencyEdge {
                from: DependencyEndpoint {
                    kind: "pod".into(),
                    namespace: Some("customer-app".into()),
                    name: Some("api".into()),
                    workload_kind: Some("Deployment".into()),
                    workload_name: Some("api".into()),
                    ip: Some("10.0.0.1".into()),
                },
                to: DependencyEndpoint {
                    kind: "service".into(),
                    namespace: Some("customer-app".into()),
                    name: Some("db".into()),
                    workload_kind: None,
                    workload_name: None,
                    ip: Some("10.0.0.2".into()),
                },
                protocol: "TCP".into(),
                destination_port: 5432,
                direction: "egress".into(),
                bytes: 1024,
                packets: 8,
                connections: 1,
                first_seen_unix_ms: 0,
                last_seen_unix_ms: 0,
            }],
            source: "tetragon_grpc",
            window_seconds: 60,
            truncated: false,
            dropped_edges: 0,
        };
        let result = build_workload_monitoring(
            &cfg,
            &Workloads::default(),
            &[],
            &NetworkInventory::default(),
            &[],
            &deps,
            WorkloadMonitoringLogs::default(),
        )
        .unwrap();
        let signals_deps = result.signals.dependencies.unwrap();
        assert_eq!(signals_deps.edges.len(), 1);
    }
}
