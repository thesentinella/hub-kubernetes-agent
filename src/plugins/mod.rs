//! Workload and PostgreSQL monitoring plugins.
//!
//! Builds namespace-scoped projections of the cluster-wide inventory for
//! downstream analysis. The workload plugin is opt-in and disabled by default;
//! when enabled with a non-empty namespace allowlist, the snapshot gains a
//! `plugins.workload_monitoring` block that reuses the existing `WorkloadRef`,
//! `PodInfo`, `ServiceInfo`, `IngressInfo`, `EventInfo`, and
//! `DependencyInventory` DTOs filtered by namespace, plus current pod log tails
//! for the same allowlist.
//!
//! The PostgreSQL plugin is also opt-in and disabled by default. It uses the
//! same namespace allowlist pattern and stays discovery-only in v1: no direct
//! database connection, only Kubernetes evidence and a synthesized health
//! status.
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

/// Build the discovery-only PostgreSQL monitoring plugin block from the
/// cluster-wide inventory. Returns `Some(...)` when the plugin is enabled and
/// the namespace allowlist is non-empty; `None` otherwise.
pub fn build_postgresql_monitoring(
    cfg: &Config,
    workloads: &Workloads,
    pods: &[PodInfo],
    network: &NetworkInventory,
    storage: &StorageInventory,
    events: &[EventInfo],
) -> Option<PostgresqlMonitoringPlugin> {
    if !cfg.postgresql_monitoring_enabled {
        return None;
    }
    if cfg.postgresql_monitoring_namespaces.is_empty() {
        return None;
    }

    let allow: Vec<&str> = cfg
        .postgresql_monitoring_namespaces
        .iter()
        .map(|s| s.as_str())
        .collect();

    let filtered_workloads = filter_workloads(&workloads.deployments, &allow)
        .into_iter()
        .chain(filter_workloads(&workloads.statefulsets, &allow))
        .chain(filter_workloads(&workloads.daemonsets, &allow))
        .filter(|workload| matches_postgresql_name(&workload.name))
        .collect::<Vec<_>>();

    let filtered_pods: Vec<PodInfo> = pods
        .iter()
        .filter(|pod| allow.iter().any(|ns| *ns == pod.namespace))
        .filter(|pod| matches_postgresql_pod(pod))
        .cloned()
        .collect();

    let filtered_services: Vec<ServiceInfo> = network
        .services
        .iter()
        .filter(|service| allow.iter().any(|ns| *ns == service.namespace))
        .filter(|service| matches_postgresql_service(service))
        .cloned()
        .collect();

    let filtered_pvcs: Vec<PersistentVolumeClaimInfo> = storage
        .persistent_volume_claims
        .iter()
        .filter(|pvc| allow.iter().any(|ns| *ns == pvc.namespace))
        .filter(|pvc| matches_postgresql_pvc(pvc))
        .cloned()
        .collect();

    let filtered_events: Vec<EventInfo> = events
        .iter()
        .filter(|event| allow.iter().any(|ns| *ns == event.namespace))
        .filter(|event| matches_postgresql_event(event))
        .cloned()
        .collect();

    let ready_pods = filtered_pods
        .iter()
        .filter(|pod| pod.phase.as_deref() == Some("Running"))
        .count();
    let service_ports = filtered_services
        .iter()
        .flat_map(|service| service.ports.iter())
        .filter(|port| port.port == 5432 || port.target_port.as_deref() == Some("5432"))
        .count();

    let status = if filtered_workloads.is_empty()
        && filtered_pods.is_empty()
        && filtered_services.is_empty()
        && filtered_pvcs.is_empty()
    {
        if filtered_events.is_empty() {
            "critical"
        } else {
            "unknown"
        }
    } else if ready_pods > 0
        && (filtered_services.is_empty() || service_ports == 0 || filtered_pvcs.is_empty())
    {
        "warning"
    } else if ready_pods > 0
        && !filtered_services.is_empty()
        && service_ports > 0
        && !filtered_pvcs.is_empty()
    {
        "healthy"
    } else {
        "warning"
    };

    let summary = match status {
        "healthy" => format!(
            "PostgreSQL workload found in {} namespace(s) with {} running pod(s)",
            allow.len(),
            ready_pods
        ),
        "warning" => "PostgreSQL workload found, but one or more signals are incomplete".to_string(),
        "unknown" => "PostgreSQL-related activity was seen, but the workload could not be classified confidently".to_string(),
        _ => "No PostgreSQL workload found in the configured namespaces".to_string(),
    };

    let mut detail = vec![
        PostgresqlMonitoringDetailItem {
            title: "namespaces".into(),
            value: cfg.postgresql_monitoring_namespaces.join(", "),
        },
        PostgresqlMonitoringDetailItem {
            title: "matched workloads".into(),
            value: filtered_workloads.len().to_string(),
        },
        PostgresqlMonitoringDetailItem {
            title: "matched pods".into(),
            value: filtered_pods.len().to_string(),
        },
        PostgresqlMonitoringDetailItem {
            title: "running pods".into(),
            value: ready_pods.to_string(),
        },
        PostgresqlMonitoringDetailItem {
            title: "matched services".into(),
            value: filtered_services.len().to_string(),
        },
        PostgresqlMonitoringDetailItem {
            title: "service ports on 5432".into(),
            value: service_ports.to_string(),
        },
        PostgresqlMonitoringDetailItem {
            title: "matched pvcs".into(),
            value: filtered_pvcs.len().to_string(),
        },
    ];

    let mut evidence = Vec::new();
    evidence.extend(
        filtered_workloads
            .iter()
            .map(|workload| PostgresqlMonitoringEvidenceItem {
                title: "workload".into(),
                value: format!("{}/{}", workload.namespace, workload.name),
            }),
    );
    evidence.extend(filtered_pods.iter().map(|pod| {
        let image = pod
            .containers
            .first()
            .map(|container| container.image.as_str())
            .unwrap_or("unknown");
        PostgresqlMonitoringEvidenceItem {
            title: "pod".into(),
            value: format!(
                "{}/{} phase={} image={}",
                pod.namespace,
                pod.name,
                pod.phase.as_deref().unwrap_or("unknown"),
                image
            ),
        }
    }));
    evidence.extend(filtered_services.iter().map(|service| {
        let ports = service
            .ports
            .iter()
            .map(|port| port.port.to_string())
            .collect::<Vec<_>>()
            .join(",");
        PostgresqlMonitoringEvidenceItem {
            title: "service".into(),
            value: format!("{}/{} ports={}", service.namespace, service.name, ports),
        }
    }));
    evidence.extend(
        filtered_pvcs
            .iter()
            .map(|pvc| PostgresqlMonitoringEvidenceItem {
                title: "pvc".into(),
                value: format!(
                    "{}/{} storage_class={} volume={}",
                    pvc.namespace,
                    pvc.name,
                    pvc.storage_class.as_deref().unwrap_or("unknown"),
                    pvc.volume_name.as_deref().unwrap_or("unknown")
                ),
            }),
    );
    evidence.extend(
        filtered_events
            .iter()
            .map(|event| PostgresqlMonitoringEvidenceItem {
                title: "event".into(),
                value: format!(
                    "{}/{} reason={} message={}",
                    event.namespace,
                    event.name,
                    event.reason.as_deref().unwrap_or("unknown"),
                    event.message.as_deref().unwrap_or("unknown")
                ),
            }),
    );

    let mut missing_data = Vec::new();
    if filtered_workloads.is_empty() {
        missing_data.push(PostgresqlMonitoringMissingDataItem {
            title: "workload discovery".into(),
            value: "No PostgreSQL-like workload name was found in the configured namespaces".into(),
        });
    }
    if filtered_pods.is_empty() {
        missing_data.push(PostgresqlMonitoringMissingDataItem {
            title: "pods".into(),
            value: "No pod matched the PostgreSQL discovery heuristics".into(),
        });
    }
    if filtered_services.is_empty() {
        missing_data.push(PostgresqlMonitoringMissingDataItem {
            title: "service".into(),
            value: "No service matched the PostgreSQL discovery heuristics".into(),
        });
    }
    if filtered_pvcs.is_empty() {
        missing_data.push(PostgresqlMonitoringMissingDataItem {
            title: "persistent volume claims".into(),
            value: "No PVC matched the PostgreSQL discovery heuristics".into(),
        });
    }
    if filtered_events.is_empty() {
        missing_data.push(PostgresqlMonitoringMissingDataItem {
            title: "events".into(),
            value:
                "No PostgreSQL-related warning events were observed in the configured namespaces"
                    .into(),
        });
    }

    if filtered_workloads.is_empty()
        && filtered_pods.is_empty()
        && filtered_services.is_empty()
        && filtered_pvcs.is_empty()
    {
        detail.push(PostgresqlMonitoringDetailItem {
            title: "classification".into(),
            value: status.to_string(),
        });
    }

    Some(PostgresqlMonitoringPlugin {
        enabled: true,
        namespaces: cfg.postgresql_monitoring_namespaces.clone(),
        schema_version: 1,
        generated_at_ms: now_ms(),
        status: status.to_string(),
        summary,
        detail,
        evidence,
        missing_data,
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

fn matches_postgresql_name(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    value.contains("postgres") || value.contains("postgresql") || value.contains("pgsql")
}

fn matches_postgresql_pod(pod: &PodInfo) -> bool {
    matches_postgresql_name(&pod.name)
        || pod
            .owner_name
            .as_deref()
            .map(matches_postgresql_name)
            .unwrap_or(false)
        || pod.containers.iter().any(|container| {
            matches_postgresql_name(&container.image)
                || matches_postgresql_name(container.technology.product.as_deref().unwrap_or(""))
                || matches_postgresql_name(container.technology.subtype.as_deref().unwrap_or(""))
        })
}

fn matches_postgresql_service(service: &ServiceInfo) -> bool {
    matches_postgresql_name(&service.name)
        || service
            .selector
            .iter()
            .any(|kv| matches_postgresql_name(&kv.key) || matches_postgresql_name(&kv.value))
        || service
            .ports
            .iter()
            .any(|port| port.port == 5432 || port.target_port.as_deref() == Some("5432"))
}

fn matches_postgresql_pvc(pvc: &PersistentVolumeClaimInfo) -> bool {
    matches_postgresql_name(&pvc.name)
        || pvc
            .volume_name
            .as_deref()
            .map(matches_postgresql_name)
            .unwrap_or(false)
        || pvc
            .storage_class
            .as_deref()
            .map(matches_postgresql_name)
            .unwrap_or(false)
}

fn matches_postgresql_event(event: &EventInfo) -> bool {
    matches_postgresql_name(&event.name)
        || event
            .reason
            .as_deref()
            .map(matches_postgresql_name)
            .unwrap_or(false)
        || event
            .message
            .as_deref()
            .map(matches_postgresql_name)
            .unwrap_or(false)
        || event
            .involved_object
            .name
            .as_deref()
            .map(matches_postgresql_name)
            .unwrap_or(false)
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
            postgresql_monitoring_enabled: false,
            postgresql_monitoring_namespaces: vec![],
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

    fn postgres_pod(ns: &str, name: &str, phase: &str) -> PodInfo {
        PodInfo {
            namespace: ns.into(),
            name: name.into(),
            age_seconds: Some(60),
            node: Some("node-1".into()),
            phase: Some(phase.into()),
            usage_cpu: None,
            usage_memory: None,
            owner_kind: Some("StatefulSet".into()),
            owner_name: Some(name.into()),
            containers: vec![ContainerInfo {
                name: "postgres".into(),
                image: "postgres:16".into(),
                image_pull_policy: None,
                technology: Technology::default(),
                resources: ResourceSpec::default(),
            }],
        }
    }

    fn postgres_service(ns: &str, name: &str) -> ServiceInfo {
        ServiceInfo {
            namespace: ns.into(),
            name: name.into(),
            type_: "ClusterIP".into(),
            cluster_ip: Some("10.0.0.2".into()),
            external_ips: vec![],
            selector: vec![KV {
                key: "app".into(),
                value: "postgresql".into(),
            }],
            ports: vec![ServicePortInfo {
                name: Some("postgresql".into()),
                protocol: Some("TCP".into()),
                port: 5432,
                target_port: Some("5432".into()),
                node_port: None,
            }],
            load_balancer_ingress: vec![],
        }
    }

    fn postgres_pvc(ns: &str, name: &str) -> PersistentVolumeClaimInfo {
        PersistentVolumeClaimInfo {
            namespace: ns.into(),
            name: name.into(),
            storage_class: Some("standard".into()),
            volume_name: Some(format!("pv-{}", name)),
        }
    }

    fn postgres_event(ns: &str, name: &str) -> EventInfo {
        EventInfo {
            namespace: ns.into(),
            name: name.into(),
            type_: Some("Warning".into()),
            reason: Some("FailedMount".into()),
            message: Some("postgresql pvc mount delayed".into()),
            count: Some(1),
            first_timestamp: None,
            last_timestamp: None,
            reporting_controller: None,
            reporting_instance: None,
            involved_object: InvolvedObjectInfo {
                kind: Some("Pod".into()),
                name: Some(name.into()),
                namespace: Some(ns.into()),
                uid: None,
            },
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

    #[test]
    fn build_postgresql_returns_none_when_disabled() {
        let cfg = base_config(false, vec!["customer-app".into()]);
        let result = build_postgresql_monitoring(
            &cfg,
            &Workloads::default(),
            &[],
            &NetworkInventory::default(),
            &StorageInventory::default(),
            &[],
        );
        assert!(result.is_none());
    }

    #[test]
    fn build_postgresql_is_healthy_with_complete_evidence() {
        let mut cfg = base_config(false, vec![]);
        cfg.postgresql_monitoring_enabled = true;
        cfg.postgresql_monitoring_namespaces = vec!["customer-db".into()];
        let workloads = Workloads {
            deployments: vec![],
            statefulsets: vec![WorkloadRef {
                namespace: "customer-db".into(),
                name: "postgresql".into(),
                replicas_desired: Some(1),
                replicas_ready: Some(1),
            }],
            daemonsets: vec![],
        };
        let pods = vec![postgres_pod("customer-db", "postgresql-0", "Running")];
        let network = NetworkInventory {
            services: vec![postgres_service("customer-db", "postgresql")],
            ingresses: vec![],
        };
        let storage = StorageInventory {
            storage_classes: vec![],
            persistent_volumes: vec![],
            persistent_volume_claims: vec![postgres_pvc("customer-db", "data-postgresql-0")],
            volume_snapshot_classes: vec![],
            volume_snapshots: vec![],
        };
        let events = vec![postgres_event("customer-db", "postgresql-0")];

        let result =
            build_postgresql_monitoring(&cfg, &workloads, &pods, &network, &storage, &events)
                .expect("plugin should be present");

        assert_eq!(result.status, "healthy");
        assert_eq!(result.namespaces, vec!["customer-db"]);
        assert!(result.summary.contains("running pod"));
        assert!(result.evidence.iter().any(|item| item.title == "service"));
        assert!(
            result.missing_data.is_empty()
                || result
                    .missing_data
                    .iter()
                    .all(|item| item.title != "service")
        );
    }

    #[test]
    fn build_postgresql_reports_warning_when_signals_are_incomplete() {
        let mut cfg = base_config(false, vec![]);
        cfg.postgresql_monitoring_enabled = true;
        cfg.postgresql_monitoring_namespaces = vec!["customer-db".into()];
        let workloads = Workloads {
            deployments: vec![],
            statefulsets: vec![WorkloadRef {
                namespace: "customer-db".into(),
                name: "postgresql".into(),
                replicas_desired: Some(1),
                replicas_ready: Some(1),
            }],
            daemonsets: vec![],
        };
        let pods = vec![postgres_pod("customer-db", "postgresql-0", "Pending")];

        let result = build_postgresql_monitoring(
            &cfg,
            &workloads,
            &pods,
            &NetworkInventory::default(),
            &StorageInventory::default(),
            &[],
        )
        .expect("plugin should be present");

        assert_eq!(result.status, "warning");
        assert!(
            result
                .missing_data
                .iter()
                .any(|item| item.title == "service")
        );
        assert!(
            result
                .missing_data
                .iter()
                .any(|item| item.title == "persistent volume claims")
        );
    }

    #[test]
    fn build_postgresql_reports_critical_when_no_evidence_matches() {
        let mut cfg = base_config(false, vec![]);
        cfg.postgresql_monitoring_enabled = true;
        cfg.postgresql_monitoring_namespaces = vec!["customer-db".into()];

        let result = build_postgresql_monitoring(
            &cfg,
            &Workloads::default(),
            &[],
            &NetworkInventory::default(),
            &StorageInventory::default(),
            &[],
        )
        .expect("plugin should be present");

        assert_eq!(result.status, "critical");
        assert!(result.summary.contains("No PostgreSQL workload"));
        assert!(!result.detail.is_empty());
    }
}
