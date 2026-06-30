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

use crate::collector;
use crate::config::Config;
use crate::model::*;

use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;
use tokio_postgres::NoTls;

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
    app_metrics: AppMetricsInventory,
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
            observed_endpoints: dependencies.observed_endpoints,
            connected_endpoints: dependencies.connected_endpoints,
            unavailable_endpoints: dependencies.unavailable_endpoints,
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
            app_metrics: if cfg.app_metrics_enabled {
                Some(app_metrics)
            } else {
                None
            },
        },
        logs,
    })
}

/// Build the discovery-only PostgreSQL monitoring plugin block from the
/// cluster-wide inventory. Returns `Some(...)` when the plugin is enabled and
/// the namespace allowlist is non-empty; `None` otherwise.
pub async fn build_postgresql_monitoring(
    client: Option<&Client>,
    cfg: &Config,
    workloads: &Workloads,
    pods: &[PodInfo],
    network: &NetworkInventory,
    storage: &StorageInventory,
    events: &[EventInfo],
) -> Option<PostgresqlMonitoringPlugin> {
    let probe_client = client.cloned();
    let probe_cfg = cfg.clone();
    let closure_client = probe_client.clone();
    let closure_cfg = probe_cfg.clone();
    build_postgresql_monitoring_with_probe(
        &probe_cfg,
        workloads,
        pods,
        network,
        storage,
        events,
        move |service| {
            let client = closure_client.clone();
            let cfg = closure_cfg.clone();
            Box::pin(async move { probe_postgresql_service(client.as_ref(), &cfg, service).await })
        },
    )
    .await
}

pub async fn diagnose_postgresql(
    client: &Client,
    cfg: &Config,
    spec: &PostgresqlDiagnosticSpec,
) -> PostgresqlDiagnosticReport {
    let namespace = spec.namespace.trim().to_string();
    if namespace.is_empty() {
        return diagnostic_report_error(
            namespace,
            "namespace is required".to_string(),
            Vec::new(),
            Vec::new(),
        );
    }

    let collected = match collector::collect(client, cfg).await {
        Ok(snapshot) => snapshot,
        Err(err) => {
            return diagnostic_report_error(
                namespace,
                format!("failed to collect cluster inventory: {err}"),
                Vec::new(),
                Vec::new(),
            );
        }
    };

    let (
        _k8s_uid,
        _cluster,
        _namespaces,
        workloads,
        pods,
        network,
        _security,
        _operational_maturity,
        _dependencies,
        _app_metrics,
        _configuration,
        storage,
        events,
        _metrics,
        _snapshot_api,
    ) = collected;

    let scope =
        scope_postgresql_diagnostic(&namespace, spec, workloads, pods, network, storage, events);

    let mut probe_cfg = cfg.clone();
    probe_cfg.postgresql_monitoring_enabled = true;
    probe_cfg.postgresql_monitoring_namespaces = vec![namespace.clone()];
    if let Some(secret_name) = spec
        .secret_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        probe_cfg.postgresql_monitoring_secret_name = Some(secret_name.to_string());
    }

    let plugin = build_postgresql_monitoring(
        Some(client),
        &probe_cfg,
        &scope.workloads,
        &scope.pods,
        &scope.network,
        &scope.storage,
        &scope.events,
    )
    .await;

    match plugin {
        Some(plugin) => diagnostic_report_from_plugin(
            namespace,
            plugin,
            scope.hint_missing_data,
            scope.hint_findings,
        ),
        None => diagnostic_report_unknown(
            namespace,
            "postgresql diagnosis unavailable".to_string(),
            scope.hint_missing_data,
            scope.hint_findings,
        ),
    }
}

type ProbeFuture<'a> = Pin<Box<dyn Future<Output = PostgresqlProbeResult> + Send + 'a>>;

struct PostgresqlDiagnosticScope {
    workloads: Workloads,
    pods: Vec<PodInfo>,
    network: NetworkInventory,
    storage: StorageInventory,
    events: Vec<EventInfo>,
    hint_missing_data: Vec<PostgresqlMonitoringMissingDataItem>,
    hint_findings: Vec<PostgresqlDiagnosticFinding>,
}

fn scope_postgresql_diagnostic(
    namespace: &str,
    spec: &PostgresqlDiagnosticSpec,
    workloads: Workloads,
    pods: Vec<PodInfo>,
    network: NetworkInventory,
    storage: StorageInventory,
    events: Vec<EventInfo>,
) -> PostgresqlDiagnosticScope {
    let service_name = spec
        .service_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let pod_selector = spec
        .pod_selector
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let workloads = Workloads {
        deployments: workloads
            .deployments
            .into_iter()
            .filter(|workload| workload.namespace == namespace)
            .filter(|workload| {
                pod_selector
                    .map(|hint| postgres_hint_matches(&workload.name, hint))
                    .unwrap_or(true)
            })
            .collect(),
        statefulsets: workloads
            .statefulsets
            .into_iter()
            .filter(|workload| workload.namespace == namespace)
            .filter(|workload| {
                pod_selector
                    .map(|hint| postgres_hint_matches(&workload.name, hint))
                    .unwrap_or(true)
            })
            .collect(),
        daemonsets: workloads
            .daemonsets
            .into_iter()
            .filter(|workload| workload.namespace == namespace)
            .filter(|workload| {
                pod_selector
                    .map(|hint| postgres_hint_matches(&workload.name, hint))
                    .unwrap_or(true)
            })
            .collect(),
    };

    let pods = pods
        .into_iter()
        .filter(|pod| pod.namespace == namespace)
        .filter(|pod| {
            pod_selector
                .map(|hint| postgres_pod_matches(pod, hint))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();

    let mut services = network
        .services
        .into_iter()
        .filter(|service| service.namespace == namespace)
        .collect::<Vec<_>>();
    if let Some(service_name) = service_name {
        let matched = services
            .iter()
            .filter(|service| service.name == service_name)
            .cloned()
            .collect::<Vec<_>>();
        if matched.is_empty() {
            let hint_missing_data = vec![PostgresqlMonitoringMissingDataItem {
                title: "service hint".to_string(),
                value: format!(
                    "no Service named `{}` matched namespace `{}`",
                    service_name, namespace
                ),
            }];
            let hint_findings = vec![PostgresqlDiagnosticFinding {
                title: "Service hint not resolved".to_string(),
                severity: "unknown".to_string(),
                description: format!(
                    "no Service named `{}` matched namespace `{}`",
                    service_name, namespace
                ),
                evidence: "service hint did not match any namespace-local Service".to_string(),
                recommendation:
                    "double-check the service_name hint or omit it to use heuristic discovery"
                        .to_string(),
            }];
            return PostgresqlDiagnosticScope {
                workloads,
                pods,
                network: NetworkInventory {
                    services: Vec::new(),
                    ingresses: network
                        .ingresses
                        .into_iter()
                        .filter(|ingress| ingress.namespace == namespace)
                        .collect(),
                },
                storage: StorageInventory {
                    storage_classes: storage.storage_classes,
                    persistent_volumes: storage.persistent_volumes,
                    persistent_volume_claims: storage
                        .persistent_volume_claims
                        .into_iter()
                        .filter(|pvc| pvc.namespace == namespace)
                        .collect(),
                    volume_snapshot_classes: storage.volume_snapshot_classes,
                    volume_snapshots: storage.volume_snapshots,
                },
                events: events
                    .into_iter()
                    .filter(|event| event.namespace == namespace)
                    .filter(|event| {
                        pod_selector
                            .map(|hint| postgres_event_matches_hint(event, hint))
                            .unwrap_or(true)
                    })
                    .collect(),
                hint_missing_data,
                hint_findings,
            };
        }
        services = matched;
    }

    let hint_missing_data = if pod_selector.is_some() && pods.is_empty() {
        vec![PostgresqlMonitoringMissingDataItem {
            title: "pod selector hint".to_string(),
            value: format!(
                "no Pod or workload name matched `{}` in namespace `{}`",
                pod_selector.unwrap_or_default(),
                namespace
            ),
        }]
    } else {
        Vec::new()
    };

    let mut hint_findings = Vec::new();
    if let Some(item) = hint_missing_data.first() {
        hint_findings.push(PostgresqlDiagnosticFinding {
            title: "Discovery hint not resolved".to_string(),
            severity: "unknown".to_string(),
            description: item.value.clone(),
            evidence: "discovery hint filtered out all candidate pods/workloads".to_string(),
            recommendation:
                "double-check the pod_selector hint or omit it to use heuristic discovery"
                    .to_string(),
        });
    }

    PostgresqlDiagnosticScope {
        workloads,
        pods,
        network: NetworkInventory {
            services,
            ingresses: network
                .ingresses
                .into_iter()
                .filter(|ingress| ingress.namespace == namespace)
                .collect(),
        },
        storage: StorageInventory {
            storage_classes: storage.storage_classes,
            persistent_volumes: storage.persistent_volumes,
            persistent_volume_claims: storage
                .persistent_volume_claims
                .into_iter()
                .filter(|pvc| pvc.namespace == namespace)
                .collect(),
            volume_snapshot_classes: storage.volume_snapshot_classes,
            volume_snapshots: storage.volume_snapshots,
        },
        events: events
            .into_iter()
            .filter(|event| event.namespace == namespace)
            .filter(|event| {
                pod_selector
                    .map(|hint| postgres_event_matches_hint(event, hint))
                    .unwrap_or(true)
            })
            .collect(),
        hint_missing_data,
        hint_findings,
    }
}

fn postgres_hint_matches(value: &str, hint: &str) -> bool {
    value.to_lowercase().contains(&hint.to_lowercase())
}

fn postgres_pod_matches(pod: &PodInfo, hint: &str) -> bool {
    postgres_hint_matches(&pod.name, hint)
        || pod
            .owner_name
            .as_deref()
            .map(|value| postgres_hint_matches(value, hint))
            .unwrap_or(false)
        || pod
            .owner_kind
            .as_deref()
            .map(|value| postgres_hint_matches(value, hint))
            .unwrap_or(false)
        || pod.containers.iter().any(|container| {
            postgres_hint_matches(&container.name, hint)
                || postgres_hint_matches(&container.image, hint)
        })
}

fn postgres_event_matches_hint(event: &EventInfo, hint: &str) -> bool {
    postgres_hint_matches(&event.name, hint)
        || event
            .reason
            .as_deref()
            .map(|value| postgres_hint_matches(value, hint))
            .unwrap_or(false)
        || event
            .message
            .as_deref()
            .map(|value| postgres_hint_matches(value, hint))
            .unwrap_or(false)
        || event
            .involved_object
            .name
            .as_deref()
            .map(|value| postgres_hint_matches(value, hint))
            .unwrap_or(false)
}

fn diagnostic_report_error(
    namespace: String,
    message: String,
    hint_missing_data: Vec<PostgresqlMonitoringMissingDataItem>,
    hint_findings: Vec<PostgresqlDiagnosticFinding>,
) -> PostgresqlDiagnosticReport {
    let finding = PostgresqlDiagnosticFinding {
        title: "PostgreSQL diagnosis failed".to_string(),
        severity: "unknown".to_string(),
        description: message.clone(),
        evidence: message.clone(),
        recommendation: "retry after the underlying collection or configuration issue is resolved"
            .to_string(),
    };
    PostgresqlDiagnosticReport {
        namespace,
        status: "unknown".to_string(),
        summary: message,
        findings: hint_findings.into_iter().chain([finding]).collect(),
        evidence: Vec::new(),
        missing_data: hint_missing_data,
        recommended_actions: vec![
            "retry after resolving the underlying collection or configuration issue".to_string(),
        ],
    }
}

fn diagnostic_report_unknown(
    namespace: String,
    summary: String,
    hint_missing_data: Vec<PostgresqlMonitoringMissingDataItem>,
    hint_findings: Vec<PostgresqlDiagnosticFinding>,
) -> PostgresqlDiagnosticReport {
    let mut findings = hint_findings;
    findings.push(PostgresqlDiagnosticFinding {
        title: "PostgreSQL diagnosis".to_string(),
        severity: "unknown".to_string(),
        description: summary.clone(),
        evidence: summary.clone(),
        recommendation: "review the missing data and discovery hints, then retry".to_string(),
    });
    PostgresqlDiagnosticReport {
        namespace,
        status: "unknown".to_string(),
        summary,
        findings,
        evidence: Vec::new(),
        missing_data: hint_missing_data,
        recommended_actions: vec![
            "review the missing data and discovery hints, then retry".to_string(),
        ],
    }
}

fn diagnostic_report_from_plugin(
    namespace: String,
    plugin: PostgresqlMonitoringPlugin,
    hint_missing_data: Vec<PostgresqlMonitoringMissingDataItem>,
    hint_findings: Vec<PostgresqlDiagnosticFinding>,
) -> PostgresqlDiagnosticReport {
    let mut status = plugin.status.clone();
    let mut summary = plugin.summary.clone();
    let mut missing_data = plugin.missing_data.clone();
    missing_data.extend(hint_missing_data);

    if !missing_data.is_empty() {
        status = "unknown".to_string();
        summary = format!(
            "{} Discovery hints or missing data prevented a fully resolved diagnosis.",
            summary
        );
    }

    let mut findings = Vec::new();
    findings.push(PostgresqlDiagnosticFinding {
        title: "PostgreSQL diagnosis".to_string(),
        severity: diagnostic_severity(&status).to_string(),
        description: summary.clone(),
        evidence: plugin
            .evidence
            .iter()
            .map(|item| format!("{}: {}", item.title, item.value))
            .collect::<Vec<_>>()
            .join("; "),
        recommendation: diagnostic_recommendation(&status).to_string(),
    });

    findings.extend(plugin.detail.iter().map(|item| {
        PostgresqlDiagnosticFinding {
            title: item.title.clone(),
            severity: diagnostic_severity(&status).to_string(),
            description: item.value.clone(),
            evidence: plugin
                .evidence
                .iter()
                .map(|e| format!("{}: {}", e.title, e.value))
                .collect::<Vec<_>>()
                .join("; "),
            recommendation: diagnostic_recommendation(&status).to_string(),
        }
    }));

    findings.extend(hint_findings);

    let recommended_actions = diagnostic_recommendations(&status, &findings);

    PostgresqlDiagnosticReport {
        namespace,
        status,
        summary,
        findings,
        evidence: plugin.evidence,
        missing_data,
        recommended_actions,
    }
}

fn diagnostic_severity(status: &str) -> &'static str {
    match status {
        "healthy" => "info",
        "warning" => "warning",
        "critical" => "critical",
        _ => "unknown",
    }
}

fn diagnostic_recommendation(status: &str) -> &'static str {
    match status {
        "healthy" => "continue monitoring and re-run if symptoms change",
        "warning" => "review the warnings and missing data, then re-run the diagnosis",
        "critical" => "treat this as an active incident and inspect the identified evidence",
        _ => "resolve the missing data or discovery issues, then re-run the diagnosis",
    }
}

fn diagnostic_recommendations(
    status: &str,
    findings: &[PostgresqlDiagnosticFinding],
) -> Vec<String> {
    let mut recommendations = vec![diagnostic_recommendation(status).to_string()];
    if findings.iter().any(|finding| finding.severity == "unknown") {
        recommendations.push("double-check discovery hints and permissions".to_string());
    }
    recommendations.sort();
    recommendations.dedup();
    recommendations
}

async fn build_postgresql_monitoring_with_probe<F>(
    cfg: &Config,
    workloads: &Workloads,
    pods: &[PodInfo],
    network: &NetworkInventory,
    storage: &StorageInventory,
    events: &[EventInfo],
    probe_fn: F,
) -> Option<PostgresqlMonitoringPlugin>
where
    F: for<'a> Fn(&'a ServiceInfo) -> ProbeFuture<'a>,
{
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

    let filtered_deployments = filter_workloads(&workloads.deployments, &allow)
        .into_iter()
        .filter(|workload| matches_postgresql_name(&workload.name))
        .collect::<Vec<_>>();
    let filtered_statefulsets = filter_workloads(&workloads.statefulsets, &allow)
        .into_iter()
        .filter(|workload| matches_postgresql_name(&workload.name))
        .collect::<Vec<_>>();
    let filtered_daemonsets = filter_workloads(&workloads.daemonsets, &allow)
        .into_iter()
        .filter(|workload| matches_postgresql_name(&workload.name))
        .collect::<Vec<_>>();
    let filtered_workloads = filtered_deployments
        .iter()
        .chain(filtered_statefulsets.iter())
        .chain(filtered_daemonsets.iter())
        .cloned()
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
    let mut missing_data = Vec::new();
    let clear_candidates = filtered_services
        .iter()
        .filter(|service| is_clear_postgresql_service(service))
        .collect::<Vec<_>>();
    let weak_candidates = filtered_services
        .iter()
        .filter(|service| {
            matches_postgresql_service(service) && !is_clear_postgresql_service(service)
        })
        .collect::<Vec<_>>();
    let mut probe_note = None;
    let probe_result = match clear_candidates.as_slice() {
        [service] => Some(probe_fn(service).await),
        [] if !filtered_services.is_empty() => {
            probe_note = Some(
                "Service did not clearly match PostgreSQL heuristics; live probe skipped"
                    .to_string(),
            );
            None
        }
        _ if clear_candidates.len() > 1 => {
            probe_note = Some(
                "Multiple services matched PostgreSQL heuristics; live probe skipped".to_string(),
            );
            None
        }
        _ => None,
    };

    let mut status = if clear_candidates.len() > 1 {
        "unknown"
    } else if clear_candidates.len() == 1 {
        if ready_pods > 0 && service_ports > 0 && !filtered_pvcs.is_empty() {
            if matches!(probe_result, Some(PostgresqlProbeResult::Connected { .. })) {
                "healthy"
            } else {
                "warning"
            }
        } else {
            "warning"
        }
    } else if !filtered_services.is_empty()
        || !filtered_workloads.is_empty()
        || !filtered_pods.is_empty()
        || !filtered_pvcs.is_empty()
    {
        "warning"
    } else if filtered_events.is_empty() {
        "critical"
    } else {
        "unknown"
    };

    if matches!(probe_result, Some(PostgresqlProbeResult::Failed { .. })) && status == "healthy" {
        status = "warning";
    }

    let summary = if let Some(result) = &probe_result {
        match result {
            PostgresqlProbeResult::Connected { latency_ms, .. } if status == "healthy" => format!(
                "PostgreSQL workload found in {} namespace(s) with {} running pod(s); live SELECT 1 succeeded in {} ms",
                allow.len(),
                ready_pods,
                latency_ms
            ),
            PostgresqlProbeResult::Connected { latency_ms, .. } => format!(
                "PostgreSQL workload found, live SELECT 1 succeeded in {} ms, but one or more signals are incomplete",
                latency_ms
            ),
            PostgresqlProbeResult::Failed { classification, .. } => format!(
                "PostgreSQL workload found, but live SELECT 1 {}",
                classification
            ),
        }
    } else {
        match status {
            "healthy" => format!(
                "PostgreSQL workload found in {} namespace(s) with {} running pod(s)",
                allow.len(),
                ready_pods
            ),
            "warning" => {
                "PostgreSQL workload found, but one or more signals are incomplete".to_string()
            }
            "unknown" => {
                "PostgreSQL-related activity was seen, but the workload could not be classified confidently".to_string()
            }
            _ => "No PostgreSQL workload found in the configured namespaces".to_string(),
        }
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
            title: "clear services".into(),
            value: clear_candidates.len().to_string(),
        },
        PostgresqlMonitoringDetailItem {
            title: "weak services".into(),
            value: weak_candidates.len().to_string(),
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
        filtered_deployments
            .iter()
            .map(|workload| PostgresqlMonitoringEvidenceItem {
                title: "workload_name".into(),
                value: format!(
                    "deployment {}/{} match=name",
                    workload.namespace, workload.name
                ),
            }),
    );
    evidence.extend(filtered_statefulsets.iter().map(|workload| {
        PostgresqlMonitoringEvidenceItem {
            title: "workload_owner".into(),
            value: format!(
                "statefulset {}/{} match=name",
                workload.namespace, workload.name
            ),
        }
    }));
    evidence.extend(
        filtered_daemonsets
            .iter()
            .map(|workload| PostgresqlMonitoringEvidenceItem {
                title: "workload_name".into(),
                value: format!(
                    "daemonset {}/{} match=name",
                    workload.namespace, workload.name
                ),
            }),
    );
    evidence.extend(filtered_pods.iter().map(|pod| {
        let image = pod
            .containers
            .first()
            .map(|container| container.image.as_str())
            .unwrap_or("unknown");
        let owner = match (&pod.owner_kind, &pod.owner_name) {
            (Some(kind), Some(name)) => format!("owner={} name={}", kind, name),
            (Some(kind), None) => format!("owner={}", kind),
            (None, Some(name)) => format!("name={}", name),
            _ => "owner=unknown".to_string(),
        };
        PostgresqlMonitoringEvidenceItem {
            title: "pod".into(),
            value: format!(
                "{}/{} {} phase={} image={}",
                pod.namespace,
                pod.name,
                owner,
                pod.phase.as_deref().unwrap_or("unknown"),
                image
            ),
        }
    }));
    evidence.extend(clear_candidates.iter().map(|service| {
        let ports = service
            .ports
            .iter()
            .map(|port| port.port.to_string())
            .collect::<Vec<_>>()
            .join(",");
        PostgresqlMonitoringEvidenceItem {
            title: "service".into(),
            value: format!(
                "clear {}/{} ports={} selector={}",
                service.namespace,
                service.name,
                ports,
                service.selector.len()
            ),
        }
    }));
    evidence.extend(weak_candidates.iter().map(|service| {
        let ports = service
            .ports
            .iter()
            .map(|port| port.port.to_string())
            .collect::<Vec<_>>()
            .join(",");
        PostgresqlMonitoringEvidenceItem {
            title: "service".into(),
            value: format!(
                "weak {}/{} ports={} selector={}",
                service.namespace,
                service.name,
                ports,
                service.selector.len()
            ),
        }
    }));
    evidence.extend(
        filtered_pvcs
            .iter()
            .map(|pvc| PostgresqlMonitoringEvidenceItem {
                title: "pvc".into(),
                value: format!(
                    "{}/{} match={} storage_class={} volume={}",
                    pvc.namespace,
                    pvc.name,
                    pvc_match_reason(pvc),
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

    if let Some(note) = probe_note {
        detail.push(PostgresqlMonitoringDetailItem {
            title: "live probe".into(),
            value: note.clone(),
        });
        missing_data.push(PostgresqlMonitoringMissingDataItem {
            title: "live probe".into(),
            value: note,
        });
    }

    if let Some(result) = &probe_result {
        match result {
            PostgresqlProbeResult::Connected {
                host,
                port,
                latency_ms,
            } => {
                detail.push(PostgresqlMonitoringDetailItem {
                    title: "live probe".into(),
                    value: format!(
                        "SELECT 1 succeeded on {}:{} in {} ms",
                        host, port, latency_ms
                    ),
                });
                evidence.push(PostgresqlMonitoringEvidenceItem {
                    title: "probe".into(),
                    value: format!(
                        "SELECT 1 succeeded on {}:{} in {} ms",
                        host, port, latency_ms
                    ),
                });
            }
            PostgresqlProbeResult::Failed {
                host,
                port,
                classification,
            } => {
                detail.push(PostgresqlMonitoringDetailItem {
                    title: "live probe".into(),
                    value: format!("SELECT 1 {} on {}:{}", classification, host, port),
                });
                evidence.push(PostgresqlMonitoringEvidenceItem {
                    title: "probe".into(),
                    value: format!("SELECT 1 {} on {}:{}", classification, host, port),
                });
                missing_data.push(PostgresqlMonitoringMissingDataItem {
                    title: "live probe".into(),
                    value: format!("SELECT 1 {} on {}:{}", classification, host, port),
                });
            }
        }
    }

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

fn matches_clear_postgresql_name(value: &str) -> bool {
    ["postgresql", "postgres", "pgsql"]
        .iter()
        .any(|needle| contains_token(value, needle))
}

fn contains_token(value: &str, needle: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let mut start = 0usize;
    while let Some(idx) = lower[start..].find(needle) {
        let idx = start + idx;
        let before = lower[..idx].chars().next_back();
        let after = lower[idx + needle.len()..].chars().next();
        let before_ok = before.map(|c| !c.is_ascii_alphanumeric()).unwrap_or(true);
        let after_ok = after.map(|c| !c.is_ascii_alphanumeric()).unwrap_or(true);
        if before_ok && after_ok {
            return true;
        }
        start = idx + needle.len();
    }
    false
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

fn is_clear_postgresql_service(service: &ServiceInfo) -> bool {
    let name_or_selector = matches_clear_postgresql_name(&service.name)
        || service.selector.iter().any(|kv| {
            matches_clear_postgresql_name(&kv.key) || matches_clear_postgresql_name(&kv.value)
        });
    let has_postgresql_port = service
        .ports
        .iter()
        .any(|port| port.port == 5432 || port.target_port.as_deref() == Some("5432"));

    name_or_selector && has_postgresql_port
}

enum PostgresqlProbeResult {
    Connected {
        host: String,
        port: u16,
        latency_ms: u128,
    },
    Failed {
        host: String,
        port: u16,
        classification: String,
    },
}

async fn probe_postgresql_service(
    client: Option<&Client>,
    cfg: &Config,
    service: &ServiceInfo,
) -> PostgresqlProbeResult {
    let Some(port) = select_postgresql_port(service) else {
        return PostgresqlProbeResult::Failed {
            host: postgresql_service_host(service),
            port: 5432,
            classification: "failed: no usable port".into(),
        };
    };

    let settings = resolve_postgresql_probe_settings(client, cfg, service, port).await;
    let host = settings.host.clone();
    let started_at = Instant::now();

    let probe = if settings.uses_tls() {
        match build_tls_connector(&settings) {
            Ok(tls) => probe_with_tls(&settings, tls).await,
            Err(classification) => PostgresqlProbeResult::Failed {
                host,
                port,
                classification,
            },
        }
    } else {
        probe_without_tls(&settings).await
    };

    match probe {
        PostgresqlProbeResult::Connected { .. } => PostgresqlProbeResult::Connected {
            host: settings.host,
            port: settings.port,
            latency_ms: started_at.elapsed().as_millis(),
        },
        PostgresqlProbeResult::Failed { classification, .. } => PostgresqlProbeResult::Failed {
            host: settings.host,
            port: settings.port,
            classification,
        },
    }
}

#[derive(Clone, Debug)]
struct PostgresqlProbeSettings {
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    database: String,
    sslmode: String,
    sslrootcert: Option<String>,
}

impl PostgresqlProbeSettings {
    fn uses_tls(&self) -> bool {
        !self.sslmode.eq_ignore_ascii_case("disable")
    }

    fn connection_string(&self) -> String {
        let mut parts = vec![
            format!("host={}", self.host),
            format!("port={}", self.port),
            format!("user={}", self.user),
            format!("dbname={}", self.database),
        ];
        if let Some(password) = &self.password {
            parts.push(format!("password={}", password));
        }
        parts.join(" ")
    }
}

async fn resolve_postgresql_probe_settings(
    client: Option<&Client>,
    cfg: &Config,
    service: &ServiceInfo,
    port: u16,
) -> PostgresqlProbeSettings {
    let mut settings = PostgresqlProbeSettings {
        host: postgresql_service_host(service),
        port,
        user: "postgres".to_string(),
        password: None,
        database: "postgres".to_string(),
        sslmode: "disable".to_string(),
        sslrootcert: None,
    };

    if let (Some(client), Some(secret_name)) =
        (client, cfg.postgresql_monitoring_secret_name.as_deref())
    {
        if let Some(secret_data) =
            load_secret_string_data(client, &service.namespace, secret_name).await
        {
            apply_postgresql_probe_overrides(&mut settings, &secret_data);
        }
    }

    apply_postgresql_probe_overrides_from_config(&mut settings, cfg);
    settings
}

fn apply_postgresql_probe_overrides(
    settings: &mut PostgresqlProbeSettings,
    values: &std::collections::BTreeMap<String, String>,
) {
    if let Some(value) = values.get("host").filter(|value| !value.trim().is_empty()) {
        settings.host = value.trim().to_string();
    }
    if let Some(value) = values
        .get("port")
        .and_then(|value| value.trim().parse::<u16>().ok())
    {
        settings.port = value;
    }
    if let Some(value) = values.get("user").filter(|value| !value.trim().is_empty()) {
        settings.user = value.trim().to_string();
    }
    if let Some(value) = values
        .get("password")
        .filter(|value| !value.trim().is_empty())
    {
        settings.password = Some(value.trim().to_string());
    }
    if let Some(value) = values
        .get("database")
        .filter(|value| !value.trim().is_empty())
    {
        settings.database = value.trim().to_string();
    }
    if let Some(value) = values
        .get("sslmode")
        .filter(|value| !value.trim().is_empty())
    {
        settings.sslmode = value.trim().to_string();
    }
    if let Some(value) = values
        .get("sslrootcert")
        .filter(|value| !value.trim().is_empty())
    {
        settings.sslrootcert = Some(value.trim().to_string());
    }
}

fn apply_postgresql_probe_overrides_from_config(
    settings: &mut PostgresqlProbeSettings,
    cfg: &Config,
) {
    if let Some(value) = cfg
        .postgresql_monitoring_host
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        settings.host = value.trim().to_string();
    }
    if let Some(value) = cfg.postgresql_monitoring_port {
        settings.port = value;
    }
    if let Some(value) = cfg
        .postgresql_monitoring_user
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        settings.user = value.trim().to_string();
    }
    if let Some(value) = cfg
        .postgresql_monitoring_password
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        settings.password = Some(value.trim().to_string());
    }
    if let Some(value) = cfg
        .postgresql_monitoring_database
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        settings.database = value.trim().to_string();
    }
    if let Some(value) = cfg
        .postgresql_monitoring_sslmode
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        settings.sslmode = value.trim().to_string();
    }
    if let Some(value) = cfg
        .postgresql_monitoring_sslrootcert
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        settings.sslrootcert = Some(value.trim().to_string());
    }
}

async fn load_secret_string_data(
    client: &Client,
    namespace: &str,
    secret_name: &str,
) -> Option<std::collections::BTreeMap<String, String>> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let secret = api.get_opt(secret_name).await.ok().flatten()?;
    let mut values = std::collections::BTreeMap::new();

    if let Some(data) = secret.data {
        for (key, bytes) in data {
            if let Ok(value) = String::from_utf8(bytes.0) {
                values.insert(key, value);
            }
        }
    }

    Some(values)
}

async fn probe_without_tls(settings: &PostgresqlProbeSettings) -> PostgresqlProbeResult {
    let connect_str = settings.connection_string();
    let started_at = Instant::now();

    let connect = timeout(
        Duration::from_secs(4),
        tokio_postgres::connect(&connect_str, NoTls),
    )
    .await;

    let (client, connection) = match connect {
        Ok(Ok(pair)) => pair,
        Ok(Err(err)) => {
            let message = err.to_string();
            return PostgresqlProbeResult::Failed {
                host: settings.host.clone(),
                port: settings.port,
                classification: classify_postgresql_probe_error(&message),
            };
        }
        Err(_) => {
            return PostgresqlProbeResult::Failed {
                host: settings.host.clone(),
                port: settings.port,
                classification: "failed: timeout".into(),
            };
        }
    };

    tokio::spawn(async move {
        let _ = connection.await;
    });

    let query = timeout(Duration::from_secs(4), client.query_one("SELECT 1", &[])).await;
    match query {
        Ok(Ok(_row)) => PostgresqlProbeResult::Connected {
            host: settings.host.clone(),
            port: settings.port,
            latency_ms: started_at.elapsed().as_millis(),
        },
        Ok(Err(err)) => {
            let message = err.to_string();
            PostgresqlProbeResult::Failed {
                host: settings.host.clone(),
                port: settings.port,
                classification: classify_postgresql_probe_error(&message),
            }
        }
        Err(_) => PostgresqlProbeResult::Failed {
            host: settings.host.clone(),
            port: settings.port,
            classification: "failed: timeout".into(),
        },
    }
}

async fn probe_with_tls(
    settings: &PostgresqlProbeSettings,
    tls: MakeTlsConnector,
) -> PostgresqlProbeResult {
    let connect_str = settings.connection_string();
    let started_at = Instant::now();

    let connect = timeout(
        Duration::from_secs(4),
        tokio_postgres::connect(&connect_str, tls),
    )
    .await;

    let (client, connection) = match connect {
        Ok(Ok(pair)) => pair,
        Ok(Err(err)) => {
            let message = err.to_string();
            return PostgresqlProbeResult::Failed {
                host: settings.host.clone(),
                port: settings.port,
                classification: classify_postgresql_probe_error(&message),
            };
        }
        Err(_) => {
            return PostgresqlProbeResult::Failed {
                host: settings.host.clone(),
                port: settings.port,
                classification: "failed: timeout".into(),
            };
        }
    };

    tokio::spawn(async move {
        let _ = connection.await;
    });

    let query = timeout(Duration::from_secs(4), client.query_one("SELECT 1", &[])).await;
    match query {
        Ok(Ok(_row)) => PostgresqlProbeResult::Connected {
            host: settings.host.clone(),
            port: settings.port,
            latency_ms: started_at.elapsed().as_millis(),
        },
        Ok(Err(err)) => {
            let message = err.to_string();
            PostgresqlProbeResult::Failed {
                host: settings.host.clone(),
                port: settings.port,
                classification: classify_postgresql_probe_error(&message),
            }
        }
        Err(_) => PostgresqlProbeResult::Failed {
            host: settings.host.clone(),
            port: settings.port,
            classification: "failed: timeout".into(),
        },
    }
}

fn build_tls_connector(settings: &PostgresqlProbeSettings) -> Result<MakeTlsConnector, String> {
    let mut builder = TlsConnector::builder();
    if let Some(pem) = settings.sslrootcert.as_deref() {
        let cert = Certificate::from_pem(pem.as_bytes())
            .map_err(|_| "failed: tls certificate parse".to_string())?;
        builder.add_root_certificate(cert);
    }

    builder
        .build()
        .map(MakeTlsConnector::new)
        .map_err(|_| "failed: tls connector build".to_string())
}

fn select_postgresql_port(service: &ServiceInfo) -> Option<u16> {
    service
        .ports
        .iter()
        .find(|port| port.port == 5432 || port.target_port.as_deref() == Some("5432"))
        .map(|port| port.port as u16)
}

fn postgresql_service_host(service: &ServiceInfo) -> String {
    format!("{}.{}.svc.cluster.local", service.name, service.namespace)
}

fn classify_postgresql_probe_error(message: &str) -> String {
    let lower = message.to_ascii_lowercase();
    let classification = if lower.contains("timed out") || lower.contains("timeout") {
        "failed: timeout"
    } else if lower.contains("refused") {
        "failed: connection refused"
    } else if lower.contains("lookup")
        || lower.contains("resolve")
        || lower.contains("name or service not known")
        || lower.contains("no such host")
    {
        "failed: dns"
    } else if lower.contains("authentication")
        || lower.contains("password")
        || lower.contains("permission denied")
    {
        "failed: auth"
    } else if lower.contains("ssl") || lower.contains("tls") || lower.contains("handshake") {
        "failed: tls"
    } else {
        "failed: io"
    };

    classification.to_string()
}

fn pvc_match_reason(pvc: &PersistentVolumeClaimInfo) -> String {
    let mut reasons = Vec::new();
    if matches_postgresql_name(&pvc.name) {
        reasons.push("name");
    }
    if pvc
        .volume_name
        .as_deref()
        .map(matches_postgresql_name)
        .unwrap_or(false)
    {
        reasons.push("volume_name");
    }
    if pvc
        .storage_class
        .as_deref()
        .map(matches_postgresql_name)
        .unwrap_or(false)
    {
        reasons.push("storage_class");
    }
    if reasons.is_empty() {
        "unknown".to_string()
    } else {
        reasons.join(",")
    }
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
            workload_monitoring_enabled: enabled,
            workload_monitoring_namespaces: namespaces,
            workload_monitoring_targets: vec!["angular".into(), "spring_boot".into()],
            app_metrics_enabled: false,
            app_metrics_discovery_enabled: true,
            app_metrics_namespaces: Vec::new(),
            app_metrics_allowlist: Vec::new(),
            app_metrics_timeout: Duration::from_secs(3),
            app_metrics_max_samples: 500,
            postgresql_monitoring_enabled: false,
            postgresql_monitoring_namespaces: vec![],
            postgresql_monitoring_secret_name: None,
            postgresql_monitoring_host: None,
            postgresql_monitoring_port: None,
            postgresql_monitoring_user: None,
            postgresql_monitoring_password: None,
            postgresql_monitoring_database: None,
            postgresql_monitoring_sslmode: None,
            postgresql_monitoring_sslrootcert: None,
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
            AppMetricsInventory::default(),
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
            AppMetricsInventory::default(),
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
            AppMetricsInventory::default(),
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
            AppMetricsInventory::default(),
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
            AppMetricsInventory::default(),
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
            observed_endpoints: 3,
            connected_endpoints: 2,
            unavailable_endpoints: 1,
        };
        let result = build_workload_monitoring(
            &cfg,
            &Workloads::default(),
            &[],
            &NetworkInventory::default(),
            &[],
            &deps,
            AppMetricsInventory::default(),
            WorkloadMonitoringLogs::default(),
        )
        .unwrap();
        let signals_deps = result.signals.dependencies.unwrap();
        assert_eq!(signals_deps.edges.len(), 1);
    }

    #[test]
    fn clear_postgresql_service_requires_name_and_port() {
        let name_only = ServiceInfo {
            namespace: "customer-db".into(),
            name: "db-service".into(),
            type_: "ClusterIP".into(),
            cluster_ip: Some("10.0.0.2".into()),
            external_ips: vec![],
            selector: vec![],
            ports: vec![ServicePortInfo {
                name: Some("postgresql".into()),
                protocol: Some("TCP".into()),
                port: 5432,
                target_port: Some("5432".into()),
                node_port: None,
            }],
            load_balancer_ingress: vec![],
        };
        assert!(!is_clear_postgresql_service(&name_only));

        let portless = ServiceInfo {
            namespace: "customer-db".into(),
            name: "postgresql".into(),
            type_: "ClusterIP".into(),
            cluster_ip: Some("10.0.0.2".into()),
            external_ips: vec![],
            selector: vec![KV {
                key: "app".into(),
                value: "postgresql".into(),
            }],
            ports: vec![ServicePortInfo {
                name: Some("http".into()),
                protocol: Some("TCP".into()),
                port: 8080,
                target_port: Some("8080".into()),
                node_port: None,
            }],
            load_balancer_ingress: vec![],
        };
        assert!(!is_clear_postgresql_service(&portless));

        let clear = postgres_service("customer-db", "postgresql");
        assert!(is_clear_postgresql_service(&clear));
    }

    #[tokio::test]
    async fn build_postgresql_returns_none_when_disabled() {
        let cfg = base_config(false, vec!["customer-app".into()]);
        let result = build_postgresql_monitoring(
            None,
            &cfg,
            &Workloads::default(),
            &[],
            &NetworkInventory::default(),
            &StorageInventory::default(),
            &[],
        )
        .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn build_postgresql_is_healthy_with_complete_evidence() {
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

        let result = build_postgresql_monitoring_with_probe(
            &cfg,
            &workloads,
            &pods,
            &network,
            &storage,
            &events,
            |_| {
                Box::pin(std::future::ready(PostgresqlProbeResult::Connected {
                    host: "postgresql.customer-db.svc.cluster.local".into(),
                    port: 5432,
                    latency_ms: 12,
                }))
            },
        )
        .await
        .expect("plugin should be present");

        assert_eq!(result.status, "healthy");
        assert_eq!(result.namespaces, vec!["customer-db"]);
        assert!(result.summary.contains("running pod"));
        assert!(
            result
                .evidence
                .iter()
                .any(|item| item.title == "service" && item.value.starts_with("clear "))
        );
        assert!(
            result
                .evidence
                .iter()
                .any(|item| item.title == "workload_owner")
        );
        assert!(
            result.missing_data.is_empty()
                || result
                    .missing_data
                    .iter()
                    .all(|item| item.title != "service")
        );
    }

    #[tokio::test]
    async fn build_postgresql_reports_warning_when_signals_are_incomplete() {
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
            None,
            &cfg,
            &workloads,
            &pods,
            &NetworkInventory::default(),
            &StorageInventory::default(),
            &[],
        )
        .await
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

    #[tokio::test]
    async fn build_postgresql_skips_live_probe_when_service_is_not_clear() {
        let mut cfg = base_config(false, vec![]);
        cfg.postgresql_monitoring_enabled = true;
        cfg.postgresql_monitoring_namespaces = vec!["customer-db".into()];

        let services = NetworkInventory {
            services: vec![ServiceInfo {
                namespace: "customer-db".into(),
                name: "db-service".into(),
                type_: "ClusterIP".into(),
                cluster_ip: Some("10.0.0.2".into()),
                external_ips: vec![],
                selector: vec![],
                ports: vec![ServicePortInfo {
                    name: Some("postgresql".into()),
                    protocol: Some("TCP".into()),
                    port: 5432,
                    target_port: Some("5432".into()),
                    node_port: None,
                }],
                load_balancer_ingress: vec![],
            }],
            ingresses: vec![],
        };

        let result = build_postgresql_monitoring(
            None,
            &cfg,
            &Workloads::default(),
            &[],
            &services,
            &StorageInventory::default(),
            &[],
        )
        .await
        .expect("plugin should be present");

        assert!(
            result
                .missing_data
                .iter()
                .any(|item| item.title == "live probe")
        );
        assert!(result.evidence.iter().all(|item| item.title != "probe"));
    }

    #[tokio::test]
    async fn build_postgresql_reports_unknown_when_multiple_clear_services_exist() {
        let mut cfg = base_config(false, vec![]);
        cfg.postgresql_monitoring_enabled = true;
        cfg.postgresql_monitoring_namespaces = vec!["customer-db".into()];

        let services = NetworkInventory {
            services: vec![
                postgres_service("customer-db", "postgresql-a"),
                postgres_service("customer-db", "postgresql-b"),
            ],
            ingresses: vec![],
        };

        let result = build_postgresql_monitoring_with_probe(
            &cfg,
            &Workloads::default(),
            &[],
            &services,
            &StorageInventory::default(),
            &[],
            |_| {
                Box::pin(std::future::ready(PostgresqlProbeResult::Connected {
                    host: "should-not-run".into(),
                    port: 5432,
                    latency_ms: 1,
                }))
            },
        )
        .await
        .expect("plugin should be present");

        assert_eq!(result.status, "unknown");
        assert!(
            result
                .missing_data
                .iter()
                .any(|item| item.title == "live probe")
        );
        assert!(result.evidence.iter().all(|item| item.title != "probe"));
        assert_eq!(
            result
                .detail
                .iter()
                .find(|item| item.title == "clear services")
                .map(|item| item.value.as_str()),
            Some("2")
        );
    }

    #[tokio::test]
    async fn build_postgresql_reports_critical_when_no_evidence_matches() {
        let mut cfg = base_config(false, vec![]);
        cfg.postgresql_monitoring_enabled = true;
        cfg.postgresql_monitoring_namespaces = vec!["customer-db".into()];

        let result = build_postgresql_monitoring(
            None,
            &cfg,
            &Workloads::default(),
            &[],
            &NetworkInventory::default(),
            &StorageInventory::default(),
            &[],
        )
        .await
        .expect("plugin should be present");

        assert_eq!(result.status, "critical");
        assert!(result.summary.contains("No PostgreSQL workload"));
        assert!(!result.detail.is_empty());
    }
}
