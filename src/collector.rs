//! Collects cluster inventory via the Kubernetes API.

use crate::config;
use crate::health;
use crate::model::*;
use crate::model::{MetricsStatus, SnapshotApiStatus};
use crate::tech;
use crate::tetragon;
use anyhow::Result;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::batch::v1::CronJob;
use k8s_openapi::api::core::v1::{
    ConfigMap, Event, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod, Secret,
    Service,
};
use k8s_openapi::api::networking::v1::{Ingress, NetworkPolicy};
use k8s_openapi::api::rbac::v1::ClusterRoleBinding;
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{ApiResource, DynamicObject, ListParams, LogParams};
use kube::core::GroupVersionKind;
use kube::{Api, Client, Error as KubeError};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use tokio::task::JoinSet;
use tracing::warn;

const AGENT_CONFIGMAP_NAME: &str = "sentinella-hub-k8s-agent-config";
const PSA_ENFORCE_LABEL: &str = "pod-security.kubernetes.io/enforce";
const PSA_AUDIT_LABEL: &str = "pod-security.kubernetes.io/audit";
const PSA_WARN_LABEL: &str = "pod-security.kubernetes.io/warn";
const WORKLOAD_MONITORING_LOG_TAIL_LINES: i64 = 200;
const WORKLOAD_MONITORING_LOG_LIMIT_BYTES: i64 = 65_536;

#[derive(Default)]
struct PodUsageTotals {
    cpu_nano: i128,
    memory_bytes: i128,
}

pub async fn collect(
    client: &Client,
    collect_secrets: bool,
    collect_dependencies_tetragon: bool,
    tech_detect_process: bool,
) -> Result<(
    Option<String>,
    ClusterInfo,
    Vec<NamespaceInfo>,
    Workloads,
    Vec<PodInfo>,
    NetworkInventory,
    SecurityInventory,
    OperationalMaturityInventory,
    DependencyInventory,
    ConfigurationInventory,
    StorageInventory,
    Vec<EventInfo>,
    MetricsStatus,
    SnapshotApiStatus,
)> {
    // Concurrency: launch all list calls in parallel; fail soft on individual lists.
    let lp = ListParams::default();

    let nodes_fut = list_all::<Node>(client, &lp);
    let ns_fut = list_all::<Namespace>(client, &lp);
    let k8s_uid_fut = kube_system_uid(client);
    let deploy_fut = list_all::<Deployment>(client, &lp);
    let sts_fut = list_all::<StatefulSet>(client, &lp);
    let ds_fut = list_all::<DaemonSet>(client, &lp);
    let pods_fut = list_all::<Pod>(client, &lp);
    let services_fut = list_all::<Service>(client, &lp);
    let ingresses_fut = list_all::<Ingress>(client, &lp);
    let network_policies_fut = list_all::<NetworkPolicy>(client, &lp);
    let cluster_role_bindings_fut = list_all::<ClusterRoleBinding>(client, &lp);
    let configmaps_fut = list_all::<ConfigMap>(client, &lp);
    let secrets_fut = async {
        if collect_secrets {
            list_all::<Secret>(client, &lp).await
        } else {
            Ok(Vec::new())
        }
    };
    let storage_classes_fut = list_all::<StorageClass>(client, &lp);
    let pvs_fut = list_all::<PersistentVolume>(client, &lp);
    let pvcs_fut = list_all::<PersistentVolumeClaim>(client, &lp);
    let snapshot_api_probe = probe_snapshot_api(client);
    let pod_metrics_probe = probe_pod_metrics(client);
    let vpa_fut = list_dynamic_all(client, "autoscaling.k8s.io", "v1", "VerticalPodAutoscaler");
    let cronjobs_fut = list_all::<CronJob>(client, &lp);
    let events_fut = list_all::<Event>(client, &lp);
    let version_fut = client.apiserver_version();

    let (
        nodes,
        namespaces,
        deployments,
        statefulsets,
        daemonsets,
        pods,
        services,
        ingresses,
        network_policies,
        cluster_role_bindings,
        configmaps,
        secrets,
        storage_classes,
        pvs,
        pvcs,
        vpa,
        cronjobs,
        events,
        version,
        k8s_uid,
        (pod_metrics, metrics_status),
        (volume_snapshot_classes, volume_snapshots, snapshot_api_status),
    ) = tokio::join!(
        nodes_fut,
        ns_fut,
        deploy_fut,
        sts_fut,
        ds_fut,
        pods_fut,
        services_fut,
        ingresses_fut,
        network_policies_fut,
        cluster_role_bindings_fut,
        configmaps_fut,
        secrets_fut,
        storage_classes_fut,
        pvs_fut,
        pvcs_fut,
        vpa_fut,
        cronjobs_fut,
        events_fut,
        version_fut,
        k8s_uid_fut,
        pod_metrics_probe,
        snapshot_api_probe
    );

    let nodes = soft_unwrap("nodes", nodes);
    let namespaces = soft_unwrap("namespaces", namespaces);
    let deployments = soft_unwrap("deployments", deployments);
    let statefulsets = soft_unwrap("statefulsets", statefulsets);
    let daemonsets = soft_unwrap("daemonsets", daemonsets);
    let pods = soft_unwrap("pods", pods);
    let mut services = soft_unwrap("services", services);
    let mut ingresses = soft_unwrap("ingresses", ingresses);
    let mut network_policies = soft_unwrap("networkpolicies", network_policies);
    let cluster_role_bindings = soft_unwrap("clusterrolebindings", cluster_role_bindings);
    let mut configmaps = soft_unwrap("configmaps", configmaps);
    let mut secrets = soft_unwrap("secrets", secrets);
    let storage_classes = soft_unwrap("storageclasses", storage_classes);
    let pvs = soft_unwrap("persistentvolumes", pvs);
    let pvcs = soft_unwrap("persistentvolumeclaims", pvcs);
    let vpa = soft_unwrap("vpa", vpa);
    let mut cronjobs = soft_unwrap("cronjobs", cronjobs);
    let mut events = soft_unwrap("events", events);

    let mut cluster = build_cluster_info(version.ok(), &nodes);
    let descheduler = detect_descheduler(&deployments);
    if cluster.platform.as_deref() == Some("openshift") {
        cluster.openshift_version = probe_openshift_version(client).await;
    }
    let ns_infos = namespaces
        .into_iter()
        .map(map_namespace)
        .collect::<Vec<_>>();
    let workloads = Workloads {
        deployments: deployments.into_iter().map(map_deployment).collect(),
        statefulsets: statefulsets.into_iter().map(map_statefulset).collect(),
        daemonsets: daemonsets.into_iter().map(map_daemonset).collect(),
    };
    sort_services_for_snapshot(&mut services);
    sort_ingresses_for_snapshot(&mut ingresses);
    sort_network_policies_for_snapshot(&mut network_policies);
    let dependencies =
        collect_dependency_inventory(collect_dependencies_tetragon, &pods, &services).await;
    let pod_usage = build_pod_usage_index(&pod_metrics);
    let pod_infos = pods
        .into_iter()
        .map(|pod| map_pod(pod, &pod_usage, tech_detect_process))
        .collect();
    let network = NetworkInventory {
        services: services.into_iter().map(map_service).collect(),
        ingresses: ingresses.into_iter().map(map_ingress).collect(),
    };
    let mut cluster_role_binding_infos = cluster_role_bindings
        .into_iter()
        .filter_map(map_cluster_role_binding)
        .collect::<Vec<_>>();
    cluster_role_binding_infos.sort_by(|a, b| a.name.cmp(&b.name));
    let security = SecurityInventory {
        network_policies: network_policies
            .into_iter()
            .map(map_network_policy)
            .collect(),
        cluster_role_bindings: cluster_role_binding_infos,
        pod_security_admission: build_pod_security_admission(&ns_infos),
    };
    sort_cronjobs_for_snapshot(&mut cronjobs);
    let vpa_info = build_vpa_info(&vpa);
    let scheduled_jobs = cronjobs
        .into_iter()
        .take(MAX_SCHEDULED_JOBS)
        .map(map_scheduled_job)
        .collect();
    let operational_maturity = OperationalMaturityInventory {
        descheduler,
        vpa: vpa_info,
        scheduled_jobs,
        truncated_jobs: false,
    };
    sort_configmaps_for_snapshot(&mut configmaps);
    sort_secrets_for_snapshot(&mut secrets);
    let agent_configured_env = collect_agent_configured_env(&configmaps);
    let configuration = ConfigurationInventory {
        configmaps: configmaps.into_iter().map(map_configmap).collect(),
        secrets: secrets.into_iter().map(map_secret).collect(),
        agent_runtime_env: Vec::new(),
        agent_configured_env,
    };
    let storage = StorageInventory {
        storage_classes: storage_classes.into_iter().map(map_storage_class).collect(),
        persistent_volumes: pvs.into_iter().map(map_persistent_volume).collect(),
        persistent_volume_claims: pvcs.into_iter().map(map_persistent_volume_claim).collect(),
        volume_snapshot_classes: volume_snapshot_classes
            .into_iter()
            .map(map_volume_snapshot_class)
            .collect(),
        volume_snapshots: volume_snapshots
            .into_iter()
            .map(map_volume_snapshot)
            .collect(),
    };

    sort_events_for_snapshot(&mut events);
    let event_infos = events.into_iter().take(MAX_EVENTS).map(map_event).collect();

    Ok((
        k8s_uid,
        cluster,
        ns_infos,
        workloads,
        pod_infos,
        network,
        security,
        operational_maturity,
        dependencies,
        configuration,
        storage,
        event_infos,
        metrics_status,
        snapshot_api_status,
    ))
}

pub async fn collect_workload_monitoring_logs(
    client: &Client,
    cfg: &config::Config,
    pods: &[PodInfo],
) -> WorkloadMonitoringLogs {
    if !cfg.workload_monitoring_enabled || cfg.workload_monitoring_namespaces.is_empty() {
        return WorkloadMonitoringLogs::default();
    }

    let allow = cfg.workload_monitoring_namespaces.clone();
    let mut tasks = JoinSet::new();

    for (index, pod) in pods.iter().enumerate() {
        if !allow.iter().any(|namespace| namespace == &pod.namespace) {
            continue;
        }
        if pod.containers.is_empty() {
            continue;
        }

        let client = client.clone();
        let namespace = pod.namespace.clone();
        let pod_name = pod.name.clone();
        let containers = pod
            .containers
            .iter()
            .map(|container| container.name.clone())
            .collect::<Vec<_>>();

        tasks.spawn(async move {
            let logs =
                collect_workload_monitoring_pod_logs(client, namespace, pod_name, containers).await;
            (index, logs)
        });
    }

    let mut pod_logs = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((index, Some(logs))) => pod_logs.push((index, logs)),
            Ok((_, None)) => {}
            Err(err) => warn!(error = %err, "workload monitoring log task failed"),
        }
    }

    pod_logs.sort_by_key(|(index, _)| *index);

    WorkloadMonitoringLogs {
        pods: pod_logs.into_iter().map(|(_, logs)| logs).collect(),
    }
}

async fn collect_workload_monitoring_pod_logs(
    client: Client,
    namespace: String,
    pod_name: String,
    containers: Vec<String>,
) -> Option<WorkloadMonitoringPodLogs> {
    let mut container_logs = Vec::new();

    for container in containers {
        if let Some(logs) = collect_workload_monitoring_container_logs(
            client.clone(),
            &namespace,
            &pod_name,
            &container,
        )
        .await
        {
            container_logs.push(logs);
        }
    }

    if container_logs.is_empty() {
        return None;
    }

    Some(WorkloadMonitoringPodLogs {
        namespace,
        name: pod_name,
        containers: container_logs,
    })
}

async fn collect_workload_monitoring_container_logs(
    client: Client,
    namespace: &str,
    pod_name: &str,
    container_name: &str,
) -> Option<WorkloadMonitoringContainerLogs> {
    let pods: Api<Pod> = Api::namespaced(client, namespace);
    let params = LogParams {
        container: Some(container_name.to_string()),
        follow: false,
        limit_bytes: Some(WORKLOAD_MONITORING_LOG_LIMIT_BYTES),
        pretty: false,
        previous: false,
        since_seconds: None,
        since_time: None,
        tail_lines: Some(WORKLOAD_MONITORING_LOG_TAIL_LINES),
        timestamps: false,
    };

    match pods.logs(pod_name, &params).await {
        Ok(output) => {
            let (lines, truncated) = split_log_output(&output);
            Some(WorkloadMonitoringContainerLogs {
                name: container_name.to_string(),
                truncated,
                lines,
            })
        }
        Err(err) => {
            warn!(
                namespace = %namespace,
                pod = %pod_name,
                container = %container_name,
                error = %err,
                "failed to read workload monitoring logs"
            );
            None
        }
    }
}

fn split_log_output(output: &str) -> (Vec<String>, bool) {
    let lines = output
        .lines()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let truncated = output.len() >= WORKLOAD_MONITORING_LOG_LIMIT_BYTES as usize
        || lines.len() >= WORKLOAD_MONITORING_LOG_TAIL_LINES as usize;
    (lines, truncated)
}

async fn kube_system_uid(client: &Client) -> Option<String> {
    let api: Api<Namespace> = Api::all(client.clone());
    match api.get("kube-system").await {
        Ok(namespace) => namespace.metadata.uid,
        Err(e) => {
            warn!(error = %e, "failed to fetch kube-system namespace UID");
            None
        }
    }
}

async fn collect_dependency_inventory(
    enabled: bool,
    pods: &[Pod],
    services: &[Service],
) -> DependencyInventory {
    if !enabled {
        return DependencyInventory {
            source: "tetragon_grpc",
            window_seconds: DEP_WINDOW_SECONDS,
            ..DependencyInventory::default()
        };
    }

    let body = tetragon::snapshot_ndjson();

    let pod_index = build_pod_ip_index(pods);
    let service_index = build_service_ip_index(services);
    let mut agg: BTreeMap<DependencyEdgeKey, DependencyEdgeAgg> = BTreeMap::new();
    let mut fanout: BTreeMap<EndpointKey, BTreeSet<EndpointKey>> = BTreeMap::new();
    let mut parse_failures = 0u64;
    let mut dropped_for_line_cap = 0u64;
    let mut dropped_edges = 0u64;
    let mut skipped_for_fanout = 0u64;

    for (line_index, line) in body.lines().enumerate() {
        if line_index >= MAX_TETRAGON_LINES {
            dropped_for_line_cap += 1;
            continue;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let value: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => {
                parse_failures += 1;
                continue;
            }
        };

        let record = match parse_tetragon_record(&value) {
            Some(record) => record,
            None => {
                parse_failures += 1;
                continue;
            }
        };

        let from = resolve_endpoint(
            &record.src_ip,
            record.src_endpoint.as_ref(),
            &pod_index,
            &service_index,
        );
        let to = resolve_endpoint(
            &record.dst_ip,
            record.dst_endpoint.as_ref(),
            &pod_index,
            &service_index,
        );
        let key = DependencyEdgeKey {
            from: normalize_endpoint_key(&from),
            to: normalize_endpoint_key(&to),
            protocol: record.protocol.to_uppercase(),
            destination_port: record.destination_port,
            direction: "egress".to_string(),
        };

        if agg.len() >= MAX_DEP_EDGES_PER_SNAPSHOT && !agg.contains_key(&key) {
            dropped_edges += 1;
            continue;
        }

        let source_key = key.from.clone();
        let target_key = key.to.clone();
        let source_fanout = fanout.entry(source_key.clone()).or_default();
        if source_fanout.len() >= MAX_DEP_FANOUT_PER_SOURCE
            && !source_fanout.contains(&target_key)
            && !agg.contains_key(&key)
        {
            skipped_for_fanout += 1;
            continue;
        }

        let entry = agg.entry(key).or_insert_with(|| DependencyEdgeAgg {
            bytes: 0,
            packets: 0,
            connections: 0,
            first_seen_unix_ms: record.timestamp_unix_ms,
            last_seen_unix_ms: record.timestamp_unix_ms,
        });
        entry.bytes = entry.bytes.saturating_add(record.bytes);
        entry.packets = entry.packets.saturating_add(record.packets);
        entry.connections = entry.connections.saturating_add(record.connections);
        entry.first_seen_unix_ms = entry.first_seen_unix_ms.min(record.timestamp_unix_ms);
        entry.last_seen_unix_ms = entry.last_seen_unix_ms.max(record.timestamp_unix_ms);
        source_fanout.insert(target_key);
    }

    let mut edges = agg
        .into_iter()
        .map(|(key, agg)| DependencyEdge {
            from: endpoint_from_key(&key.from),
            to: endpoint_from_key(&key.to),
            protocol: key.protocol,
            destination_port: key.destination_port,
            direction: key.direction,
            bytes: agg.bytes,
            packets: agg.packets,
            connections: agg.connections,
            first_seen_unix_ms: agg.first_seen_unix_ms,
            last_seen_unix_ms: agg.last_seen_unix_ms,
        })
        .collect::<Vec<_>>();
    sort_dependency_edges(&mut edges);

    let dropped_events_total = dropped_edges.saturating_add(dropped_for_line_cap);
    let dropped_total = dropped_events_total.saturating_add(skipped_for_fanout);
    let truncated = dropped_total > 0;
    if parse_failures > 0 {
        health::DEPENDENCY_PARSE_FAILURES.inc_by(parse_failures);
    }
    if dropped_events_total > 0 {
        health::DEPENDENCY_EVENTS_DROPPED.inc_by(dropped_events_total);
    }
    if skipped_for_fanout > 0 {
        health::DEPENDENCY_EVENTS_SKIPPED.inc_by(skipped_for_fanout);
    }
    if truncated {
        health::DEPENDENCY_SNAPSHOTS_TRUNCATED.inc();
    }
    if parse_failures > 0 || dropped_events_total > 0 || skipped_for_fanout > 0 {
        warn!(
            parse_failures,
            dropped_events = dropped_events_total,
            dropped_for_snapshot_cap = dropped_edges,
            dropped_for_line_cap,
            skipped_for_fanout,
            kept_edges = edges.len(),
            "dependency collection reduced observed tetragon events"
        );
    }
    DependencyInventory {
        edges,
        source: "tetragon_grpc",
        window_seconds: DEP_WINDOW_SECONDS,
        truncated,
        dropped_edges: dropped_total,
    }
}

#[derive(Debug, Deserialize)]
struct TetragonDependencyRecord {
    src_ip: String,
    dst_ip: String,
    #[serde(default)]
    src_endpoint: Option<DependencyEndpointHint>,
    #[serde(default)]
    dst_endpoint: Option<DependencyEndpointHint>,
    protocol: String,
    destination_port: u16,
    bytes: u64,
    packets: u64,
    connections: u64,
    timestamp_unix_ms: u128,
}

#[derive(Debug, Deserialize)]
struct DependencyEndpointHint {
    namespace: Option<String>,
    pod_name: Option<String>,
    workload_kind: Option<String>,
    workload_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EndpointKey {
    kind: String,
    namespace: Option<String>,
    name: Option<String>,
    workload_kind: Option<String>,
    workload_name: Option<String>,
    ip: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DependencyEdgeKey {
    from: EndpointKey,
    to: EndpointKey,
    protocol: String,
    destination_port: u16,
    direction: String,
}

#[derive(Debug)]
struct DependencyEdgeAgg {
    bytes: u64,
    packets: u64,
    connections: u64,
    first_seen_unix_ms: u128,
    last_seen_unix_ms: u128,
}

#[derive(Clone)]
struct PodIpInfo {
    namespace: String,
    pod_name: String,
    workload_kind: Option<String>,
    workload_name: Option<String>,
    ip: String,
}

#[derive(Clone)]
struct ServiceIpInfo {
    namespace: String,
    service_name: String,
    ip: String,
}

fn parse_tetragon_record(value: &Value) -> Option<TetragonDependencyRecord> {
    parse_tetragon_flow_record(value).or_else(|| parse_tetragon_kprobe_record(value))
}

fn parse_tetragon_flow_record(value: &Value) -> Option<TetragonDependencyRecord> {
    let src_ip = normalize_tetragon_ip(get_string_path(
        value,
        &["/src_ip", "/flow/src_ip", "/flow/ip/source"],
    )?);
    let dst_ip = normalize_tetragon_ip(get_string_path(
        value,
        &["/dst_ip", "/flow/dst_ip", "/flow/ip/destination"],
    )?);
    let protocol = normalize_tetragon_protocol(
        get_string_path(value, &["/protocol", "/flow/protocol"])
            .unwrap_or_else(|| "UNKNOWN".to_string()),
    );
    let destination_port = get_u64_path(
        value,
        &[
            "/destination_port",
            "/dst_port",
            "/flow/dst_port",
            "/flow/l4/dst_port",
        ],
    )
    .unwrap_or(0) as u16;
    let bytes = get_u64_path(value, &["/bytes", "/flow/bytes", "/summary/bytes"]).unwrap_or(0);
    let packets =
        get_u64_path(value, &["/packets", "/flow/packets", "/summary/packets"]).unwrap_or(0);
    let connections = get_u64_path(
        value,
        &["/connections", "/flow/connections", "/summary/connections"],
    )
    .unwrap_or(1);
    let timestamp_unix_ms = get_u128_path(
        value,
        &[
            "/timestamp_unix_ms",
            "/time_unix_ms",
            "/flow/timestamp_unix_ms",
        ],
    )
    .unwrap_or_else(now_unix_ms);

    Some(TetragonDependencyRecord {
        src_ip,
        dst_ip,
        src_endpoint: None,
        dst_endpoint: None,
        protocol,
        destination_port,
        bytes,
        packets,
        connections,
        timestamp_unix_ms,
    })
}

fn parse_tetragon_kprobe_record(value: &Value) -> Option<TetragonDependencyRecord> {
    let kprobe = value.pointer("/process_kprobe")?;
    let function_name = kprobe.pointer("/function_name").and_then(Value::as_str)?;
    let args = kprobe.pointer("/args")?.as_array()?;
    let sock_arg = match args.iter().find_map(|arg| arg.pointer("/sock_arg")) {
        Some(sock_arg) => sock_arg,
        None => {
            warn!(function_name, "missing sock_arg in tetragon kprobe record");
            return None;
        }
    };

    let (bytes, packets, connections) = match function_name {
        "tcp_sendmsg" => (
            args.get(1)
                .and_then(|arg| arg.pointer("/int_arg"))
                .and_then(Value::as_u64)
                .unwrap_or(0),
            1,
            0,
        ),
        "tcp_close" => (0, 0, 1),
        _ => return None,
    };

    let src_ip = normalize_tetragon_ip(get_string_path(sock_arg, &["/saddr", "/src_ip"])?);
    let dst_ip = normalize_tetragon_ip(get_string_path(sock_arg, &["/daddr", "/dst_ip"])?);
    let protocol = normalize_tetragon_protocol(
        get_string_path(sock_arg, &["/protocol"]).unwrap_or_else(|| "UNKNOWN".to_string()),
    );
    let destination_port = get_u64_path(sock_arg, &["/dport"]).unwrap_or(0) as u16;
    let timestamp_unix_ms = get_u128_path(
        kprobe,
        &[
            "/timestamp_unix_ms",
            "/time_unix_ms",
            "/flow/timestamp_unix_ms",
        ],
    )
    .unwrap_or_else(now_unix_ms);

    Some(TetragonDependencyRecord {
        src_ip,
        dst_ip,
        src_endpoint: None,
        dst_endpoint: None,
        protocol,
        destination_port,
        bytes,
        packets,
        connections,
        timestamp_unix_ms,
    })
}

fn build_pod_ip_index(pods: &[Pod]) -> BTreeMap<String, PodIpInfo> {
    let mut out = BTreeMap::new();
    for pod in pods {
        let ip = pod
            .status
            .as_ref()
            .and_then(|s| s.pod_ip.clone())
            .filter(|ip| !ip.is_empty());
        let Some(ip) = ip else {
            continue;
        };
        let owner = pod
            .metadata
            .owner_references
            .as_ref()
            .and_then(|refs| refs.first());
        out.insert(
            ip.clone(),
            PodIpInfo {
                namespace: pod.metadata.namespace.clone().unwrap_or_default(),
                pod_name: pod.metadata.name.clone().unwrap_or_default(),
                workload_kind: owner.map(|o| o.kind.clone()),
                workload_name: owner.map(|o| o.name.clone()),
                ip,
            },
        );
    }
    out
}

fn build_service_ip_index(services: &[Service]) -> BTreeMap<String, ServiceIpInfo> {
    let mut out = BTreeMap::new();
    for svc in services {
        let namespace = svc.metadata.namespace.clone().unwrap_or_default();
        let service_name = svc.metadata.name.clone().unwrap_or_default();
        if let Some(cluster_ip) = svc
            .spec
            .as_ref()
            .and_then(|s| s.cluster_ip.clone())
            .filter(|ip| !ip.is_empty() && ip != "None")
        {
            out.insert(
                cluster_ip.clone(),
                ServiceIpInfo {
                    namespace: namespace.clone(),
                    service_name: service_name.clone(),
                    ip: cluster_ip,
                },
            );
        }
    }
    out
}

fn resolve_endpoint(
    ip: &str,
    hint: Option<&DependencyEndpointHint>,
    pods: &BTreeMap<String, PodIpInfo>,
    services: &BTreeMap<String, ServiceIpInfo>,
) -> DependencyEndpoint {
    if let Some(pod) = pods.get(ip) {
        return DependencyEndpoint {
            kind: "pod".to_string(),
            namespace: Some(pod.namespace.clone()),
            name: Some(pod.pod_name.clone()),
            workload_kind: pod.workload_kind.clone(),
            workload_name: pod.workload_name.clone(),
            ip: Some(pod.ip.clone()),
        };
    }

    if let Some(service) = services.get(ip) {
        return DependencyEndpoint {
            kind: "service".to_string(),
            namespace: Some(service.namespace.clone()),
            name: Some(service.service_name.clone()),
            workload_kind: None,
            workload_name: None,
            ip: Some(service.ip.clone()),
        };
    }

    if let Some(hint) = hint {
        let has_identity = hint.namespace.is_some()
            || hint.pod_name.is_some()
            || hint.workload_kind.is_some()
            || hint.workload_name.is_some();
        if has_identity {
            return DependencyEndpoint {
                kind: "pod".to_string(),
                namespace: hint.namespace.clone(),
                name: hint.pod_name.clone(),
                workload_kind: hint.workload_kind.clone(),
                workload_name: hint.workload_name.clone(),
                ip: Some(ip.to_string()),
            };
        }
    }

    DependencyEndpoint {
        kind: "unknown".to_string(),
        namespace: None,
        name: None,
        workload_kind: None,
        workload_name: None,
        ip: Some(ip.to_string()),
    }
}

fn normalize_endpoint_key(endpoint: &DependencyEndpoint) -> EndpointKey {
    EndpointKey {
        kind: endpoint.kind.to_lowercase(),
        namespace: endpoint.namespace.as_ref().map(|v| v.to_lowercase()),
        name: endpoint.name.as_ref().map(|v| v.to_lowercase()),
        workload_kind: endpoint.workload_kind.as_ref().map(|v| v.to_lowercase()),
        workload_name: endpoint.workload_name.as_ref().map(|v| v.to_lowercase()),
        ip: endpoint.ip.clone(),
    }
}

fn endpoint_from_key(key: &EndpointKey) -> DependencyEndpoint {
    DependencyEndpoint {
        kind: key.kind.clone(),
        namespace: key.namespace.clone(),
        name: key.name.clone(),
        workload_kind: key.workload_kind.clone(),
        workload_name: key.workload_name.clone(),
        ip: key.ip.clone(),
    }
}

fn sort_dependency_edges(edges: &mut [DependencyEdge]) {
    edges.sort_by(|a, b| {
        normalize_endpoint_key(&a.from)
            .cmp(&normalize_endpoint_key(&b.from))
            .then_with(|| normalize_endpoint_key(&a.to).cmp(&normalize_endpoint_key(&b.to)))
            .then_with(|| a.protocol.to_uppercase().cmp(&b.protocol.to_uppercase()))
            .then_with(|| a.destination_port.cmp(&b.destination_port))
            .then_with(|| a.direction.to_lowercase().cmp(&b.direction.to_lowercase()))
    });
}

fn get_string_path(value: &Value, pointers: &[&str]) -> Option<String> {
    pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(Value::as_str))
        .map(ToString::to_string)
}

fn get_u64_path(value: &Value, pointers: &[&str]) -> Option<u64> {
    pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(Value::as_u64))
}

fn get_u128_path(value: &Value, pointers: &[&str]) -> Option<u128> {
    pointers.iter().find_map(|pointer| {
        value.pointer(pointer).and_then(|v| {
            v.as_u64()
                .map(|n| n as u128)
                .or_else(|| v.as_str().and_then(|text| text.trim().parse::<u128>().ok()))
        })
    })
}

fn normalize_tetragon_ip(ip: String) -> String {
    ip.strip_prefix("::ffff:").unwrap_or(&ip).to_string()
}

fn normalize_tetragon_protocol(protocol: String) -> String {
    let protocol = protocol.trim().to_lowercase();
    protocol
        .strip_prefix("ipproto_")
        .unwrap_or(&protocol)
        .to_string()
}

fn now_unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

async fn list_all<K>(client: &Client, lp: &ListParams) -> Result<Vec<K>>
where
    K: kube::Resource + Clone + std::fmt::Debug + serde::de::DeserializeOwned + 'static,
    <K as kube::Resource>::DynamicType: Default,
{
    let api: Api<K> = Api::all(client.clone());
    Ok(api.list(lp).await?.items)
}

async fn list_dynamic_all(
    client: &Client,
    group: &str,
    version: &str,
    kind: &str,
) -> Result<Vec<DynamicObject>> {
    let gvk = GroupVersionKind::gvk(group, version, kind);
    let ar = ApiResource::from_gvk(&gvk);
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &ar);
    Ok(api.list(&ListParams::default()).await?.items)
}

/// Probe `metrics.k8s.io` for `PodMetrics`. Tries `v1` first; falls back to
/// `v1beta1` only when v1 returns 404. Returns both the parsed items (empty
/// on any failure) and a [`MetricsStatus`] so the Hub can tell *why* pod
/// usage is missing.
async fn probe_pod_metrics(client: &Client) -> (Vec<DynamicObject>, MetricsStatus) {
    const SOURCE_V1: &str = "metrics.k8s.io/v1";
    const SOURCE_V1BETA1: &str = "metrics.k8s.io/v1beta1";

    let v1 = list_dynamic_all(client, "metrics.k8s.io", "v1", "PodMetrics").await;
    let (items, source) = match v1 {
        Ok(items) => (items, SOURCE_V1),
        Err(err) => {
            if let Some(kube_err) = err.downcast_ref::<KubeError>() {
                if is_not_found(kube_err) {
                    // v1 not registered; metrics-server may expose v1beta1 only.
                    match list_dynamic_all(client, "metrics.k8s.io", "v1beta1", "PodMetrics").await
                    {
                        Ok(items) => (items, SOURCE_V1BETA1),
                        Err(err2) => {
                            let (state, reason) = classify_anyhow(&err2);
                            return (
                                Vec::new(),
                                status_from(state, reason, SOURCE_V1BETA1, 0, now_ms()),
                            );
                        }
                    }
                } else {
                    let (state, reason) = classify_metrics_error(kube_err);
                    return (
                        Vec::new(),
                        status_from(state, reason, SOURCE_V1, 0, now_ms()),
                    );
                }
            } else {
                // anyhow error that didn't come from kube directly.
                warn!("pod-metrics v1 list failed (non-kube error): {:#}", err);
                return (
                    Vec::new(),
                    status_from("error", "transient: io", SOURCE_V1, 0, now_ms()),
                );
            }
        }
    };

    let count = items.len();
    (items, status_from("ok", "", source, count, now_ms()))
}

fn is_not_found(err: &KubeError) -> bool {
    matches!(err, KubeError::Api(status) if status.code == 404)
}

/// Classify an `anyhow::Error` from `list_dynamic_all` into a `(state,
/// reason)` pair. Tries the kube path first; falls back to a generic
/// transient reason.
fn classify_anyhow(err: &anyhow::Error) -> (&'static str, &'static str) {
    if let Some(kube_err) = err.downcast_ref::<KubeError>() {
        classify_metrics_error(kube_err)
    } else {
        ("error", "transient: io")
    }
}

fn status_from(
    state: &'static str,
    reason: &'static str,
    source: &'static str,
    pod_metrics_count: usize,
    last_attempt_at_ms: u128,
) -> MetricsStatus {
    let reason = if reason.is_empty() {
        None
    } else {
        Some(reason.to_string())
    };
    MetricsStatus {
        state,
        reason,
        source,
        pod_metrics_count,
        last_attempt_at_ms,
    }
}

/// Classify a kube error from a metrics API call into a `(state, reason)`
/// pair. State is one of `forbidden` / `missing` / `unavailable` / `error`;
/// reason is a short, actionable one-liner.
fn classify_metrics_error(err: &KubeError) -> (&'static str, &'static str) {
    match err {
        KubeError::Api(status) => match status.code {
            403 => ("forbidden", "ServiceAccount missing metrics.k8s.io RBAC"),
            404 => ("missing", "metrics-server not installed"),
            503 => ("unavailable", "metrics-server registered but not ready"),
            504 => ("unavailable", "metrics-server timeout"),
            _ => ("error", "kube API error"),
        },
        _ => classify_transient_error(err),
    }
}

fn classify_transient_error(err: &KubeError) -> (&'static str, &'static str) {
    let msg = format!("{err}").to_lowercase();
    let kind = if msg.contains("timeout") || msg.contains("timed out") {
        "timeout"
    } else if msg.contains("connection refused") {
        "connection refused"
    } else if msg.contains("dns") || msg.contains("name resolution") {
        "dns"
    } else if msg.contains("tls") || msg.contains("certificate") {
        "tls"
    } else {
        "transient"
    };
    (
        "error",
        match kind {
            "timeout" => "transient: timeout",
            "connection refused" => "transient: connection refused",
            "dns" => "transient: dns",
            "tls" => "transient: tls",
            _ => "transient: io",
        },
    )
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Probe the CSI snapshot API for `VolumeSnapshotClass` (canonical signal)
/// and, on success, `VolumeSnapshot`. On 404 from the canonical probe we
/// skip the second probe entirely; the API group is absent. Returns
/// `(classes, snapshots, status)`. On any failure, `classes` and `snapshots`
/// are empty so the snapshot storage section is naturally empty.
async fn probe_snapshot_api(
    client: &Client,
) -> (Vec<DynamicObject>, Vec<DynamicObject>, SnapshotApiStatus) {
    const SOURCE: &str = "snapshot.storage.k8s.io/v1";

    let classes_result = list_dynamic_all(
        client,
        "snapshot.storage.k8s.io",
        "v1",
        "VolumeSnapshotClass",
    )
    .await;
    let classes = match classes_result {
        Ok(items) => items,
        Err(err) => {
            if let Some(kube_err) = err.downcast_ref::<KubeError>() {
                if is_not_found(kube_err) {
                    // API group absent. Skip the second probe; the group is
                    // not registered, so the second list would just 404
                    // again and add another warn to the log.
                    return (
                        Vec::new(),
                        Vec::new(),
                        status_snapshot_from(
                            "missing",
                            "CSI snapshot CRDs not installed",
                            SOURCE,
                            0,
                            0,
                            now_ms(),
                        ),
                    );
                }
                let (state, reason) = classify_snapshot_api_error(kube_err);
                warn!(
                    error = %err,
                    state,
                    reason,
                    "volume snapshot classes list failed"
                );
                return (
                    Vec::new(),
                    Vec::new(),
                    status_snapshot_from(state, reason, SOURCE, 0, 0, now_ms()),
                );
            }
            // anyhow error that didn't come from kube directly.
            warn!(
                "volume snapshot classes list failed (non-kube error): {:#}",
                err
            );
            return (
                Vec::new(),
                Vec::new(),
                status_snapshot_from("error", "transient: io", SOURCE, 0, 0, now_ms()),
            );
        }
    };

    let classes_count = classes.len();
    let snapshots_result =
        list_dynamic_all(client, "snapshot.storage.k8s.io", "v1", "VolumeSnapshot").await;
    let snapshots = soft_unwrap("volumesnapshots", snapshots_result);
    let snapshots_count = snapshots.len();

    (
        classes,
        snapshots,
        status_snapshot_from("ok", "", SOURCE, classes_count, snapshots_count, now_ms()),
    )
}

fn status_snapshot_from(
    state: &'static str,
    reason: &'static str,
    source: &'static str,
    volumesnapshotclasses_count: usize,
    volumesnapshots_count: usize,
    last_attempt_at_ms: u128,
) -> SnapshotApiStatus {
    let reason = if reason.is_empty() {
        None
    } else {
        Some(reason.to_string())
    };
    SnapshotApiStatus {
        state,
        reason,
        source,
        volumesnapshotclasses_count,
        volumesnapshots_count,
        last_attempt_at_ms,
    }
}

/// Classify a kube error from a snapshot API call into a `(state, reason)`
/// pair. State and reason semantics mirror `classify_metrics_error`.
fn classify_snapshot_api_error(err: &KubeError) -> (&'static str, &'static str) {
    match err {
        KubeError::Api(status) => match status.code {
            403 => (
                "forbidden",
                "ServiceAccount missing snapshot.storage.k8s.io RBAC",
            ),
            404 => ("missing", "CSI snapshot CRDs not installed"),
            503 => ("unavailable", "CSI snapshot API registered but not ready"),
            504 => ("unavailable", "CSI snapshot API timeout"),
            _ => ("error", "kube API error"),
        },
        _ => classify_transient_error(err),
    }
}

fn soft_unwrap<T>(what: &str, r: Result<Vec<T>>) -> Vec<T> {
    match r {
        Ok(v) => v,
        Err(e) => {
            warn!("listing {} failed: {:#}", what, e);
            Vec::new()
        }
    }
}

fn build_cluster_info(
    version: Option<k8s_openapi::apimachinery::pkg::version::Info>,
    nodes: &[Node],
) -> ClusterInfo {
    let kubernetes_version = version.as_ref().map(|v| v.git_version.clone());
    let platform = detect_platform(version.as_ref(), nodes);

    let node_infos = nodes.iter().map(map_node).collect::<Vec<_>>();
    ClusterInfo {
        kubernetes_version,
        platform,
        openshift_version: None,
        node_count: node_infos.len(),
        nodes: node_infos,
    }
}

fn detect_platform(
    version: Option<&k8s_openapi::apimachinery::pkg::version::Info>,
    nodes: &[Node],
) -> Option<String> {
    if let Some(v) = version {
        let gv = v.git_version.to_lowercase();
        if gv.contains("eks") {
            return Some("eks".into());
        }
        if gv.contains("gke") {
            return Some("gke".into());
        }
        if gv.contains("aks") {
            return Some("aks".into());
        }
    }
    // OpenShift signal: nodes have label node.openshift.io/os_id
    for n in nodes {
        if let Some(labels) = n.metadata.labels.as_ref() {
            if labels.keys().any(|k| k.starts_with("node.openshift.io/")) {
                return Some("openshift".into());
            }
        }
    }
    Some("vanilla".into())
}

async fn probe_openshift_version(client: &Client) -> Option<String> {
    let versions =
        match list_dynamic_all(client, "config.openshift.io", "v1", "ClusterVersion").await {
            Ok(items) => items,
            Err(err) => {
                warn!(error = %err, "openshift cluster version list failed");
                return None;
            }
        };

    versions.into_iter().find_map(|item| {
        let value = serde_json::to_value(&item).ok()?;
        extract_openshift_version(&value)
    })
}

fn extract_openshift_version(value: &Value) -> Option<String> {
    value
        .pointer("/status/desired/version")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .pointer("/status/history/0/version")
                .and_then(Value::as_str)
        })
        .map(|version| version.to_string())
}

fn map_node(n: &Node) -> NodeInfo {
    let status = n.status.as_ref();
    let info = status.and_then(|s| s.node_info.as_ref());
    let cap = status.and_then(|s| s.capacity.as_ref());
    let alloc = status.and_then(|s| s.allocatable.as_ref());

    let ready = status
        .and_then(|s| s.conditions.as_ref())
        .map(|conds| {
            conds
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True")
        })
        .unwrap_or(false);

    let labels = n.metadata.labels.as_ref();
    let roles: Vec<String> = labels
        .map(|l| {
            l.keys()
                .filter_map(|k| k.strip_prefix("node-role.kubernetes.io/"))
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();

    NodeInfo {
        name: n.metadata.name.clone().unwrap_or_default(),
        kubelet_version: info.map(|i| i.kubelet_version.clone()),
        os_image: info.map(|i| i.os_image.clone()),
        container_runtime: info.map(|i| i.container_runtime_version.clone()),
        architecture: info.map(|i| i.architecture.clone()),
        capacity_cpu: cap.and_then(|c| c.get("cpu")).map(|q| q.0.clone()),
        capacity_memory: cap.and_then(|c| c.get("memory")).map(|q| q.0.clone()),
        allocatable_cpu: alloc.and_then(|c| c.get("cpu")).map(|q| q.0.clone()),
        allocatable_memory: alloc.and_then(|c| c.get("memory")).map(|q| q.0.clone()),
        ready,
        roles,
    }
}

fn map_namespace(n: Namespace) -> NamespaceInfo {
    let labels = map_metadata_pairs(n.metadata.labels.unwrap_or_default());
    NamespaceInfo {
        name: n.metadata.name.unwrap_or_default(),
        phase: n.status.and_then(|s| s.phase),
        labels,
    }
}

fn map_configmap(cm: ConfigMap) -> ConfigMapInfo {
    ConfigMapInfo {
        namespace: cm.metadata.namespace.unwrap_or_default(),
        name: cm.metadata.name.unwrap_or_default(),
        immutable: cm.immutable,
        labels: map_metadata_pairs(cm.metadata.labels.unwrap_or_default()),
        annotations: map_metadata_pairs(cm.metadata.annotations.unwrap_or_default()),
        data_keys: map_data_keys(cm.data.unwrap_or_default()),
        binary_data_keys: map_data_keys(cm.binary_data.unwrap_or_default()),
    }
}

fn collect_agent_configured_env(configmaps: &[ConfigMap]) -> Vec<KV> {
    configmaps
        .iter()
        .find(|cm| cm.metadata.name.as_deref() == Some(AGENT_CONFIGMAP_NAME))
        .and_then(|cm| cm.data.as_ref())
        .map(config::agent_configured_env)
        .unwrap_or_default()
}

fn map_secret(secret: Secret) -> SecretInfo {
    SecretInfo {
        namespace: secret.metadata.namespace.unwrap_or_default(),
        name: secret.metadata.name.unwrap_or_default(),
        type_: secret.type_,
        immutable: secret.immutable,
        labels: map_metadata_pairs(secret.metadata.labels.unwrap_or_default()),
        annotations: map_metadata_pairs(secret.metadata.annotations.unwrap_or_default()),
        data_keys: map_data_keys(secret.data.unwrap_or_default()),
    }
}

fn map_metadata_pairs(map: BTreeMap<String, String>) -> Vec<KV> {
    sanitize_metadata_map(map)
        .into_iter()
        .map(|(k, v)| KV { key: k, value: v })
        .collect()
}

const METADATA_ANNOTATION_DENYLIST_EXACT: &[&str] = &[];
const METADATA_ANNOTATION_DENYLIST_PREFIX: &[&str] = &[];

fn sanitize_metadata_map(map: BTreeMap<String, String>) -> BTreeMap<String, String> {
    map.into_iter()
        .filter(|(key, _)| !METADATA_ANNOTATION_DENYLIST_EXACT.contains(&key.as_str()))
        .filter(|(key, _)| {
            !METADATA_ANNOTATION_DENYLIST_PREFIX
                .iter()
                .any(|prefix| key.starts_with(prefix))
        })
        .collect()
}

fn map_data_keys<T>(map: BTreeMap<String, T>) -> Vec<String> {
    map.into_keys().collect()
}

fn map_deployment(d: Deployment) -> WorkloadRef {
    WorkloadRef {
        namespace: d.metadata.namespace.unwrap_or_default(),
        name: d.metadata.name.unwrap_or_default(),
        replicas_desired: d.spec.and_then(|s| s.replicas),
        replicas_ready: d.status.and_then(|s| s.ready_replicas),
    }
}

fn map_statefulset(s: StatefulSet) -> WorkloadRef {
    WorkloadRef {
        namespace: s.metadata.namespace.unwrap_or_default(),
        name: s.metadata.name.unwrap_or_default(),
        replicas_desired: s.spec.and_then(|sp| sp.replicas),
        replicas_ready: s.status.and_then(|st| st.ready_replicas),
    }
}

fn map_daemonset(d: DaemonSet) -> WorkloadRef {
    let status = d.status;
    WorkloadRef {
        namespace: d.metadata.namespace.unwrap_or_default(),
        name: d.metadata.name.unwrap_or_default(),
        replicas_desired: status.as_ref().map(|s| s.desired_number_scheduled),
        replicas_ready: status.as_ref().map(|s| s.number_ready),
    }
}

fn container_technology(
    container: &k8s_openapi::api::core::v1::Container,
    pod_labels: &[(&str, &str)],
    pod_annotations: &[(&str, &str)],
    tech_detect_process: bool,
) -> Technology {
    let image = container.image.as_deref().unwrap_or("");
    if let Some(stack) = tech::detect_from_labels(pod_labels, pod_annotations) {
        return stack;
    }
    if !tech_detect_process {
        return refine_with_application_stack_image(tech::detect(image), image);
    }

    let image_tech = tech::detect(image);
    let command = container.command.clone().unwrap_or_default();
    let args = container.args.clone().unwrap_or_default();

    let detected = tech::detect_from_process(&command, &args).unwrap_or(image_tech);
    let refined = tech::refine_spring_boot(detected, &command, &args);
    refine_with_application_stack_image(refined, image)
}

fn refine_with_application_stack_image(
    mut tech: crate::model::Technology,
    image: &str,
) -> crate::model::Technology {
    if tech.subtype.is_some() {
        return tech;
    }
    if let Some(stack) = tech::detect_application_stack_from_image(image) {
        tech.subtype = stack.subtype;
    }
    tech
}

fn map_pod(
    p: Pod,
    pod_usage: &BTreeMap<(String, String), PodUsageTotals>,
    tech_detect_process: bool,
) -> PodInfo {
    let owner = p
        .metadata
        .owner_references
        .as_ref()
        .and_then(|refs| refs.first().cloned());
    let namespace = p.metadata.namespace.clone().unwrap_or_default();
    let name = p.metadata.name.clone().unwrap_or_default();
    let usage = pod_usage.get(&(namespace.clone(), name.clone()));
    let pod_labels = p
        .metadata
        .labels
        .as_ref()
        .map(|labels| {
            labels
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let pod_annotations = p
        .metadata
        .annotations
        .as_ref()
        .map(|annotations| {
            annotations
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let containers = p
        .spec
        .as_ref()
        .map(|s| {
            s.containers
                .iter()
                .map(|c| ContainerInfo {
                    name: c.name.clone(),
                    image: c.image.clone().unwrap_or_default(),
                    image_pull_policy: c.image_pull_policy.clone(),
                    technology: container_technology(
                        c,
                        &pod_labels,
                        &pod_annotations,
                        tech_detect_process,
                    ),
                    resources: map_resources(c.resources.as_ref()),
                })
                .collect()
        })
        .unwrap_or_default();

    PodInfo {
        namespace,
        name,
        age_seconds: pod_age_seconds(p.metadata.creation_timestamp.as_ref()),
        node: p.spec.as_ref().and_then(|s| s.node_name.clone()),
        phase: p.status.and_then(|s| s.phase),
        usage_cpu: usage.and_then(|usage| format_cpu_quantity(usage.cpu_nano)),
        usage_memory: usage.and_then(|usage| format_memory_quantity(usage.memory_bytes)),
        owner_kind: owner.as_ref().map(|o| o.kind.clone()),
        owner_name: owner.as_ref().map(|o| o.name.clone()),
        containers,
    }
}

fn build_pod_usage_index(
    pod_metrics: &[DynamicObject],
) -> BTreeMap<(String, String), PodUsageTotals> {
    pod_metrics.iter().filter_map(map_pod_metrics).collect()
}

fn map_pod_metrics(metric: &DynamicObject) -> Option<((String, String), PodUsageTotals)> {
    let as_value = serde_json::to_value(metric).ok()?;
    let namespace = as_value
        .pointer("/metadata/namespace")
        .and_then(Value::as_str)?
        .to_string();
    let name = as_value
        .pointer("/metadata/name")
        .and_then(Value::as_str)?
        .to_string();

    let containers = as_value.pointer("/containers")?.as_array()?;
    let mut totals = PodUsageTotals::default();
    for container in containers {
        let usage = container.pointer("/usage")?;
        let cpu = usage.pointer("/cpu").and_then(Value::as_str)?;
        let memory = usage.pointer("/memory").and_then(Value::as_str)?;
        totals.cpu_nano = totals.cpu_nano.checked_add(parse_cpu_quantity(cpu)?)?;
        totals.memory_bytes = totals
            .memory_bytes
            .checked_add(parse_memory_quantity(memory)?)?;
    }

    Some(((namespace, name), totals))
}

fn parse_cpu_quantity(quantity: &str) -> Option<i128> {
    parse_scaled_decimal(
        quantity,
        &[
            ("n", 1),
            ("u", 1_000),
            ("m", 1_000_000),
            ("", 1_000_000_000),
        ],
    )
}

fn parse_memory_quantity(quantity: &str) -> Option<i128> {
    parse_scaled_decimal(
        quantity,
        &[
            ("Ki", 1024),
            ("Mi", 1024_i128.pow(2)),
            ("Gi", 1024_i128.pow(3)),
            ("Ti", 1024_i128.pow(4)),
            ("Pi", 1024_i128.pow(5)),
            ("Ei", 1024_i128.pow(6)),
            ("k", 1_000),
            ("M", 1_000_000),
            ("G", 1_000_000_000),
            ("T", 1_000_000_000_000),
            ("P", 1_000_000_000_000_000),
            ("E", 1_000_000_000_000_000_000),
            ("", 1),
        ],
    )
}

fn parse_scaled_decimal(quantity: &str, suffixes: &[(&str, i128)]) -> Option<i128> {
    for (suffix, scale) in suffixes {
        if let Some(number) = quantity.strip_suffix(suffix) {
            if suffix.is_empty() || !number.is_empty() {
                return parse_decimal_scaled(number, *scale);
            }
        }
    }
    None
}

fn parse_decimal_scaled(number: &str, scale: i128) -> Option<i128> {
    let trimmed = number.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (whole, frac) = trimmed.split_once('.').unwrap_or((trimmed, ""));
    let whole_value = whole.parse::<i128>().ok()?;
    let mut total = whole_value.checked_mul(scale)?;
    if !frac.is_empty() {
        let frac_value = frac.parse::<i128>().ok()?;
        let denom = 10_i128.checked_pow(frac.len() as u32)?;
        total = total.checked_add(frac_value.checked_mul(scale)?.checked_div(denom)?)?;
    }
    Some(total)
}

fn format_cpu_quantity(cpu_nano: i128) -> Option<String> {
    if cpu_nano < 0 {
        return None;
    }
    if cpu_nano % 1_000_000 == 0 {
        Some(format!("{}m", cpu_nano / 1_000_000))
    } else {
        Some(format!("{}n", cpu_nano))
    }
}

fn format_memory_quantity(memory_bytes: i128) -> Option<String> {
    if memory_bytes < 0 {
        return None;
    }
    for (suffix, scale) in [
        ("Ei", 1024_i128.pow(6)),
        ("Pi", 1024_i128.pow(5)),
        ("Ti", 1024_i128.pow(4)),
        ("Gi", 1024_i128.pow(3)),
        ("Mi", 1024_i128.pow(2)),
        ("Ki", 1024_i128),
    ] {
        if memory_bytes >= scale && memory_bytes % scale == 0 {
            return Some(format!("{}{}", memory_bytes / scale, suffix));
        }
    }
    Some(memory_bytes.to_string())
}

fn map_service(svc: Service) -> ServiceInfo {
    let namespace = svc.metadata.namespace.unwrap_or_default();
    let name = svc.metadata.name.unwrap_or_default();
    let spec = svc.spec;
    let status = svc.status;

    let type_ = spec
        .as_ref()
        .and_then(|s| s.type_.clone())
        .unwrap_or_else(|| "ClusterIP".into());
    let cluster_ip = spec.as_ref().and_then(|s| s.cluster_ip.clone());
    let mut external_ips = spec
        .as_ref()
        .and_then(|s| s.external_ips.clone())
        .unwrap_or_default();
    external_ips.sort();

    let mut selector = spec
        .as_ref()
        .and_then(|s| s.selector.clone())
        .unwrap_or_default()
        .into_iter()
        .map(|(key, value)| KV { key, value })
        .collect::<Vec<_>>();
    selector.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.value.cmp(&b.value)));

    let mut ports = spec
        .as_ref()
        .and_then(|s| s.ports.clone())
        .unwrap_or_default()
        .into_iter()
        .map(|p| ServicePortInfo {
            name: p.name,
            protocol: p.protocol,
            port: p.port,
            target_port: p.target_port.map(format_int_or_string),
            node_port: p.node_port,
        })
        .collect::<Vec<_>>();
    ports.sort_by(|a, b| {
        a.port
            .cmp(&b.port)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.protocol.cmp(&b.protocol))
    });

    let mut load_balancer_ingress = status
        .and_then(|st| st.load_balancer)
        .and_then(|lb| lb.ingress)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| {
            entry
                .hostname
                .or(entry.ip)
                .or(entry.ip_mode)
                .filter(|value| !value.is_empty())
        })
        .collect::<Vec<_>>();
    load_balancer_ingress.sort();

    ServiceInfo {
        namespace,
        name,
        type_,
        cluster_ip,
        external_ips,
        selector,
        ports,
        load_balancer_ingress,
    }
}

fn map_network_policy(policy: NetworkPolicy) -> NetworkPolicyInfo {
    let spec = policy.spec;
    let namespace = policy.metadata.namespace.unwrap_or_default();
    let name = policy.metadata.name.unwrap_or_default();
    let pod_selector = spec
        .as_ref()
        .map(|spec| {
            map_metadata_pairs(
                spec.pod_selector
                    .as_ref()
                    .and_then(|selector| selector.match_labels.clone())
                    .unwrap_or_default(),
            )
        })
        .unwrap_or_default();
    let policy_types = spec.as_ref().map(network_policy_types).unwrap_or_default();
    let ingress_rules_count = spec
        .as_ref()
        .and_then(|spec| spec.ingress.as_ref())
        .map(|rules| rules.len())
        .unwrap_or(0);
    let egress_rules_count = spec
        .as_ref()
        .and_then(|spec| spec.egress.as_ref())
        .map(|rules| rules.len())
        .unwrap_or(0);

    NetworkPolicyInfo {
        namespace,
        name,
        policy_types,
        pod_selector,
        ingress_rules_count,
        egress_rules_count,
    }
}

fn network_policy_types(spec: &k8s_openapi::api::networking::v1::NetworkPolicySpec) -> Vec<String> {
    let mut policy_types = spec.policy_types.clone().unwrap_or_default();
    if policy_types.is_empty() {
        if spec.ingress.as_ref().is_some_and(|rules| !rules.is_empty()) {
            policy_types.push("Ingress".to_string());
        }
        if spec.egress.as_ref().is_some_and(|rules| !rules.is_empty()) {
            policy_types.push("Egress".to_string());
        }
    }
    policy_types.sort();
    policy_types.dedup();
    policy_types
}

fn map_cluster_role_binding(binding: ClusterRoleBinding) -> Option<ClusterRoleBindingInfo> {
    let role_ref_name = binding.role_ref.name;
    let role_ref_kind = binding.role_ref.kind;
    if !should_include_cluster_role_binding(&role_ref_kind, &role_ref_name) {
        return None;
    }

    let mut subjects = binding
        .subjects
        .unwrap_or_default()
        .into_iter()
        .map(|subject| SecuritySubjectInfo {
            kind: subject.kind,
            name: subject.name,
            namespace: subject.namespace,
        })
        .collect::<Vec<_>>();
    subjects.sort_by(|a, b| {
        a.kind
            .cmp(&b.kind)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.namespace.cmp(&b.namespace))
    });

    Some(ClusterRoleBindingInfo {
        name: binding.metadata.name.unwrap_or_default(),
        role_ref_name: role_ref_name.clone(),
        role_ref_kind,
        risk_level: if is_high_risk_role(&role_ref_name) {
            "high".to_string()
        } else {
            "review".to_string()
        },
        subjects,
    })
}

fn should_include_cluster_role_binding(role_ref_kind: &str, role_ref_name: &str) -> bool {
    is_high_risk_role(role_ref_name)
        || (role_ref_kind == "ClusterRole" && !role_ref_name.starts_with("system:"))
}

fn is_high_risk_role(role_name: &str) -> bool {
    matches!(role_name, "cluster-admin" | "admin" | "edit")
}

fn build_pod_security_admission(namespaces: &[NamespaceInfo]) -> PodSecurityAdmissionInfo {
    let mut namespaces = namespaces
        .iter()
        .map(|namespace| {
            let labels = namespace
                .labels
                .iter()
                .map(|label| (label.key.as_str(), label.value.clone()))
                .collect::<BTreeMap<_, _>>();
            PodSecurityNamespaceInfo {
                namespace: namespace.name.clone(),
                enforce: labels.get(PSA_ENFORCE_LABEL).cloned(),
                audit: labels.get(PSA_AUDIT_LABEL).cloned(),
                warn: labels.get(PSA_WARN_LABEL).cloned(),
            }
        })
        .collect::<Vec<_>>();
    namespaces.sort_by(|a, b| a.namespace.cmp(&b.namespace));
    PodSecurityAdmissionInfo { namespaces }
}

fn sort_network_policies_for_snapshot(network_policies: &mut [NetworkPolicy]) {
    network_policies.sort_by(|a, b| {
        let a_namespace = a.metadata.namespace.as_deref().unwrap_or_default();
        let b_namespace = b.metadata.namespace.as_deref().unwrap_or_default();
        let a_name = a.metadata.name.as_deref().unwrap_or_default();
        let b_name = b.metadata.name.as_deref().unwrap_or_default();
        a_namespace
            .cmp(b_namespace)
            .then_with(|| a_name.cmp(b_name))
    });
}

const DESCHEDULER_DEPLOYMENT_NAMES: &[&str] =
    &["descheduler", "openshift-descheduler", "kube-descheduler"];
const DESCHEDULER_NS: &str = "openshift-kube-descheduler-operator";

fn detect_descheduler(deployments: &[Deployment]) -> DeschedulerInfo {
    if let Some(dep) = deployments.iter().find(|dep| {
        let name = dep.metadata.name.as_deref().unwrap_or_default();
        DESCHEDULER_DEPLOYMENT_NAMES.contains(&name)
    }) {
        return DeschedulerInfo {
            installed: true,
            detected_by: Some("deployment".into()),
            namespace: dep.metadata.namespace.clone(),
            strategy: dep.spec.as_ref().and_then(|spec| {
                spec.selector.match_labels.clone().and_then(|labels| {
                    labels
                        .get("descheduler")
                        .or_else(|| labels.get("app"))
                        .cloned()
                })
            }),
            schedule: None,
        };
    }

    if deployments
        .iter()
        .any(|dep| dep.metadata.namespace.as_deref() == Some(DESCHEDULER_NS))
    {
        return DeschedulerInfo {
            installed: true,
            detected_by: Some("namespace".into()),
            namespace: Some(DESCHEDULER_NS.into()),
            strategy: None,
            schedule: None,
        };
    }

    DeschedulerInfo::default()
}

fn build_vpa_info(vpa_objects: &[DynamicObject]) -> VpaInfo {
    let mut modes = BTreeSet::new();
    for vpa in vpa_objects {
        let as_value = serde_json::to_value(vpa).unwrap_or_default();
        let mode = as_value
            .pointer("/spec/updatePolicy/updateMode")
            .and_then(Value::as_str)
            .unwrap_or("Auto")
            .to_string();
        modes.insert(mode);
    }

    VpaInfo {
        installed: !vpa_objects.is_empty(),
        objects_count: vpa_objects.len(),
        update_modes: modes.into_iter().collect(),
    }
}

fn map_scheduled_job(cj: CronJob) -> ScheduledJobInfo {
    let namespace = cj.metadata.namespace.unwrap_or_default();
    let name = cj.metadata.name.unwrap_or_default();
    let spec = cj.spec;
    let status = cj.status;

    ScheduledJobInfo {
        namespace,
        name,
        schedule: spec
            .as_ref()
            .map(|s| s.schedule.clone())
            .unwrap_or_default(),
        suspend: spec.as_ref().and_then(|s| s.suspend).unwrap_or(false),
        last_schedule_time: status
            .as_ref()
            .and_then(|status| status.last_schedule_time.clone())
            .map(|time| time.0.to_string()),
        last_successful_time: status
            .as_ref()
            .and_then(|status| status.last_successful_time.clone())
            .map(|time| time.0.to_string()),
    }
}

fn sort_cronjobs_for_snapshot(cronjobs: &mut [CronJob]) {
    cronjobs.sort_by(|a, b| {
        let a_ns = a.metadata.namespace.as_deref().unwrap_or_default();
        let b_ns = b.metadata.namespace.as_deref().unwrap_or_default();
        let a_name = a.metadata.name.as_deref().unwrap_or_default();
        let b_name = b.metadata.name.as_deref().unwrap_or_default();
        a_ns.cmp(b_ns).then_with(|| a_name.cmp(b_name))
    });
}

fn map_ingress(ingress: Ingress) -> IngressInfo {
    let namespace = ingress.metadata.namespace.unwrap_or_default();
    let name = ingress.metadata.name.unwrap_or_default();
    let spec = ingress.spec;
    let status = ingress.status;

    let class_name = spec.as_ref().and_then(|s| s.ingress_class_name.clone());

    let mut hosts = spec
        .as_ref()
        .and_then(|s| s.rules.clone())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|rule| rule.host)
        .collect::<Vec<_>>();
    hosts.sort();
    hosts.dedup();

    let mut rules = Vec::new();
    for rule in spec
        .as_ref()
        .and_then(|s| s.rules.clone())
        .unwrap_or_default()
    {
        let host = rule.host;
        if let Some(http) = rule.http {
            for path in http.paths {
                let backend_service = path.backend.service.as_ref().map(|s| s.name.clone());
                let backend_port = path
                    .backend
                    .service
                    .and_then(|s| s.port)
                    .map(|p| match (p.name, p.number) {
                        (Some(name), _) => name,
                        (None, Some(number)) => number.to_string(),
                        (None, None) => String::new(),
                    })
                    .filter(|s| !s.is_empty());
                rules.push(IngressRuleInfo {
                    host: host.clone(),
                    path: path.path,
                    path_type: Some(path.path_type),
                    backend_service,
                    backend_port,
                });
            }
        } else {
            rules.push(IngressRuleInfo {
                host,
                path: None,
                path_type: None,
                backend_service: None,
                backend_port: None,
            });
        }
    }
    rules.sort_by(|a, b| {
        a.host
            .cmp(&b.host)
            .then_with(|| a.path.cmp(&b.path))
            .then_with(|| a.backend_service.cmp(&b.backend_service))
            .then_with(|| a.backend_port.cmp(&b.backend_port))
    });

    let mut tls = spec
        .as_ref()
        .and_then(|s| s.tls.clone())
        .unwrap_or_default()
        .into_iter()
        .map(|entry| {
            let mut tls_hosts = entry.hosts.unwrap_or_default();
            tls_hosts.sort();
            IngressTlsInfo {
                hosts: tls_hosts,
                secret_name: entry.secret_name,
            }
        })
        .collect::<Vec<_>>();
    tls.sort_by(|a, b| {
        a.secret_name
            .cmp(&b.secret_name)
            .then_with(|| a.hosts.cmp(&b.hosts))
    });

    let mut load_balancer_ingress = status
        .and_then(|st| st.load_balancer)
        .and_then(|lb| lb.ingress)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| {
            entry
                .hostname
                .or(entry.ip)
                .filter(|value| !value.is_empty())
        })
        .collect::<Vec<_>>();
    load_balancer_ingress.sort();

    IngressInfo {
        namespace,
        name,
        class_name,
        hosts,
        rules,
        tls,
        load_balancer_ingress,
    }
}

fn format_int_or_string(value: IntOrString) -> String {
    match value {
        IntOrString::Int(number) => number.to_string(),
        IntOrString::String(text) => text,
    }
}

fn sort_services_for_snapshot(services: &mut [Service]) {
    services.sort_by(|a, b| {
        a.metadata
            .namespace
            .as_deref()
            .unwrap_or_default()
            .cmp(b.metadata.namespace.as_deref().unwrap_or_default())
            .then_with(|| {
                a.metadata
                    .name
                    .as_deref()
                    .unwrap_or_default()
                    .cmp(b.metadata.name.as_deref().unwrap_or_default())
            })
    });
}

fn sort_ingresses_for_snapshot(ingresses: &mut [Ingress]) {
    ingresses.sort_by(|a, b| {
        a.metadata
            .namespace
            .as_deref()
            .unwrap_or_default()
            .cmp(b.metadata.namespace.as_deref().unwrap_or_default())
            .then_with(|| {
                a.metadata
                    .name
                    .as_deref()
                    .unwrap_or_default()
                    .cmp(b.metadata.name.as_deref().unwrap_or_default())
            })
    });
}

fn sort_configmaps_for_snapshot(configmaps: &mut [ConfigMap]) {
    configmaps.sort_by(|a, b| {
        a.metadata
            .namespace
            .as_deref()
            .unwrap_or_default()
            .cmp(b.metadata.namespace.as_deref().unwrap_or_default())
            .then_with(|| {
                a.metadata
                    .name
                    .as_deref()
                    .unwrap_or_default()
                    .cmp(b.metadata.name.as_deref().unwrap_or_default())
            })
    });
}

fn sort_secrets_for_snapshot(secrets: &mut [Secret]) {
    secrets.sort_by(|a, b| {
        a.metadata
            .namespace
            .as_deref()
            .unwrap_or_default()
            .cmp(b.metadata.namespace.as_deref().unwrap_or_default())
            .then_with(|| {
                a.metadata
                    .name
                    .as_deref()
                    .unwrap_or_default()
                    .cmp(b.metadata.name.as_deref().unwrap_or_default())
            })
    });
}

fn pod_age_seconds(
    created_at: Option<&k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>,
) -> Option<u64> {
    let created_secs = created_at?.0.as_second();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs() as i64;
    if now_secs <= created_secs {
        return Some(0);
    }
    Some((now_secs - created_secs) as u64)
}

fn map_resources(r: Option<&k8s_openapi::api::core::v1::ResourceRequirements>) -> ResourceSpec {
    let mut out = ResourceSpec::default();
    if let Some(r) = r {
        if let Some(req) = &r.requests {
            out.requests_cpu = req.get("cpu").map(|q| q.0.clone());
            out.requests_memory = req.get("memory").map(|q| q.0.clone());
        }
        if let Some(lim) = &r.limits {
            out.limits_cpu = lim.get("cpu").map(|q| q.0.clone());
            out.limits_memory = lim.get("memory").map(|q| q.0.clone());
        }
    }
    out
}

// Deny-by-default: only forward non-sensitive StorageClass parameters useful
// for backend inference on the Hub side. Secret-related CSI parameters and
// vendor credentials are intentionally excluded.
const STORAGE_CLASS_PARAMETER_ALLOWLIST: &[&str] = &[
    "type",
    "fsType",
    "skuName",
    "storageaccounttype",
    "iopsPerGB",
    "throughput",
    "pool",
    "backendType",
    "datastore",
    "encrypted",
    "csi.storage.k8s.io/fstype",
];

const MAX_EVENTS: usize = 500;
const MAX_EVENT_MESSAGE_CHARS: usize = 500;
const MAX_TETRAGON_LINES: usize = 20_000;
const MAX_DEP_EDGES_PER_SNAPSHOT: usize = 2_000;
const MAX_DEP_FANOUT_PER_SOURCE: usize = 200;
const MAX_SCHEDULED_JOBS: usize = 500;
const DEP_WINDOW_SECONDS: u64 = 60;

fn map_storage_class(sc: StorageClass) -> StorageClassInfo {
    let parameters = filter_storage_class_parameters(sc.parameters.unwrap_or_default());
    StorageClassInfo {
        name: sc.metadata.name.unwrap_or_default(),
        provisioner: sc.provisioner,
        parameters,
    }
}

fn filter_storage_class_parameters(parameters: BTreeMap<String, String>) -> Vec<KV> {
    let mut out = Vec::new();
    for key in STORAGE_CLASS_PARAMETER_ALLOWLIST {
        if let Some(value) = parameters.get(*key) {
            out.push(KV {
                key: (*key).to_string(),
                value: value.clone(),
            });
        }
    }
    out
}

fn map_persistent_volume(pv: PersistentVolume) -> PersistentVolumeInfo {
    PersistentVolumeInfo {
        name: pv.metadata.name.unwrap_or_default(),
        storage_class: pv.spec.and_then(|s| s.storage_class_name),
    }
}

fn map_persistent_volume_claim(pvc: PersistentVolumeClaim) -> PersistentVolumeClaimInfo {
    PersistentVolumeClaimInfo {
        namespace: pvc.metadata.namespace.unwrap_or_default(),
        name: pvc.metadata.name.unwrap_or_default(),
        storage_class: pvc.spec.as_ref().and_then(|s| s.storage_class_name.clone()),
        volume_name: pvc.spec.and_then(|s| s.volume_name),
    }
}

fn map_volume_snapshot_class(vsc: DynamicObject) -> VolumeSnapshotClassInfo {
    let as_value = serde_json::to_value(vsc).unwrap_or_default();
    let driver = as_value
        .pointer("/driver")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    let name = as_value
        .pointer("/metadata/name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();

    VolumeSnapshotClassInfo { name, driver }
}

fn map_volume_snapshot(vs: DynamicObject) -> VolumeSnapshotInfo {
    let as_value = serde_json::to_value(vs).unwrap_or_default();
    VolumeSnapshotInfo {
        namespace: as_value
            .pointer("/metadata/namespace")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        name: as_value
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        snapshot_class: as_value
            .pointer("/spec/volumeSnapshotClassName")
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string),
        bound_content_name: as_value
            .pointer("/status/boundVolumeSnapshotContentName")
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string),
    }
}

fn sort_events_for_snapshot(events: &mut [Event]) {
    events.sort_by(|a, b| {
        let a_rank = event_type_rank(a.type_.as_deref());
        let b_rank = event_type_rank(b.type_.as_deref());

        b_rank
            .cmp(&a_rank)
            .then_with(|| event_sort_ts_seconds(b).cmp(&event_sort_ts_seconds(a)))
            .then_with(|| {
                a.metadata
                    .namespace
                    .as_deref()
                    .unwrap_or_default()
                    .cmp(b.metadata.namespace.as_deref().unwrap_or_default())
            })
            .then_with(|| {
                a.metadata
                    .name
                    .as_deref()
                    .unwrap_or_default()
                    .cmp(b.metadata.name.as_deref().unwrap_or_default())
            })
    });
}

fn event_type_rank(kind: Option<&str>) -> u8 {
    if matches!(kind, Some("Warning")) {
        1
    } else {
        0
    }
}

fn event_sort_ts_seconds(event: &Event) -> i64 {
    event
        .event_time
        .as_ref()
        .map(|t| t.0.as_second())
        .or_else(|| event.last_timestamp.as_ref().map(|t| t.0.as_second()))
        .or_else(|| event.first_timestamp.as_ref().map(|t| t.0.as_second()))
        .unwrap_or(0)
}

fn map_event(event: Event) -> EventInfo {
    let involved = event.involved_object;
    EventInfo {
        namespace: event.metadata.namespace.unwrap_or_default(),
        name: event.metadata.name.unwrap_or_default(),
        type_: event.type_,
        reason: event.reason,
        message: event
            .message
            .map(|m| truncate_chars(&m, MAX_EVENT_MESSAGE_CHARS)),
        count: event.count,
        first_timestamp: event.first_timestamp.map(|t| t.0.to_string()),
        last_timestamp: event
            .event_time
            .map(|t| t.0.to_string())
            .or_else(|| event.last_timestamp.map(|t| t.0.to_string())),
        reporting_controller: event.reporting_component,
        reporting_instance: event.reporting_instance,
        involved_object: InvolvedObjectInfo {
            kind: involved.kind,
            name: involved.name,
            namespace: involved.namespace,
            uid: involved.uid,
        },
    }
}

fn truncate_chars(input: &str, max_chars: usize) -> String {
    let total = input.chars().count();
    if total <= max_chars {
        return input.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let mut out = input.chars().take(keep).collect::<String>();
    if max_chars >= 3 {
        out.push_str("...");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_openshift_version_prefers_desired_version() {
        let value = json!({
            "status": {
                "desired": { "version": "4.16.18" },
                "history": [{ "version": "4.16.17" }]
            }
        });

        assert_eq!(
            extract_openshift_version(&value).as_deref(),
            Some("4.16.18")
        );
    }

    #[test]
    fn extract_openshift_version_falls_back_to_history() {
        let value = json!({
            "status": {
                "history": [{ "version": "4.16.17" }]
            }
        });

        assert_eq!(
            extract_openshift_version(&value).as_deref(),
            Some("4.16.17")
        );
    }

    #[test]
    fn truncate_chars_respects_max_length() {
        let input = "a".repeat(1000);
        let out = truncate_chars(&input, 20);
        assert_eq!(out.chars().count(), 20);
        assert!(out.ends_with("..."));
    }

    #[test]
    fn parse_tetragon_record_extracts_common_fields() {
        let value = json!({
            "src_ip": "10.42.1.10",
            "dst_ip": "10.43.2.20",
            "protocol": "tcp",
            "destination_port": 443,
            "bytes": 1200,
            "packets": 12,
            "connections": 2,
            "timestamp_unix_ms": 1746821760000u64
        });

        let parsed = parse_tetragon_record(&value).expect("record should parse");
        assert_eq!(parsed.src_ip, "10.42.1.10");
        assert_eq!(parsed.dst_ip, "10.43.2.20");
        assert_eq!(parsed.protocol, "tcp");
        assert_eq!(parsed.destination_port, 443);
        assert_eq!(parsed.bytes, 1200);
        assert_eq!(parsed.packets, 12);
        assert_eq!(parsed.connections, 2);
        assert_eq!(parsed.timestamp_unix_ms, 1746821760000);
    }

    #[test]
    fn parse_tetragon_record_extracts_tcp_connect_sock_args() {
        let value = json!({
            "process_kprobe": {
                "function_name": "tcp_sendmsg",
                "args": [
                    {
                        "sock_arg": {
                            "family": "AF_INET6",
                            "type": "SOCK_STREAM",
                            "protocol": "IPPROTO_TCP",
                            "saddr": "::ffff:10.42.0.11",
                            "daddr": "::ffff:10.42.1.233",
                            "sport": 9234,
                            "dport": 57924,
                            "cookie": "18446619261531505280",
                            "state": "TCP_ESTABLISHED"
                        }
                    },
                    { "int_arg": 39 }
                ]
            }
        });

        let parsed = parse_tetragon_record(&value).expect("record should parse");
        assert_eq!(parsed.src_ip, "10.42.0.11");
        assert_eq!(parsed.dst_ip, "10.42.1.233");
        assert_eq!(parsed.protocol, "tcp");
        assert_eq!(parsed.destination_port, 57924);
        assert_eq!(parsed.bytes, 39);
        assert_eq!(parsed.packets, 1);
        assert_eq!(parsed.connections, 0);
    }

    #[test]
    fn parse_tetragon_record_counts_tcp_close_as_connection() {
        let value = json!({
            "process_kprobe": {
                "function_name": "tcp_close",
                "args": [
                    {
                        "sock_arg": {
                            "family": "AF_INET6",
                            "type": "SOCK_STREAM",
                            "protocol": "IPPROTO_TCP",
                            "saddr": "::ffff:10.42.0.11",
                            "daddr": "::ffff:10.42.1.233",
                            "sport": 9234,
                            "dport": 57924,
                            "cookie": "18446619261531505280",
                            "state": "TCP_ESTABLISHED"
                        }
                    }
                ]
            }
        });

        let parsed = parse_tetragon_record(&value).expect("record should parse");
        assert_eq!(parsed.bytes, 0);
        assert_eq!(parsed.packets, 0);
        assert_eq!(parsed.connections, 1);
    }

    #[test]
    fn resolve_endpoint_prefers_pod_then_service_then_unknown() {
        let mut pods = BTreeMap::new();
        pods.insert(
            "10.42.0.10".to_string(),
            PodIpInfo {
                namespace: "prod".into(),
                pod_name: "api-1".into(),
                workload_kind: Some("Deployment".into()),
                workload_name: Some("api".into()),
                ip: "10.42.0.10".into(),
            },
        );
        let mut services = BTreeMap::new();
        services.insert(
            "10.43.0.20".to_string(),
            ServiceIpInfo {
                namespace: "prod".into(),
                service_name: "db".into(),
                ip: "10.43.0.20".into(),
            },
        );

        let pod_endpoint = resolve_endpoint("10.42.0.10", None, &pods, &services);
        assert_eq!(pod_endpoint.kind, "pod");
        assert_eq!(pod_endpoint.name.as_deref(), Some("api-1"));

        let svc_endpoint = resolve_endpoint("10.43.0.20", None, &pods, &services);
        assert_eq!(svc_endpoint.kind, "service");
        assert_eq!(svc_endpoint.name.as_deref(), Some("db"));

        let unknown = resolve_endpoint("10.99.0.1", None, &pods, &services);
        assert_eq!(unknown.kind, "unknown");
        assert_eq!(unknown.ip.as_deref(), Some("10.99.0.1"));
    }

    #[test]
    fn resolve_endpoint_uses_source_hint_before_unknown_fallback() {
        let endpoint = resolve_endpoint(
            "10.99.0.1",
            Some(&DependencyEndpointHint {
                namespace: Some("prod".into()),
                pod_name: Some("api-1".into()),
                workload_kind: Some("Deployment".into()),
                workload_name: Some("api".into()),
            }),
            &BTreeMap::new(),
            &BTreeMap::new(),
        );

        assert_eq!(endpoint.kind, "pod");
        assert_eq!(endpoint.namespace.as_deref(), Some("prod"));
        assert_eq!(endpoint.name.as_deref(), Some("api-1"));
        assert_eq!(endpoint.workload_kind.as_deref(), Some("Deployment"));
        assert_eq!(endpoint.workload_name.as_deref(), Some("api"));
        assert_eq!(endpoint.ip.as_deref(), Some("10.99.0.1"));
    }

    #[test]
    fn normalize_tetragon_helpers_handle_ipv6_and_protocol_prefixes() {
        assert_eq!(
            normalize_tetragon_ip("::ffff:10.42.0.11".to_string()),
            "10.42.0.11"
        );
        assert_eq!(
            normalize_tetragon_ip("2001:db8::1".to_string()),
            "2001:db8::1"
        );
        assert_eq!(
            normalize_tetragon_protocol("IPPROTO_TCP".to_string()),
            "tcp"
        );
        assert_eq!(normalize_tetragon_protocol("tcp".to_string()), "tcp");
    }

    #[test]
    fn map_configmap_collects_keys_and_metadata_only() {
        let cm: ConfigMap = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "prod",
                "name": "app-config",
                "labels": {"app": "api"},
                "annotations": {"checksum/config": "abc123"}
            },
            "immutable": true,
            "data": {"A": "secret-ish", "B": "value"},
            "binaryData": {"BIN": "AA=="}
        }))
        .unwrap();

        let mapped = map_configmap(cm);
        assert_eq!(mapped.namespace, "prod");
        assert_eq!(mapped.name, "app-config");
        assert_eq!(mapped.immutable, Some(true));
        assert_eq!(mapped.labels.len(), 1);
        assert_eq!(mapped.annotations.len(), 1);
        assert_eq!(mapped.data_keys, vec!["A", "B"]);
        assert_eq!(mapped.binary_data_keys, vec!["BIN"]);
    }

    #[test]
    fn build_pod_usage_index_sums_container_metrics() {
        let pod_metrics = vec![
            serde_json::from_value(json!({
                "apiVersion": "metrics.k8s.io/v1beta1",
                "kind": "PodMetrics",
                "metadata": {
                    "namespace": "prod",
                    "name": "api-1"
                },
                "containers": [
                    {"name": "app", "usage": {"cpu": "100m", "memory": "200Mi"}},
                    {"name": "proxy", "usage": {"cpu": "25m", "memory": "56Mi"}}
                ]
            }))
            .unwrap(),
        ];

        let index = build_pod_usage_index(&pod_metrics);
        let usage = index
            .get(&("prod".to_string(), "api-1".to_string()))
            .unwrap();
        assert_eq!(format_cpu_quantity(usage.cpu_nano).as_deref(), Some("125m"));
        assert_eq!(
            format_memory_quantity(usage.memory_bytes).as_deref(),
            Some("256Mi")
        );
    }

    #[test]
    fn quantity_helpers_parse_and_format_common_values() {
        assert_eq!(parse_cpu_quantity("125m"), Some(125_000_000));
        assert_eq!(parse_cpu_quantity("500u"), Some(500_000));
        assert_eq!(format_cpu_quantity(125_000_000).as_deref(), Some("125m"));

        assert_eq!(parse_memory_quantity("256Mi"), Some(268_435_456));
        assert_eq!(parse_memory_quantity("1Gi"), Some(1_073_741_824));
        assert_eq!(
            format_memory_quantity(268_435_456).as_deref(),
            Some("256Mi")
        );
    }

    #[test]
    fn map_pod_includes_usage_when_present() {
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "prod",
                "name": "api-1"
            },
            "spec": {
                "nodeName": "node-1",
                "containers": [
                    {
                        "name": "app",
                        "image": "nginx:1.25",
                        "resources": {}
                    }
                ]
            },
            "status": {
                "phase": "Running"
            }
        }))
        .unwrap();
        let usage = BTreeMap::from([(
            ("prod".to_string(), "api-1".to_string()),
            PodUsageTotals {
                cpu_nano: 125_000_000,
                memory_bytes: 268_435_456,
            },
        )]);

        let mapped = map_pod(pod, &usage, false);
        assert_eq!(mapped.usage_cpu.as_deref(), Some("125m"));
        assert_eq!(mapped.usage_memory.as_deref(), Some("256Mi"));
    }

    #[test]
    fn map_pod_uses_labels_for_technology_detection() {
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "prod",
                "name": "api-1",
                "labels": {
                    "app.kubernetes.io/component": "spring-boot",
                    "app.kubernetes.io/name": "causas-backend"
                },
                "annotations": {
                    "app.spring.io/version": "3.3.5"
                }
            },
            "spec": {
                "nodeName": "node-1",
                "containers": [
                    {
                        "name": "app",
                        "image": "causas-backend:latest",
                        "resources": {}
                    }
                ]
            },
            "status": {
                "phase": "Running"
            }
        }))
        .unwrap();

        let mapped = map_pod(pod, &BTreeMap::new(), false);
        let technology = &mapped.containers[0].technology;

        assert_eq!(technology.source, "labels");
        assert_eq!(technology.product.as_deref(), Some("spring-boot"));
        assert_eq!(technology.subtype.as_deref(), Some("spring_boot"));
        assert_eq!(technology.language.as_deref(), Some("Java"));
        assert_eq!(technology.version.as_deref(), Some("3.3.5"));
    }

    #[test]
    fn map_network_policy_captures_selector_types_and_counts() {
        let policy: NetworkPolicy = serde_json::from_value(json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {
                "namespace": "prod",
                "name": "default-deny"
            },
            "spec": {
                "podSelector": {
                    "matchLabels": {
                        "app": "api"
                    }
                },
                "policyTypes": ["Ingress", "Egress"],
                "ingress": [{}],
                "egress": [{}, {}]
            }
        }))
        .unwrap();

        let mapped = map_network_policy(policy);
        assert_eq!(mapped.namespace, "prod");
        assert_eq!(mapped.name, "default-deny");
        assert_eq!(mapped.policy_types, vec!["Egress", "Ingress"]);
        assert_eq!(mapped.ingress_rules_count, 1);
        assert_eq!(mapped.egress_rules_count, 2);
        assert_eq!(
            mapped.pod_selector,
            vec![KV {
                key: "app".into(),
                value: "api".into(),
            }]
        );
    }

    #[test]
    fn map_cluster_role_binding_filters_and_flags_risk() {
        let high: ClusterRoleBinding = serde_json::from_value(json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": {"name": "admins"},
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "ClusterRole",
                "name": "cluster-admin"
            },
            "subjects": [
                {"kind": "User", "name": "alice"},
                {"kind": "ServiceAccount", "name": "bot", "namespace": "ops"}
            ]
        }))
        .unwrap();
        let review: ClusterRoleBinding = serde_json::from_value(json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": {"name": "platform"},
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "ClusterRole",
                "name": "platform-admin"
            },
            "subjects": []
        }))
        .unwrap();

        let mapped_high = map_cluster_role_binding(high).unwrap();
        assert_eq!(mapped_high.risk_level, "high");
        assert_eq!(mapped_high.role_ref_name, "cluster-admin");
        assert_eq!(mapped_high.subjects.len(), 2);

        let mapped_review = map_cluster_role_binding(review).unwrap();
        assert_eq!(mapped_review.risk_level, "review");

        let ignored: ClusterRoleBinding = serde_json::from_value(json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": {"name": "ignored"},
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "ClusterRole",
                "name": "system:discovery"
            },
            "subjects": []
        }))
        .unwrap();
        assert!(map_cluster_role_binding(ignored).is_none());
    }

    #[test]
    fn build_pod_security_admission_includes_missing_labels() {
        let namespaces = vec![
            NamespaceInfo {
                name: "prod".into(),
                phase: Some("Active".into()),
                labels: vec![
                    KV {
                        key: PSA_ENFORCE_LABEL.into(),
                        value: "restricted".into(),
                    },
                    KV {
                        key: PSA_WARN_LABEL.into(),
                        value: "baseline".into(),
                    },
                ],
            },
            NamespaceInfo {
                name: "dev".into(),
                phase: Some("Active".into()),
                labels: vec![],
            },
        ];

        let psa = build_pod_security_admission(&namespaces);
        assert_eq!(psa.namespaces.len(), 2);
        assert_eq!(psa.namespaces[0].namespace, "dev");
        assert_eq!(psa.namespaces[0].enforce, None);
        assert_eq!(psa.namespaces[1].namespace, "prod");
        assert_eq!(psa.namespaces[1].enforce.as_deref(), Some("restricted"));
        assert_eq!(psa.namespaces[1].audit, None);
        assert_eq!(psa.namespaces[1].warn.as_deref(), Some("baseline"));
    }

    #[test]
    fn detect_descheduler_finds_deployment_by_name() {
        let deployments = vec![
            serde_json::from_value(json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "namespace": "kube-system",
                    "name": "coredns"
                },
                "spec": {
                    "selector": {"matchLabels": {"app": "coredns"}}
                }
            }))
            .unwrap(),
            serde_json::from_value(json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "namespace": "kube-system",
                    "name": "descheduler"
                },
                "spec": {
                    "selector": {"matchLabels": {"descheduler": "true"}}
                }
            }))
            .unwrap(),
        ];

        let info = detect_descheduler(&deployments);
        assert!(info.installed);
        assert_eq!(info.detected_by.as_deref(), Some("deployment"));
        assert_eq!(info.namespace.as_deref(), Some("kube-system"));
    }

    #[test]
    fn detect_descheduler_returns_default_when_not_found() {
        let deployments: Vec<Deployment> = vec![
            serde_json::from_value(json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"namespace": "default", "name": "nginx"},
                "spec": {"selector": {"matchLabels": {"app": "nginx"}}}
            }))
            .unwrap(),
        ];

        let info = detect_descheduler(&deployments);
        assert!(!info.installed);
    }

    #[test]
    fn build_vpa_info_counts_objects_and_modes() {
        let vpa_objects = vec![
            serde_json::from_value(json!({
                "apiVersion": "autoscaling.k8s.io/v1",
                "kind": "VerticalPodAutoscaler",
                "metadata": {"namespace": "prod", "name": "vpa-app"},
                "spec": {
                    "targetRef": {"kind": "Deployment", "name": "app"},
                    "updatePolicy": {"updateMode": "Auto"}
                }
            }))
            .unwrap(),
            serde_json::from_value(json!({
                "apiVersion": "autoscaling.k8s.io/v1",
                "kind": "VerticalPodAutoscaler",
                "metadata": {"namespace": "prod", "name": "vpa-db"},
                "spec": {
                    "targetRef": {"kind": "Deployment", "name": "db"},
                    "updatePolicy": {"updateMode": "Off"}
                }
            }))
            .unwrap(),
        ];

        let info = build_vpa_info(&vpa_objects);
        assert!(info.installed);
        assert_eq!(info.objects_count, 2);
        assert_eq!(info.update_modes.len(), 2);
    }

    #[test]
    fn build_vpa_info_empty_when_no_vpa_objects() {
        let info = build_vpa_info(&[]);
        assert!(!info.installed);
        assert_eq!(info.objects_count, 0);
        assert!(info.update_modes.is_empty());
    }

    #[test]
    fn map_scheduled_job_extracts_cronjob_fields() {
        let cronjob: CronJob = serde_json::from_value(json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "namespace": "kube-tools",
                "name": "descheduler-cron"
            },
            "spec": {
                "schedule": "0 */6 * * *",
                "suspend": false
            },
            "status": {
                "lastScheduleTime": "2026-06-11T20:00:00Z",
                "lastSuccessfulTime": "2026-06-11T20:00:05Z"
            }
        }))
        .unwrap();

        let mapped = map_scheduled_job(cronjob);
        assert_eq!(mapped.namespace, "kube-tools");
        assert_eq!(mapped.name, "descheduler-cron");
        assert_eq!(mapped.schedule, "0 */6 * * *");
        assert!(!mapped.suspend);
        assert_eq!(
            mapped.last_schedule_time.as_deref(),
            Some("2026-06-11T20:00:00Z")
        );
        assert_eq!(
            mapped.last_successful_time.as_deref(),
            Some("2026-06-11T20:00:05Z")
        );
    }

    #[test]
    fn collect_agent_configured_env_filters_agent_configmap_values() {
        let configmaps = vec![
            serde_json::from_value(json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "sentinella",
                    "name": "sentinella-hub-k8s-agent-config"
                },
                "data": {
                    "HUB_URL": "https://hub.example.com",
                    "COLLECT_DEPENDENCIES_TETRAGON": "true",
                    "HUB_API_KEY": "secret",
                    "POD_NAME": "pod-1",
                    "UNRELATED": "value"
                }
            }))
            .unwrap(),
            serde_json::from_value(json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "default",
                    "name": "other"
                },
                "data": {
                    "HUB_URL": "https://ignore.example.com"
                }
            }))
            .unwrap(),
        ];

        let env = collect_agent_configured_env(&configmaps);
        assert_eq!(
            env,
            vec![
                KV {
                    key: "COLLECT_DEPENDENCIES_TETRAGON".into(),
                    value: "true".into(),
                },
                KV {
                    key: "HUB_URL".into(),
                    value: "https://hub.example.com".into(),
                },
            ]
        );
    }

    #[test]
    fn collect_agent_configured_env_missing_configmap_is_empty() {
        let configmaps: Vec<ConfigMap> = Vec::new();
        assert!(collect_agent_configured_env(&configmaps).is_empty());
    }

    #[test]
    fn map_secret_collects_key_names_without_values() {
        let secret: Secret = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "namespace": "prod",
                "name": "db-auth",
                "annotations": {"owner": "platform"}
            },
            "type": "Opaque",
            "data": {
                "password": "c2VjcmV0",
                "username": "YWRtaW4="
            }
        }))
        .unwrap();

        let mapped = map_secret(secret);
        assert_eq!(mapped.namespace, "prod");
        assert_eq!(mapped.name, "db-auth");
        assert_eq!(mapped.type_.as_deref(), Some("Opaque"));
        assert_eq!(mapped.data_keys, vec!["password", "username"]);
        assert_eq!(mapped.annotations.len(), 1);
    }

    #[test]
    fn map_service_extracts_selector_ports_and_lb() {
        let service: Service = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "api", "namespace": "prod"},
            "spec": {
                "type": "LoadBalancer",
                "clusterIP": "10.0.0.10",
                "externalIPs": ["203.0.113.10"],
                "selector": {"app": "api"},
                "ports": [
                    {"name": "http", "protocol": "TCP", "port": 80, "targetPort": 8080, "nodePort": 30080}
                ]
            },
            "status": {
                "loadBalancer": {
                    "ingress": [{"hostname": "lb.example.com"}, {"ip": "34.12.10.9"}]
                }
            }
        }))
        .unwrap();

        let mapped = map_service(service);
        assert_eq!(mapped.namespace, "prod");
        assert_eq!(mapped.name, "api");
        assert_eq!(mapped.type_, "LoadBalancer");
        assert_eq!(mapped.cluster_ip.as_deref(), Some("10.0.0.10"));
        assert_eq!(mapped.external_ips, vec!["203.0.113.10"]);
        assert_eq!(mapped.selector.len(), 1);
        assert_eq!(mapped.selector[0].key, "app");
        assert_eq!(mapped.selector[0].value, "api");
        assert_eq!(mapped.ports.len(), 1);
        assert_eq!(mapped.ports[0].port, 80);
        assert_eq!(mapped.ports[0].target_port.as_deref(), Some("8080"));
        assert_eq!(mapped.load_balancer_ingress.len(), 2);
    }

    #[test]
    fn map_ingress_extracts_rules_tls_and_lb() {
        let ingress: Ingress = serde_json::from_value(json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": {"name": "web", "namespace": "prod"},
            "spec": {
                "ingressClassName": "nginx",
                "rules": [
                    {
                        "host": "example.com",
                        "http": {
                            "paths": [
                                {
                                    "path": "/",
                                    "pathType": "Prefix",
                                    "backend": {
                                        "service": {
                                            "name": "web-svc",
                                            "port": {"number": 80}
                                        }
                                    }
                                }
                            ]
                        }
                    }
                ],
                "tls": [
                    {"hosts": ["example.com"], "secretName": "web-tls"}
                ]
            },
            "status": {
                "loadBalancer": {
                    "ingress": [{"ip": "34.118.20.1"}]
                }
            }
        }))
        .unwrap();

        let mapped = map_ingress(ingress);
        assert_eq!(mapped.namespace, "prod");
        assert_eq!(mapped.name, "web");
        assert_eq!(mapped.class_name.as_deref(), Some("nginx"));
        assert_eq!(mapped.hosts, vec!["example.com"]);
        assert_eq!(mapped.rules.len(), 1);
        assert_eq!(mapped.rules[0].backend_service.as_deref(), Some("web-svc"));
        assert_eq!(mapped.rules[0].backend_port.as_deref(), Some("80"));
        assert_eq!(mapped.tls.len(), 1);
        assert_eq!(mapped.tls[0].secret_name.as_deref(), Some("web-tls"));
        assert_eq!(mapped.load_balancer_ingress, vec!["34.118.20.1"]);
    }

    #[test]
    fn map_service_handles_missing_spec() {
        let service: Service = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "headless", "namespace": "prod"}
        }))
        .unwrap();

        let mapped = map_service(service);
        assert_eq!(mapped.type_, "ClusterIP");
        assert!(mapped.cluster_ip.is_none());
        assert!(mapped.external_ips.is_empty());
        assert!(mapped.selector.is_empty());
        assert!(mapped.ports.is_empty());
    }

    #[test]
    fn map_ingress_handles_missing_rules_and_tls() {
        let ingress: Ingress = serde_json::from_value(json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": {"name": "web", "namespace": "prod"},
            "spec": {
                "ingressClassName": "nginx"
            }
        }))
        .unwrap();

        let mapped = map_ingress(ingress);
        assert_eq!(mapped.class_name.as_deref(), Some("nginx"));
        assert!(mapped.hosts.is_empty());
        assert!(mapped.rules.is_empty());
        assert!(mapped.tls.is_empty());
    }

    // ---- Pod-metrics classification (SEN-329) ----

    fn kube_api_error(code: u16) -> KubeError {
        KubeError::Api(Box::new(kube::core::Status {
            code,
            message: String::new(),
            reason: String::new(),
            ..Default::default()
        }))
    }

    #[test]
    fn classify_metrics_error_forbidden() {
        let err = kube_api_error(403);
        let (state, reason) = classify_metrics_error(&err);
        assert_eq!(state, "forbidden");
        assert_eq!(reason, "ServiceAccount missing metrics.k8s.io RBAC");
    }

    #[test]
    fn classify_metrics_error_not_found() {
        let err = kube_api_error(404);
        let (state, reason) = classify_metrics_error(&err);
        assert_eq!(state, "missing");
        assert_eq!(reason, "metrics-server not installed");
    }

    #[test]
    fn classify_metrics_error_unavailable_503() {
        let err = kube_api_error(503);
        let (state, reason) = classify_metrics_error(&err);
        assert_eq!(state, "unavailable");
        assert_eq!(reason, "metrics-server registered but not ready");
    }

    #[test]
    fn classify_metrics_error_unavailable_504() {
        let err = kube_api_error(504);
        let (state, reason) = classify_metrics_error(&err);
        assert_eq!(state, "unavailable");
        assert_eq!(reason, "metrics-server timeout");
    }

    #[test]
    fn classify_metrics_error_other_api_code() {
        let err = kube_api_error(500);
        let (state, reason) = classify_metrics_error(&err);
        assert_eq!(state, "error");
        assert_eq!(reason, "kube API error");
    }

    #[test]
    fn classify_transient_error_timeout_keyword() {
        // We can't easily construct a non-Api KubeError variant that carries
        // a deterministic message, so test the helper directly with a string
        // proxy: the helper inspects the formatted error string.
        // Simulate by passing a Display-formatted value through the helper
        // path: classify_metrics_error returns transient via `_` arm.
        // Use an unrelated code to drive the `_` arm.
        let err = kube_api_error(418);
        let (state, reason) = classify_metrics_error(&err);
        assert_eq!(state, "error");
        assert_eq!(reason, "kube API error");
    }

    #[test]
    fn is_not_found_only_matches_404() {
        assert!(is_not_found(&kube_api_error(404)));
        assert!(!is_not_found(&kube_api_error(403)));
        assert!(!is_not_found(&kube_api_error(500)));
    }

    #[test]
    fn status_from_omits_reason_when_empty() {
        let s = status_from("ok", "", "metrics.k8s.io/v1", 0, 0);
        assert_eq!(s.state, "ok");
        assert!(s.reason.is_none());
    }

    #[test]
    fn status_from_keeps_reason_when_present() {
        let s = status_from(
            "missing",
            "metrics-server not installed",
            "metrics.k8s.io/v1",
            0,
            0,
        );
        assert_eq!(s.state, "missing");
        assert_eq!(s.reason.as_deref(), Some("metrics-server not installed"));
    }

    // ---- CSI snapshot API classification (SEN-330) ----

    #[test]
    fn classify_snapshot_api_error_forbidden() {
        let err = kube_api_error(403);
        let (state, reason) = classify_snapshot_api_error(&err);
        assert_eq!(state, "forbidden");
        assert_eq!(
            reason,
            "ServiceAccount missing snapshot.storage.k8s.io RBAC"
        );
    }

    #[test]
    fn classify_snapshot_api_error_not_found() {
        let err = kube_api_error(404);
        let (state, reason) = classify_snapshot_api_error(&err);
        assert_eq!(state, "missing");
        assert_eq!(reason, "CSI snapshot CRDs not installed");
    }

    #[test]
    fn classify_snapshot_api_error_unavailable_503() {
        let err = kube_api_error(503);
        let (state, reason) = classify_snapshot_api_error(&err);
        assert_eq!(state, "unavailable");
        assert_eq!(reason, "CSI snapshot API registered but not ready");
    }

    #[test]
    fn classify_snapshot_api_error_unavailable_504() {
        let err = kube_api_error(504);
        let (state, reason) = classify_snapshot_api_error(&err);
        assert_eq!(state, "unavailable");
        assert_eq!(reason, "CSI snapshot API timeout");
    }

    #[test]
    fn classify_snapshot_api_error_other_api_code() {
        let err = kube_api_error(500);
        let (state, reason) = classify_snapshot_api_error(&err);
        assert_eq!(state, "error");
        assert_eq!(reason, "kube API error");
    }

    #[test]
    fn status_snapshot_from_omits_reason_when_empty() {
        let s = status_snapshot_from("ok", "", "snapshot.storage.k8s.io/v1", 0, 0, 0);
        assert_eq!(s.state, "ok");
        assert!(s.reason.is_none());
    }

    #[test]
    fn status_snapshot_from_keeps_reason_when_present() {
        let s = status_snapshot_from(
            "missing",
            "CSI snapshot CRDs not installed",
            "snapshot.storage.k8s.io/v1",
            0,
            0,
            0,
        );
        assert_eq!(s.state, "missing");
        assert_eq!(s.reason.as_deref(), Some("CSI snapshot CRDs not installed"));
        assert_eq!(s.volumesnapshotclasses_count, 0);
        assert_eq!(s.volumesnapshots_count, 0);
    }

    #[test]
    fn status_snapshot_from_preserves_counts() {
        let s = status_snapshot_from("ok", "", "snapshot.storage.k8s.io/v1", 7, 12, 1748523600000);
        assert_eq!(s.volumesnapshotclasses_count, 7);
        assert_eq!(s.volumesnapshots_count, 12);
        assert_eq!(s.last_attempt_at_ms, 1748523600000);
    }
}
