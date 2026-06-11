//! Collects cluster inventory via the Kubernetes API.

use crate::health;
use crate::model::*;
use crate::tech;
use crate::tetragon;
use anyhow::Result;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::core::v1::{
    ConfigMap, Event, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod, Secret,
    Service,
};
use k8s_openapi::api::networking::v1::Ingress;
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{ApiResource, DynamicObject, ListParams};
use kube::core::GroupVersionKind;
use kube::{Api, Client};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use tracing::warn;

pub async fn collect(
    client: &Client,
    collect_secrets: bool,
    collect_dependencies_tetragon: bool,
) -> Result<(
    Option<String>,
    ClusterInfo,
    Vec<NamespaceInfo>,
    Workloads,
    Vec<PodInfo>,
    NetworkInventory,
    DependencyInventory,
    ConfigurationInventory,
    StorageInventory,
    Vec<EventInfo>,
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
    let volume_snapshot_classes_fut = list_dynamic_all(
        client,
        "snapshot.storage.k8s.io",
        "v1",
        "VolumeSnapshotClass",
    );
    let volume_snapshots_fut =
        list_dynamic_all(client, "snapshot.storage.k8s.io", "v1", "VolumeSnapshot");
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
        configmaps,
        secrets,
        storage_classes,
        pvs,
        pvcs,
        volume_snapshot_classes,
        volume_snapshots,
        events,
        version,
        k8s_uid,
    ) = tokio::join!(
        nodes_fut,
        ns_fut,
        deploy_fut,
        sts_fut,
        ds_fut,
        pods_fut,
        services_fut,
        ingresses_fut,
        configmaps_fut,
        secrets_fut,
        storage_classes_fut,
        pvs_fut,
        pvcs_fut,
        volume_snapshot_classes_fut,
        volume_snapshots_fut,
        events_fut,
        version_fut,
        k8s_uid_fut
    );

    let nodes = soft_unwrap("nodes", nodes);
    let namespaces = soft_unwrap("namespaces", namespaces);
    let deployments = soft_unwrap("deployments", deployments);
    let statefulsets = soft_unwrap("statefulsets", statefulsets);
    let daemonsets = soft_unwrap("daemonsets", daemonsets);
    let pods = soft_unwrap("pods", pods);
    let mut services = soft_unwrap("services", services);
    let mut ingresses = soft_unwrap("ingresses", ingresses);
    let mut configmaps = soft_unwrap("configmaps", configmaps);
    let mut secrets = soft_unwrap("secrets", secrets);
    let storage_classes = soft_unwrap("storageclasses", storage_classes);
    let pvs = soft_unwrap("persistentvolumes", pvs);
    let pvcs = soft_unwrap("persistentvolumeclaims", pvcs);
    let volume_snapshot_classes = soft_unwrap("volumesnapshotclasses", volume_snapshot_classes);
    let volume_snapshots = soft_unwrap("volumesnapshots", volume_snapshots);
    let mut events = soft_unwrap("events", events);

    let cluster = build_cluster_info(version.ok(), &nodes);
    let ns_infos = namespaces.into_iter().map(map_namespace).collect();
    let workloads = Workloads {
        deployments: deployments.into_iter().map(map_deployment).collect(),
        statefulsets: statefulsets.into_iter().map(map_statefulset).collect(),
        daemonsets: daemonsets.into_iter().map(map_daemonset).collect(),
    };
    sort_services_for_snapshot(&mut services);
    sort_ingresses_for_snapshot(&mut ingresses);
    let dependencies =
        collect_dependency_inventory(collect_dependencies_tetragon, &pods, &services).await;
    let pod_infos = pods.into_iter().map(map_pod).collect();
    let network = NetworkInventory {
        services: services.into_iter().map(map_service).collect(),
        ingresses: ingresses.into_iter().map(map_ingress).collect(),
    };
    sort_configmaps_for_snapshot(&mut configmaps);
    sort_secrets_for_snapshot(&mut secrets);
    let configuration = ConfigurationInventory {
        configmaps: configmaps.into_iter().map(map_configmap).collect(),
        secrets: secrets.into_iter().map(map_secret).collect(),
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
        dependencies,
        configuration,
        storage,
        event_infos,
    ))
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

fn map_pod(p: Pod) -> PodInfo {
    let owner = p
        .metadata
        .owner_references
        .as_ref()
        .and_then(|refs| refs.first().cloned());

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
                    technology: tech::detect(c.image.as_deref().unwrap_or("")),
                    resources: map_resources(c.resources.as_ref()),
                })
                .collect()
        })
        .unwrap_or_default();

    PodInfo {
        namespace: p.metadata.namespace.unwrap_or_default(),
        name: p.metadata.name.unwrap_or_default(),
        age_seconds: pod_age_seconds(p.metadata.creation_timestamp.as_ref()),
        node: p.spec.as_ref().and_then(|s| s.node_name.clone()),
        phase: p.status.and_then(|s| s.phase),
        owner_kind: owner.as_ref().map(|o| o.kind.clone()),
        owner_name: owner.as_ref().map(|o| o.name.clone()),
        containers,
    }
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
}
