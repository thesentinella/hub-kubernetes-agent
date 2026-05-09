//! Collects cluster inventory via the Kubernetes API.

use crate::model::*;
use crate::tech;
use anyhow::Result;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::core::v1::{
    Event, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod, Service,
};
use k8s_openapi::api::networking::v1::Ingress;
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{ApiResource, DynamicObject, ListParams, LogParams};
use kube::core::GroupVersionKind;
use kube::{Api, Client};
use std::collections::BTreeMap;
use tracing::warn;

pub async fn collect(
    client: &Client,
) -> Result<(
    ClusterInfo,
    Vec<NamespaceInfo>,
    Workloads,
    Vec<PodInfo>,
    NetworkInventory,
    StorageInventory,
    Vec<EventInfo>,
    Vec<PodLogInfo>,
)> {
    // Concurrency: launch all list calls in parallel; fail soft on individual lists.
    let lp = ListParams::default();

    let nodes_fut = list_all::<Node>(client, &lp);
    let ns_fut = list_all::<Namespace>(client, &lp);
    let deploy_fut = list_all::<Deployment>(client, &lp);
    let sts_fut = list_all::<StatefulSet>(client, &lp);
    let ds_fut = list_all::<DaemonSet>(client, &lp);
    let pods_fut = list_all::<Pod>(client, &lp);
    let services_fut = list_all::<Service>(client, &lp);
    let ingresses_fut = list_all::<Ingress>(client, &lp);
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
        storage_classes,
        pvs,
        pvcs,
        volume_snapshot_classes,
        volume_snapshots,
        events,
        version,
    ) = tokio::join!(
        nodes_fut,
        ns_fut,
        deploy_fut,
        sts_fut,
        ds_fut,
        pods_fut,
        services_fut,
        ingresses_fut,
        storage_classes_fut,
        pvs_fut,
        pvcs_fut,
        volume_snapshot_classes_fut,
        volume_snapshots_fut,
        events_fut,
        version_fut
    );

    let nodes = soft_unwrap("nodes", nodes);
    let namespaces = soft_unwrap("namespaces", namespaces);
    let deployments = soft_unwrap("deployments", deployments);
    let statefulsets = soft_unwrap("statefulsets", statefulsets);
    let daemonsets = soft_unwrap("daemonsets", daemonsets);
    let pods = soft_unwrap("pods", pods);
    let mut services = soft_unwrap("services", services);
    let mut ingresses = soft_unwrap("ingresses", ingresses);
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
    let pod_logs = collect_problematic_pod_logs(client, &pods).await;
    let pod_infos = pods.into_iter().map(map_pod).collect();
    sort_services_for_snapshot(&mut services);
    sort_ingresses_for_snapshot(&mut ingresses);
    let network = NetworkInventory {
        services: services.into_iter().map(map_service).collect(),
        ingresses: ingresses.into_iter().map(map_ingress).collect(),
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
        cluster,
        ns_infos,
        workloads,
        pod_infos,
        network,
        storage,
        event_infos,
        pod_logs,
    ))
}

async fn collect_problematic_pod_logs(client: &Client, pods: &[Pod]) -> Vec<PodLogInfo> {
    let mut candidates = pods
        .iter()
        .flat_map(problematic_container_candidates)
        .collect::<Vec<_>>();

    candidates.sort_by(|a, b| {
        a.namespace
            .cmp(&b.namespace)
            .then_with(|| a.pod.cmp(&b.pod))
            .then_with(|| a.container.cmp(&b.container))
    });

    let mut results = Vec::new();
    let mut total_chars = 0usize;
    let mut pods_seen = std::collections::BTreeSet::new();
    let mut per_pod_container_count = std::collections::BTreeMap::<(String, String), usize>::new();

    for candidate in candidates {
        let pod_key = (candidate.namespace.clone(), candidate.pod.clone());
        if !pods_seen.contains(&pod_key) && pods_seen.len() >= MAX_LOG_PODS {
            break;
        }
        let count = per_pod_container_count
            .entry(pod_key.clone())
            .and_modify(|c| *c += 1)
            .or_insert(1);
        if *count > MAX_LOG_CONTAINERS_PER_POD {
            continue;
        }
        pods_seen.insert(pod_key);

        if let Some(entry) = fetch_container_logs(client, &candidate, false, &mut total_chars).await
        {
            results.push(entry);
        }
        if candidate.include_previous {
            if let Some(entry) =
                fetch_container_logs(client, &candidate, true, &mut total_chars).await
            {
                results.push(entry);
            }
        }

        if total_chars >= MAX_TOTAL_LOG_CHARS {
            break;
        }
    }

    results
}

async fn fetch_container_logs(
    client: &Client,
    candidate: &ProblematicContainerCandidate,
    previous: bool,
    total_chars: &mut usize,
) -> Option<PodLogInfo> {
    let pods_api: Api<Pod> = Api::namespaced(client.clone(), &candidate.namespace);
    let log_params = LogParams {
        container: Some(candidate.container.clone()),
        previous,
        timestamps: true,
        tail_lines: Some(MAX_LOG_LINES as i64),
        ..LogParams::default()
    };

    match pods_api.logs(&candidate.pod, &log_params).await {
        Ok(logs) => {
            if logs.trim().is_empty() {
                return None;
            }
            let mut lines = Vec::new();
            for raw in logs.lines().take(MAX_LOG_LINES) {
                if *total_chars >= MAX_TOTAL_LOG_CHARS {
                    break;
                }
                let line = truncate_chars(raw, MAX_LOG_LINE_CHARS);
                *total_chars += line.chars().count();
                lines.push(line);
            }
            if lines.is_empty() {
                return None;
            }

            Some(PodLogInfo {
                namespace: candidate.namespace.clone(),
                pod: candidate.pod.clone(),
                container: candidate.container.clone(),
                source: if previous {
                    "previous".to_string()
                } else {
                    "current".to_string()
                },
                reason: candidate.reason.clone(),
                lines,
            })
        }
        Err(e) => {
            warn!(
                namespace = %candidate.namespace,
                pod = %candidate.pod,
                container = %candidate.container,
                previous,
                error = %e,
                "pod log collection failed"
            );
            None
        }
    }
}

#[derive(Clone)]
struct ProblematicContainerCandidate {
    namespace: String,
    pod: String,
    container: String,
    reason: String,
    include_previous: bool,
}

fn problematic_container_candidates(pod: &Pod) -> Vec<ProblematicContainerCandidate> {
    let namespace = pod.metadata.namespace.clone().unwrap_or_default();
    let pod_name = pod.metadata.name.clone().unwrap_or_default();
    let phase = pod.status.as_ref().and_then(|s| s.phase.clone());

    let mut out = Vec::new();
    let statuses = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .cloned()
        .unwrap_or_default();

    for status in statuses {
        let mut reasons = Vec::new();
        let mut include_previous = false;

        if status.restart_count > 0 {
            reasons.push(format!("restart_count={}", status.restart_count));
            include_previous = true;
        }

        if let Some(state) = status.state.as_ref() {
            if let Some(waiting) = state.waiting.as_ref() {
                let wait_reason = waiting.reason.clone().unwrap_or_default();
                if wait_reason == "CrashLoopBackOff"
                    || wait_reason == "ImagePullBackOff"
                    || wait_reason == "ErrImagePull"
                    || wait_reason == "CreateContainerConfigError"
                    || wait_reason == "CreateContainerError"
                {
                    reasons.push(format!("waiting:{}", wait_reason));
                    include_previous = include_previous || wait_reason == "CrashLoopBackOff";
                }
            }

            if let Some(terminated) = state.terminated.as_ref() {
                if terminated.exit_code != 0 {
                    reasons.push(format!("terminated_exit_code={}", terminated.exit_code));
                    include_previous = true;
                }
            }
        }

        if phase.as_deref() == Some("Failed") {
            reasons.push("pod_phase=Failed".to_string());
        }
        if phase.as_deref() == Some("Unknown") {
            reasons.push("pod_phase=Unknown".to_string());
        }

        if phase.as_deref() == Some("Pending") && reasons.is_empty() {
            if let Some(state) = status.state.as_ref() {
                if let Some(waiting) = state.waiting.as_ref() {
                    if let Some(wait_reason) = waiting.reason.as_ref() {
                        if wait_reason == "ImagePullBackOff"
                            || wait_reason == "ErrImagePull"
                            || wait_reason == "CreateContainerConfigError"
                            || wait_reason == "CreateContainerError"
                            || wait_reason == "ContainerCreating"
                        {
                            reasons.push(format!("pending_waiting:{}", wait_reason));
                        }
                    }
                }
            }
        }

        reasons.sort();
        reasons.dedup();
        if !reasons.is_empty() {
            out.push(ProblematicContainerCandidate {
                namespace: namespace.clone(),
                pod: pod_name.clone(),
                container: status.name,
                reason: reasons.join(";"),
                include_previous,
            });
        }
    }

    out
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
    let labels = n
        .metadata
        .labels
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| KV { key: k, value: v })
        .collect();
    NamespaceInfo {
        name: n.metadata.name.unwrap_or_default(),
        phase: n.status.and_then(|s| s.phase),
        labels,
    }
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
const MAX_LOG_PODS: usize = 20;
const MAX_LOG_CONTAINERS_PER_POD: usize = 2;
const MAX_LOG_LINES: usize = 80;
const MAX_LOG_LINE_CHARS: usize = 500;
const MAX_TOTAL_LOG_CHARS: usize = 200_000;

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
    fn problematic_container_detects_pending_pull_failure() {
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "prod", "name": "api-123"},
            "status": {
                "phase": "Pending",
                "containerStatuses": [
                    {
                        "name": "app",
                        "image": "repo/app:1",
                        "imageID": "docker://sha256:abc",
                        "ready": false,
                        "restartCount": 0,
                        "state": {"waiting": {"reason": "ImagePullBackOff"}}
                    }
                ]
            }
        }))
        .unwrap();

        let candidates = problematic_container_candidates(&pod);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].container, "app");
        assert_eq!(candidates[0].reason, "waiting:ImagePullBackOff");
    }

    #[test]
    fn problematic_container_combines_reasons() {
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "prod", "name": "api-123"},
            "status": {
                "phase": "Running",
                "containerStatuses": [
                    {
                        "name": "app",
                        "image": "repo/app:1",
                        "imageID": "docker://sha256:abc",
                        "ready": false,
                        "restartCount": 3,
                        "state": {"waiting": {"reason": "CrashLoopBackOff"}}
                    }
                ]
            }
        }))
        .unwrap();

        let candidates = problematic_container_candidates(&pod);
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].reason,
            "restart_count=3;waiting:CrashLoopBackOff"
        );
        assert!(candidates[0].include_previous);
    }

    #[test]
    fn problematic_container_skips_healthy_running() {
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "prod", "name": "api-123"},
            "status": {
                "phase": "Running",
                "containerStatuses": [
                    {
                        "name": "app",
                        "image": "repo/app:1",
                        "imageID": "docker://sha256:abc",
                        "ready": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-09T00:00:00Z"}}
                    }
                ]
            }
        }))
        .unwrap();

        let candidates = problematic_container_candidates(&pod);
        assert!(candidates.is_empty());
    }

    #[test]
    fn truncate_chars_respects_max_length() {
        let input = "a".repeat(1000);
        let out = truncate_chars(&input, 20);
        assert_eq!(out.chars().count(), 20);
        assert!(out.ends_with("..."));
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
