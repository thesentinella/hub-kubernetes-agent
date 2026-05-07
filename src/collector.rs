//! Collects cluster inventory via the Kubernetes API.

use crate::model::*;
use crate::tech;
use anyhow::Result;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::core::v1::{Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod};
use k8s_openapi::api::storage::v1::StorageClass;
use kube::api::{ApiResource, DynamicObject, ListParams};
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
    StorageInventory,
)> {
    // Concurrency: launch all list calls in parallel; fail soft on individual lists.
    let lp = ListParams::default();

    let nodes_fut = list_all::<Node>(client, &lp);
    let ns_fut = list_all::<Namespace>(client, &lp);
    let deploy_fut = list_all::<Deployment>(client, &lp);
    let sts_fut = list_all::<StatefulSet>(client, &lp);
    let ds_fut = list_all::<DaemonSet>(client, &lp);
    let pods_fut = list_all::<Pod>(client, &lp);
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
    let version_fut = client.apiserver_version();

    let (
        nodes,
        namespaces,
        deployments,
        statefulsets,
        daemonsets,
        pods,
        storage_classes,
        pvs,
        pvcs,
        volume_snapshot_classes,
        volume_snapshots,
        version,
    ) = tokio::join!(
        nodes_fut,
        ns_fut,
        deploy_fut,
        sts_fut,
        ds_fut,
        pods_fut,
        storage_classes_fut,
        pvs_fut,
        pvcs_fut,
        volume_snapshot_classes_fut,
        volume_snapshots_fut,
        version_fut
    );

    let nodes = soft_unwrap("nodes", nodes);
    let namespaces = soft_unwrap("namespaces", namespaces);
    let deployments = soft_unwrap("deployments", deployments);
    let statefulsets = soft_unwrap("statefulsets", statefulsets);
    let daemonsets = soft_unwrap("daemonsets", daemonsets);
    let pods = soft_unwrap("pods", pods);
    let storage_classes = soft_unwrap("storageclasses", storage_classes);
    let pvs = soft_unwrap("persistentvolumes", pvs);
    let pvcs = soft_unwrap("persistentvolumeclaims", pvcs);
    let volume_snapshot_classes = soft_unwrap("volumesnapshotclasses", volume_snapshot_classes);
    let volume_snapshots = soft_unwrap("volumesnapshots", volume_snapshots);

    let cluster = build_cluster_info(version.ok(), &nodes);
    let ns_infos = namespaces.into_iter().map(map_namespace).collect();
    let workloads = Workloads {
        deployments: deployments.into_iter().map(map_deployment).collect(),
        statefulsets: statefulsets.into_iter().map(map_statefulset).collect(),
        daemonsets: daemonsets.into_iter().map(map_daemonset).collect(),
    };
    let pod_infos = pods.into_iter().map(map_pod).collect();
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

    Ok((cluster, ns_infos, workloads, pod_infos, storage))
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
