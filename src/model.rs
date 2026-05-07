//! DTOs sent to the Sentinella Hub. Keep field names stable — the Hub depends on them.

use serde::{Deserialize, Serialize};

/// Top-level inventory snapshot.
#[derive(Serialize, Debug)]
pub struct InventorySnapshot {
    pub schema_version: u32,
    pub agent: AgentInfo,
    pub cluster_id: String,
    pub timestamp_ms: u128,
    pub cluster: ClusterInfo,
    pub namespaces: Vec<NamespaceInfo>,
    pub workloads: Workloads,
    pub pods: Vec<PodInfo>,
    pub storage: StorageInventory,
}

#[derive(Serialize, Debug)]
pub struct AgentInfo {
    pub name: &'static str,
    pub version: &'static str,
    pub pod_name: String,
    pub pod_namespace: String,
    pub node_name: String,
}

#[derive(Serialize, Debug, Default)]
pub struct ClusterInfo {
    pub kubernetes_version: Option<String>,
    pub platform: Option<String>, // "openshift", "vanilla", "eks", ... if detectable
    pub node_count: usize,
    pub nodes: Vec<NodeInfo>,
}

#[derive(Serialize, Debug)]
pub struct NodeInfo {
    pub name: String,
    pub kubelet_version: Option<String>,
    pub os_image: Option<String>,
    pub container_runtime: Option<String>,
    pub architecture: Option<String>,
    pub capacity_cpu: Option<String>,
    pub capacity_memory: Option<String>,
    pub allocatable_cpu: Option<String>,
    pub allocatable_memory: Option<String>,
    pub ready: bool,
    pub roles: Vec<String>,
}

#[derive(Serialize, Debug)]
pub struct NamespaceInfo {
    pub name: String,
    pub phase: Option<String>,
    pub labels: Vec<KV>,
}

#[derive(Serialize, Debug, Default)]
pub struct Workloads {
    pub deployments: Vec<WorkloadRef>,
    pub statefulsets: Vec<WorkloadRef>,
    pub daemonsets: Vec<WorkloadRef>,
}

#[derive(Serialize, Debug, Default)]
pub struct StorageInventory {
    pub storage_classes: Vec<StorageClassInfo>,
    pub persistent_volumes: Vec<PersistentVolumeInfo>,
    pub persistent_volume_claims: Vec<PersistentVolumeClaimInfo>,
    pub volume_snapshot_classes: Vec<VolumeSnapshotClassInfo>,
    pub volume_snapshots: Vec<VolumeSnapshotInfo>,
}

#[derive(Serialize, Debug)]
pub struct StorageClassInfo {
    pub name: String,
    pub provisioner: String,
    pub parameters: Vec<KV>,
}

#[derive(Serialize, Debug)]
pub struct PersistentVolumeInfo {
    pub name: String,
    pub storage_class: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct PersistentVolumeClaimInfo {
    pub namespace: String,
    pub name: String,
    pub storage_class: Option<String>,
    pub volume_name: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct VolumeSnapshotClassInfo {
    pub name: String,
    pub driver: String,
}

#[derive(Serialize, Debug)]
pub struct VolumeSnapshotInfo {
    pub namespace: String,
    pub name: String,
    pub snapshot_class: Option<String>,
    pub bound_content_name: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct WorkloadRef {
    pub namespace: String,
    pub name: String,
    pub replicas_desired: Option<i32>,
    pub replicas_ready: Option<i32>,
}

#[derive(Serialize, Debug)]
pub struct PodInfo {
    pub namespace: String,
    pub name: String,
    pub node: Option<String>,
    pub phase: Option<String>,
    pub owner_kind: Option<String>,
    pub owner_name: Option<String>,
    pub containers: Vec<ContainerInfo>,
}

#[derive(Serialize, Debug)]
pub struct ContainerInfo {
    pub name: String,
    pub image: String,
    pub image_pull_policy: Option<String>,
    pub technology: Technology,
    pub resources: ResourceSpec,
}

#[derive(Serialize, Debug, Default)]
pub struct ResourceSpec {
    pub requests_cpu: Option<String>,
    pub requests_memory: Option<String>,
    pub limits_cpu: Option<String>,
    pub limits_memory: Option<String>,
}

#[derive(Serialize, Debug, Default)]
pub struct Technology {
    pub vendor: Option<String>,
    pub product: Option<String>,
    pub version: Option<String>,
    pub language: Option<String>,
    pub source: &'static str, // "image" for now; future: "labels", "exec"
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct KV {
    pub key: String,
    pub value: String,
}

// ---------- Command channel ----------

#[derive(Deserialize, Debug)]
pub struct CommandBatch {
    #[serde(default)]
    pub commands: Vec<Command>,
}

/// A command from the Hub. The `kind` field selects the dispatch handler;
/// `spec` carries the kind-specific payload.
///
/// Known kinds (this is a shared contract with the Hub — do not rename):
///
/// - `preview_workload_resources` — server-side dry-run of a resource patch.
///   Returns the patch that would be applied and the observed before/after.
///   Spec shape: [`WorkloadResourcesSpec`].
/// - `apply_workload_resources` — applies the resource patch.
///   Spec shape: [`WorkloadResourcesSpec`].
///
/// The two-command pattern (preview, then apply) is intentional:
/// - Each artifact is a separate Hub record with its own id, timestamp, and
///   audit trail. Easier for dashboards and easier for compliance.
/// - The cluster state may change between preview and apply (HPA scaled, new
///   pods, etc.); the apply re-validates against fresh state rather than
///   relying on a stale preview.
#[derive(Deserialize, Debug)]
pub struct Command {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub spec: serde_json::Value,
}

/// Spec payload for `preview_workload_resources` and `apply_workload_resources`.
///
/// Targets a workload controller (Deployment / StatefulSet / DaemonSet), not a
/// Pod directly. Patching a pod is futile because its controller will recreate
/// it with the original spec.
///
/// Either `requests` or `limits` (or both) must be provided. Omit a side to
/// leave it unchanged; provide an empty map to clear all entries on that side.
/// Individual resource keys cannot be cleared independently in this shape.
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct WorkloadResourcesSpec {
    /// Workload kind: "Deployment", "StatefulSet", or "DaemonSet".
    pub workload_kind: String,
    /// Workload namespace.
    pub namespace: String,
    /// Workload name.
    pub name: String,
    /// Container name within the workload's pod template.
    pub container: String,
    /// New resource values. Omit a side (requests or limits) to leave it
    /// untouched; provide an empty map to remove all entries on that side.
    #[serde(default)]
    pub requests: Option<ResourceMap>,
    #[serde(default)]
    pub limits: Option<ResourceMap>,
}

/// Resource quantity map, e.g. `{"cpu": "500m", "memory": "512Mi"}`.
/// Values follow the standard Kubernetes Quantity format.
#[derive(Deserialize, Debug, Default)]
#[allow(dead_code)]
pub struct ResourceMap {
    #[serde(default)]
    pub cpu: Option<String>,
    #[serde(default)]
    pub memory: Option<String>,
}

/// Result returned to the Hub after a command runs.
///
/// `status` values:
/// - `ok` — handler ran successfully (for previews this means the dry-run
///   completed, not that anything was changed).
/// - `error` — handler failed; `message` carries the cause.
/// - `skipped` — agent refused to execute (e.g. `ACTIONS_ENABLED=false`,
///   safety guard tripped, target controlled by HPA/VPA).
/// - `not_implemented` — the command kind is recognized but its handler is
///   not yet wired up. Distinct from "unknown" so the Hub can tell the
///   difference between "agent too old" and "Hub sent garbage".
/// - `unknown` — the command kind is not recognized.
///
/// For resource patch commands (preview or apply), the audit fields below
/// are populated.
#[derive(Serialize, Debug)]
pub struct CommandResult {
    pub command_id: String,
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub finished_at_ms: u128,

    /// True if the operation was a dry-run (preview) rather than a live apply.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,

    /// The strategic-merge patch the agent computed and sent to the apiserver.
    /// Present for both preview and apply when status is `ok`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_patch: Option<serde_json::Value>,

    /// Observed resource block on the targeted container before the operation.
    /// Lets the Hub render a diff in the dashboard.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_before: Option<serde_json::Value>,

    /// Observed resource block after the operation. For preview, this is the
    /// dry-run response from the apiserver. For apply, this is what the
    /// apiserver actually persisted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_after: Option<serde_json::Value>,

    /// Safety warnings the agent detected (HPA managing the workload, VPA in
    /// Auto mode, LimitRange / ResourceQuota constraints, etc.). Non-fatal —
    /// the Hub may surface these in the dashboard for operator review.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub warnings: Vec<String>,
}

impl CommandResult {
    /// Minimal result for non-patch commands (skipped, unknown, error without
    /// audit data).
    pub fn simple(command_id: String, status: &'static str, message: Option<String>) -> Self {
        Self {
            command_id,
            status,
            message,
            finished_at_ms: now_ms(),
            dry_run: None,
            applied_patch: None,
            observed_before: None,
            observed_after: None,
            warnings: Vec::new(),
        }
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn command_result_simple_fields() {
        let r = CommandResult::simple("cmd-1".into(), "ok", None);
        assert_eq!(r.command_id, "cmd-1");
        assert_eq!(r.status, "ok");
        assert!(r.message.is_none());
        assert!(r.dry_run.is_none());
        assert!(r.applied_patch.is_none());
        assert!(r.warnings.is_empty());
        assert!(r.finished_at_ms > 0);
    }

    #[test]
    fn command_result_simple_with_message() {
        let r = CommandResult::simple("cmd-2".into(), "error", Some("boom".into()));
        assert_eq!(r.message.as_deref(), Some("boom"));
    }

    #[test]
    fn command_result_serialization_omits_none_fields() {
        let r = CommandResult::simple("cmd-3".into(), "skipped", None);
        let v: Value = serde_json::to_value(&r).unwrap();
        assert!(!v.as_object().unwrap().contains_key("message"));
        assert!(!v.as_object().unwrap().contains_key("dry_run"));
        assert!(!v.as_object().unwrap().contains_key("applied_patch"));
        assert!(!v.as_object().unwrap().contains_key("warnings"));
    }

    #[test]
    fn command_result_serialization_includes_warnings() {
        let mut r = CommandResult::simple("cmd-4".into(), "ok", None);
        r.warnings.push("HPA detected".into());
        let v: Value = serde_json::to_value(&r).unwrap();
        let warnings = v["warnings"].as_array().unwrap();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0], "HPA detected");
    }

    #[test]
    fn command_batch_deserializes_empty() {
        let json = r#"{}"#;
        let batch: CommandBatch = serde_json::from_str(json).unwrap();
        assert!(batch.commands.is_empty());
    }

    #[test]
    fn command_batch_deserializes_commands() {
        let json = r#"{
            "commands": [
                {"id": "c1", "type": "preview_workload_resources", "spec": {}}
            ]
        }"#;
        let batch: CommandBatch = serde_json::from_str(json).unwrap();
        assert_eq!(batch.commands.len(), 1);
        assert_eq!(batch.commands[0].id, "c1");
        assert_eq!(batch.commands[0].kind, "preview_workload_resources");
    }

    #[test]
    fn workload_resources_spec_deserializes() {
        let json = r#"{
            "workload_kind": "Deployment",
            "namespace": "prod",
            "name": "api",
            "container": "app",
            "requests": {"cpu": "500m", "memory": "256Mi"},
            "limits": null
        }"#;
        let spec: WorkloadResourcesSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.workload_kind, "Deployment");
        assert_eq!(spec.namespace, "prod");
        let req = spec.requests.unwrap();
        assert_eq!(req.cpu.as_deref(), Some("500m"));
        assert_eq!(req.memory.as_deref(), Some("256Mi"));
    }

    #[test]
    fn workload_resources_spec_omitted_requests_is_none() {
        let json = r#"{
            "workload_kind": "StatefulSet",
            "namespace": "default",
            "name": "db",
            "container": "postgres"
        }"#;
        let spec: WorkloadResourcesSpec = serde_json::from_str(json).unwrap();
        assert!(spec.requests.is_none());
        assert!(spec.limits.is_none());
    }
}
