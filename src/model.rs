//! DTOs sent to the Sentinella Hub. Keep field names stable — the Hub depends on them.

use serde::{Deserialize, Serialize};

/// Top-level inventory snapshot.
#[derive(Serialize, Debug)]
pub struct InventorySnapshot {
    pub schema_version: u32,
    pub agent: AgentInfo,
    pub cluster_id: String,
    pub timestamp_ms: u128,
    pub k8s_uid: Option<String>,
    pub cluster: ClusterInfo,
    pub namespaces: Vec<NamespaceInfo>,
    pub workloads: Workloads,
    pub pods: Vec<PodInfo>,
    pub network: NetworkInventory,
    pub security: SecurityInventory,
    pub operational_maturity: OperationalMaturityInventory,
    pub dependencies: DependencyInventory,
    pub configuration: ConfigurationInventory,
    pub storage: StorageInventory,
    pub events: Vec<EventInfo>,
    /// Pod-metrics availability state. Always present so the Hub can render a
    /// clear, actionable signal alongside (possibly null) pod usage values.
    /// See `src/collector.rs::probe_pod_metrics` for classification.
    pub metrics: MetricsStatus,
    /// CSI snapshot API availability state. Always present so the Hub can
    /// render a clear, actionable signal when the snapshot CRDs are absent
    /// or the API is unhealthy. See `src/collector.rs::probe_snapshot_api`
    /// for classification.
    pub snapshot_api: SnapshotApiStatus,
}

/// Pod-metrics availability state, surfaced in every snapshot so the Hub can
/// distinguish "metrics-server is not installed" from "ServiceAccount lacks
/// `metrics.k8s.io` RBAC" from a transient failure.
///
/// `state` values:
/// - `ok` — the metrics API returned successfully (with or without items).
/// - `missing` — the metrics API is not registered (404). metrics-server is
///   almost certainly not installed in this cluster.
/// - `forbidden` — the agent's ServiceAccount is not allowed to read
///   `metrics.k8s.io` resources (403). Grant the reader RBAC.
/// - `unavailable` — the metrics API is registered but not ready (503/504).
///   metrics-server is installed but unhealthy; check its pods.
/// - `error` — any other transient or unexpected failure.
///
/// `reason` is a short, actionable one-liner suitable for a UI badge. It is
/// always present for non-`ok` states and omitted on success.
#[derive(Serialize, Debug)]
pub struct MetricsStatus {
    pub state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub source: &'static str,
    pub pod_metrics_count: usize,
    pub last_attempt_at_ms: u128,
}

/// CSI snapshot API availability state, surfaced in every snapshot so the Hub
/// can distinguish "CSI snapshot CRDs are not installed" from "ServiceAccount
/// lacks `snapshot.storage.k8s.io` RBAC" from a transient failure. The
/// `state` and `reason` semantics mirror [`MetricsStatus`].
#[derive(Serialize, Debug)]
pub struct SnapshotApiStatus {
    pub state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub source: &'static str,
    pub volumesnapshotclasses_count: usize,
    pub volumesnapshots_count: usize,
    pub last_attempt_at_ms: u128,
}

#[derive(Deserialize, Debug, Default)]
pub struct SnapshotCreated {
    #[serde(default)]
    pub already_existed: bool,
}

#[derive(Deserialize, Debug, Default)]
pub struct ClusterStatus {
    pub last_seen_at: Option<String>,
    pub k8s_uid: Option<String>,
}

#[derive(Serialize, Debug, Default)]
pub struct DependencyInventory {
    pub edges: Vec<DependencyEdge>,
    pub source: &'static str,
    pub window_seconds: u64,
    pub truncated: bool,
    pub dropped_edges: u64,
}

#[derive(Serialize, Debug)]
pub struct DependencyEdge {
    pub from: DependencyEndpoint,
    pub to: DependencyEndpoint,
    pub protocol: String,
    pub destination_port: u16,
    pub direction: String,
    pub bytes: u64,
    pub packets: u64,
    pub connections: u64,
    pub first_seen_unix_ms: u128,
    pub last_seen_unix_ms: u128,
}

#[derive(Serialize, Debug)]
pub struct DependencyEndpoint {
    pub kind: String,
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub workload_kind: Option<String>,
    pub workload_name: Option<String>,
    pub ip: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct AgentInfo {
    pub name: &'static str,
    pub version: String,
    pub pod_name: String,
    pub pod_namespace: String,
    pub node_name: String,
    pub actions_enabled: bool,
    pub collect_dependencies_tetragon: bool,
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
pub struct OperationalMaturityInventory {
    pub descheduler: DeschedulerInfo,
    pub vpa: VpaInfo,
    pub scheduled_jobs: Vec<ScheduledJobInfo>,
    pub truncated_jobs: bool,
}

#[derive(Serialize, Debug, Default)]
pub struct DeschedulerInfo {
    pub installed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detected_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
}

#[derive(Serialize, Debug, Default)]
pub struct VpaInfo {
    pub installed: bool,
    pub objects_count: usize,
    pub update_modes: Vec<String>,
}

#[derive(Serialize, Debug)]
pub struct ScheduledJobInfo {
    pub namespace: String,
    pub name: String,
    pub schedule: String,
    pub suspend: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_schedule_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_successful_time: Option<String>,
}

#[derive(Serialize, Debug, Default)]
pub struct NetworkInventory {
    pub services: Vec<ServiceInfo>,
    pub ingresses: Vec<IngressInfo>,
}

#[derive(Serialize, Debug, Default)]
pub struct SecurityInventory {
    pub network_policies: Vec<NetworkPolicyInfo>,
    pub cluster_role_bindings: Vec<ClusterRoleBindingInfo>,
    pub pod_security_admission: PodSecurityAdmissionInfo,
}

#[derive(Serialize, Debug)]
pub struct NetworkPolicyInfo {
    pub namespace: String,
    pub name: String,
    pub policy_types: Vec<String>,
    pub pod_selector: Vec<KV>,
    pub ingress_rules_count: usize,
    pub egress_rules_count: usize,
}

#[derive(Serialize, Debug)]
pub struct ClusterRoleBindingInfo {
    pub name: String,
    pub role_ref_name: String,
    pub role_ref_kind: String,
    pub risk_level: String,
    pub subjects: Vec<SecuritySubjectInfo>,
}

#[derive(Serialize, Debug)]
pub struct SecuritySubjectInfo {
    pub kind: String,
    pub name: String,
    pub namespace: Option<String>,
}

#[derive(Serialize, Debug, Default)]
pub struct PodSecurityAdmissionInfo {
    pub namespaces: Vec<PodSecurityNamespaceInfo>,
}

#[derive(Serialize, Debug)]
pub struct PodSecurityNamespaceInfo {
    pub namespace: String,
    pub enforce: Option<String>,
    pub audit: Option<String>,
    pub warn: Option<String>,
}

#[derive(Serialize, Debug, Default)]
pub struct ConfigurationInventory {
    pub configmaps: Vec<ConfigMapInfo>,
    pub secrets: Vec<SecretInfo>,
    pub agent_runtime_env: Vec<KV>,
    pub agent_configured_env: Vec<KV>,
}

#[derive(Serialize, Debug)]
pub struct ConfigMapInfo {
    pub namespace: String,
    pub name: String,
    pub immutable: Option<bool>,
    pub labels: Vec<KV>,
    pub annotations: Vec<KV>,
    pub data_keys: Vec<String>,
    pub binary_data_keys: Vec<String>,
}

#[derive(Serialize, Debug)]
pub struct SecretInfo {
    pub namespace: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub immutable: Option<bool>,
    pub labels: Vec<KV>,
    pub annotations: Vec<KV>,
    pub data_keys: Vec<String>,
}

#[derive(Serialize, Debug)]
pub struct ServiceInfo {
    pub namespace: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub cluster_ip: Option<String>,
    pub external_ips: Vec<String>,
    pub selector: Vec<KV>,
    pub ports: Vec<ServicePortInfo>,
    pub load_balancer_ingress: Vec<String>,
}

#[derive(Serialize, Debug)]
pub struct ServicePortInfo {
    pub name: Option<String>,
    pub protocol: Option<String>,
    pub port: i32,
    pub target_port: Option<String>,
    pub node_port: Option<i32>,
}

#[derive(Serialize, Debug)]
pub struct IngressInfo {
    pub namespace: String,
    pub name: String,
    pub class_name: Option<String>,
    pub hosts: Vec<String>,
    pub rules: Vec<IngressRuleInfo>,
    pub tls: Vec<IngressTlsInfo>,
    pub load_balancer_ingress: Vec<String>,
}

#[derive(Serialize, Debug)]
pub struct IngressRuleInfo {
    pub host: Option<String>,
    pub path: Option<String>,
    pub path_type: Option<String>,
    pub backend_service: Option<String>,
    pub backend_port: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct IngressTlsInfo {
    pub hosts: Vec<String>,
    pub secret_name: Option<String>,
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
    pub age_seconds: Option<u64>,
    pub node: Option<String>,
    pub phase: Option<String>,
    pub usage_cpu: Option<String>,
    pub usage_memory: Option<String>,
    pub owner_kind: Option<String>,
    pub owner_name: Option<String>,
    pub containers: Vec<ContainerInfo>,
}

#[derive(Serialize, Debug)]
pub struct EventInfo {
    pub namespace: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub count: Option<i32>,
    pub first_timestamp: Option<String>,
    pub last_timestamp: Option<String>,
    pub reporting_controller: Option<String>,
    pub reporting_instance: Option<String>,
    pub involved_object: InvolvedObjectInfo,
}

#[derive(Serialize, Debug, Default)]
pub struct InvolvedObjectInfo {
    pub kind: Option<String>,
    pub name: Option<String>,
    pub namespace: Option<String>,
    pub uid: Option<String>,
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

#[derive(Serialize, Debug, Default, Clone, PartialEq, Eq)]
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
/// - `self_update` — requests an immediate agent restart.
///   Spec shape: [`SelfUpdateSpec`].
/// - `update_agent` — updates the agent DaemonSet container image.
///   Spec shape: [`UpdateAgentSpec`].
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

/// Spec payload for `self_update`.
///
/// This command requests an immediate process restart so Kubernetes can
/// recreate the pod. The agent does not mutate workload images or manifests.
#[derive(Deserialize, Debug, Default)]
#[allow(dead_code)]
pub struct SelfUpdateSpec {
    #[serde(default)]
    pub target_version: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub strategy: Option<String>,
}

/// Spec payload for `update_agent`.
///
/// `image` must be an allowed Artifact Registry reference with either a tag
/// or sha256 digest.
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct UpdateAgentSpec {
    pub image: String,
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

    /// Internal control signal used by the runtime loop. When true on a
    /// successful command, the agent exits after ack attempt so Kubernetes can
    /// restart the pod.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_requested: Option<bool>,
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
            restart_requested: None,
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

    #[test]
    fn snapshot_created_deserializes_already_existed() {
        let json = r#"{"already_existed":true}"#;
        let body: SnapshotCreated = serde_json::from_str(json).unwrap();
        assert!(body.already_existed);
    }

    #[test]
    fn snapshot_created_defaults_to_false() {
        let json = r#"{}"#;
        let body: SnapshotCreated = serde_json::from_str(json).unwrap();
        assert!(!body.already_existed);
    }

    #[test]
    fn cluster_status_deserializes_optional_fields() {
        let json = r#"{"last_seen_at":"2026-05-29T16:00:00Z","k8s_uid":"uid-123"}"#;
        let body: ClusterStatus = serde_json::from_str(json).unwrap();
        assert_eq!(body.last_seen_at.as_deref(), Some("2026-05-29T16:00:00Z"));
        assert_eq!(body.k8s_uid.as_deref(), Some("uid-123"));
    }

    #[test]
    fn agent_info_serializes_collect_dependencies_tetragon() {
        let agent = AgentInfo {
            name: "agent",
            version: "1.0.0".into(),
            pod_name: "pod-1".into(),
            pod_namespace: "sentinella".into(),
            node_name: "node-1".into(),
            actions_enabled: false,
            collect_dependencies_tetragon: true,
        };

        let value = serde_json::to_value(&agent).unwrap();
        assert_eq!(value["collect_dependencies_tetragon"], true);
    }

    #[test]
    fn metrics_status_serializes_with_state_and_reason() {
        let s = MetricsStatus {
            state: "missing",
            reason: Some("metrics-server not installed".to_string()),
            source: "metrics.k8s.io/v1",
            pod_metrics_count: 0,
            last_attempt_at_ms: 1748523600000,
        };
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["state"], "missing");
        assert_eq!(v["reason"], "metrics-server not installed");
        assert_eq!(v["source"], "metrics.k8s.io/v1");
        assert_eq!(v["pod_metrics_count"], 0);
        assert_eq!(v["last_attempt_at_ms"].as_u64().unwrap(), 1748523600000);
    }

    #[test]
    fn metrics_status_omits_reason_when_state_is_ok() {
        let s = MetricsStatus {
            state: "ok",
            reason: None,
            source: "metrics.k8s.io/v1",
            pod_metrics_count: 7,
            last_attempt_at_ms: 1748523600000,
        };
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["state"], "ok");
        assert!(
            !v.as_object().unwrap().contains_key("reason"),
            "reason must be omitted on ok"
        );
    }

    #[test]
    fn snapshot_api_status_serializes_with_state_and_reason() {
        let s = SnapshotApiStatus {
            state: "missing",
            reason: Some("CSI snapshot CRDs not installed".to_string()),
            source: "snapshot.storage.k8s.io/v1",
            volumesnapshotclasses_count: 0,
            volumesnapshots_count: 0,
            last_attempt_at_ms: 1748523600000,
        };
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["state"], "missing");
        assert_eq!(v["reason"], "CSI snapshot CRDs not installed");
        assert_eq!(v["source"], "snapshot.storage.k8s.io/v1");
        assert_eq!(v["volumesnapshotclasses_count"], 0);
        assert_eq!(v["volumesnapshots_count"], 0);
        assert_eq!(v["last_attempt_at_ms"].as_u64().unwrap(), 1748523600000);
    }

    #[test]
    fn snapshot_api_status_omits_reason_when_state_is_ok() {
        let s = SnapshotApiStatus {
            state: "ok",
            reason: None,
            source: "snapshot.storage.k8s.io/v1",
            volumesnapshotclasses_count: 3,
            volumesnapshots_count: 5,
            last_attempt_at_ms: 1748523600000,
        };
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["state"], "ok");
        assert!(
            !v.as_object().unwrap().contains_key("reason"),
            "reason must be omitted on ok"
        );
        assert_eq!(v["volumesnapshotclasses_count"], 3);
        assert_eq!(v["volumesnapshots_count"], 5);
    }
}
