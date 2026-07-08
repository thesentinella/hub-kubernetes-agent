//! Command executor.
//!
//! `actions_enabled = false` is the default and the executor refuses to
//! execute any command while in read-only mode.

use crate::config::Config;
use crate::model::{
    ActionVerification, Command, CommandResult, ExecutionMode, PostgresqlDiagnosticSpec,
    ResourceMap, RolloutRestartSpec, ScaleSpec, SelfUpdateSpec, SentinellaHubActionPolicy,
    SentinellaHubActionPolicyCondition, SentinellaHubActionPolicyLimits,
    SentinellaHubActionPolicyStatus, UpdateAgentSpec, WorkloadResourcesSpec, WorkloadTargetRef,
    parse_cpu_quantity, parse_memory_quantity, policy_action_is_supported,
    policy_action_targets_workload, policy_resource_is_supported, policy_status_is_stale,
};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::api::core::v1::ResourceRequirements;
use k8s_openapi::api::core::v1::{LimitRange, ResourceQuota};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::api::{ApiResource, DynamicObject, ListParams, Patch, PatchParams};
use kube::core::GroupVersionKind;
use kube::{Api, Client};
use once_cell::sync::Lazy;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::time::sleep;
use tracing::{info, warn};

const UPDATE_AGENT_ALLOWED_PREFIX: &str =
    "us-east1-docker.pkg.dev/sentinella-hub/kubernetes-agent/";
const UPDATE_AGENT_NAMESPACE: &str = "sentinella";
const UPDATE_AGENT_DAEMONSET: &str = "sentinella-hub-k8s-agent";
const UPDATE_AGENT_CONTAINER: &str = "agent";
const ACTION_POLICY_GROUP: &str = "sentinella.io";
const ACTION_POLICY_VERSION: &str = "v1alpha1";
const ACTION_POLICY_KIND: &str = "SentinellaHubActionPolicy";
pub(crate) const ACTION_MODE_NAMESPACE_LABEL: &str = "sentinella.io/action-mode";
pub(crate) const ACTION_MODE_NAMESPACE_LABEL_ENABLED: &str = "enabled";

#[derive(Default)]
struct CommandDedupState {
    completed: HashMap<String, CommandResult>,
    running: HashSet<String>,
}

static COMMAND_DEDUP: Lazy<Mutex<CommandDedupState>> =
    Lazy::new(|| Mutex::new(CommandDedupState::default()));

#[allow(dead_code)]
struct EffectivePolicy {
    name: String,
    policy: SentinellaHubActionPolicy,
    status: SentinellaHubActionPolicyStatus,
}

pub struct Executor {
    cfg: Config,
    client: Client,
}

#[allow(dead_code)]
impl Executor {
    pub fn new(cfg: Config, client: Client) -> Self {
        Self { cfg, client }
    }

    pub async fn execute(&self, cmd: &Command) -> CommandResult {
        if let Some(result) = dedup_begin(cmd) {
            return result;
        }

        let result = self.execute_inner(cmd).await;
        dedup_finish(&result);
        result
    }

    async fn execute_inner(&self, cmd: &Command) -> CommandResult {
        match cmd.kind.as_str() {
            "diagnose_postgresql" => {
                if !self.cfg.readonly_commands_enabled {
                    warn!(command_id = %cmd.id, kind = %cmd.kind, "read-only commands disabled; skipping");
                    return CommandResult::simple(
                        cmd.id.clone(),
                        "skipped",
                        Some(
                            "agent read-only commands disabled (READONLY_COMMANDS_ENABLED=false)"
                                .into(),
                        ),
                    );
                }

                match parse_spec::<PostgresqlDiagnosticSpec>(cmd) {
                    Ok(spec) => self.diagnose_postgresql(&cmd.id, spec).await,
                    Err(e) => spec_error(cmd, e),
                }
            }
            "preview_workload_resources" => {
                if !self.cfg.actions_enabled {
                    warn!(command_id = %cmd.id, kind = %cmd.kind, "actions disabled; skipping");
                    return CommandResult::simple(
                        cmd.id.clone(),
                        "skipped",
                        Some("agent in read-only mode (ACTIONS_ENABLED=false)".into()),
                    );
                }

                match parse_spec::<WorkloadResourcesSpec>(cmd) {
                    Ok(spec) => self.preview_workload_resources(&cmd.id, spec).await,
                    Err(e) => spec_error(cmd, e),
                }
            }
            "apply_workload_resources" => {
                if !self.cfg.actions_enabled {
                    warn!(command_id = %cmd.id, kind = %cmd.kind, "actions disabled; skipping");
                    return CommandResult::simple(
                        cmd.id.clone(),
                        "skipped",
                        Some("agent in read-only mode (ACTIONS_ENABLED=false)".into()),
                    );
                }

                match parse_spec::<WorkloadResourcesSpec>(cmd) {
                    Ok(spec) => self.apply_workload_resources(&cmd.id, spec).await,
                    Err(e) => spec_error(cmd, e),
                }
            }
            "rollout_restart" => {
                if !self.cfg.actions_enabled {
                    warn!(command_id = %cmd.id, kind = %cmd.kind, "actions disabled; skipping");
                    return CommandResult::simple(
                        cmd.id.clone(),
                        "skipped",
                        Some("agent in read-only mode (ACTIONS_ENABLED=false)".into()),
                    );
                }

                match parse_spec::<RolloutRestartSpec>(cmd) {
                    Ok(spec) => self.rollout_restart(&cmd.id, spec).await,
                    Err(e) => spec_error(cmd, e),
                }
            }
            "scale" => {
                if !self.cfg.actions_enabled {
                    warn!(command_id = %cmd.id, kind = %cmd.kind, "actions disabled; skipping");
                    return CommandResult::simple(
                        cmd.id.clone(),
                        "skipped",
                        Some("agent in read-only mode (ACTIONS_ENABLED=false)".into()),
                    );
                }

                match parse_spec::<ScaleSpec>(cmd) {
                    Ok(spec) => self.scale_workload(&cmd.id, spec).await,
                    Err(e) => spec_error(cmd, e),
                }
            }
            "self_update" => {
                if !self.cfg.actions_enabled {
                    warn!(command_id = %cmd.id, kind = %cmd.kind, "actions disabled; skipping");
                    return CommandResult::simple(
                        cmd.id.clone(),
                        "skipped",
                        Some("agent in read-only mode (ACTIONS_ENABLED=false)".into()),
                    );
                }

                match parse_spec::<SelfUpdateSpec>(cmd) {
                    Ok(spec) => self.self_update(&cmd.id, spec).await,
                    Err(e) => spec_error(cmd, e),
                }
            }
            "update_agent" => {
                if !self.cfg.actions_enabled {
                    warn!(command_id = %cmd.id, kind = %cmd.kind, "actions disabled; skipping");
                    return CommandResult::simple(
                        cmd.id.clone(),
                        "skipped",
                        Some("agent in read-only mode (ACTIONS_ENABLED=false)".into()),
                    );
                }

                match parse_spec::<UpdateAgentSpec>(cmd) {
                    Ok(spec) => self.update_agent(&cmd.id, spec).await,
                    Err(e) => spec_error(cmd, e),
                }
            }
            other => {
                info!(command_id = %cmd.id, kind = %other, "unknown command kind");
                CommandResult::simple(
                    cmd.id.clone(),
                    "unknown",
                    Some(format!("unknown command kind: {}", other)),
                )
            }
        }
    }

    // -------- Handlers --------
    //
    // v0.2 implementation plan for both handlers:
    //
    //   1. Resolve the workload via kube::Api<{Deployment|StatefulSet|DaemonSet}>
    //      in the target namespace.
    //   2. Locate the named container in spec.template.spec.containers; reject
    //      if not found.
    //   3. Pre-flight safety checks (collect into `warnings`, do not fatally
    //      reject unless dangerous):
    //         - HPA targeting this workload? -> warn (CPU/memory targets
    //           interact with requests).
    //         - VPA in `Auto` or `Recreate` mode targeting this workload? ->
    //           warn loudly (we will fight the VPA).
    //         - Namespace LimitRange admits the new values? (test by computing
    //           min/max/default; reject pre-flight rather than let the
    //           apiserver reject the patch).
    //         - Namespace ResourceQuota has headroom? (compute delta vs.
    //           current usage).
    //         - PodDisruptionBudget would block the rolling restart? -> warn.
    //   4. Build the strategic-merge patch:
    //        {"spec":{"template":{"spec":{"containers":[
    //          {"name":"<container>", "resources":{
    //             "requests": {...}, "limits": {...}
    //          }}
    //        ]}}}}
    //      Strategic merge is critical here — JSON merge would clobber the
    //      whole containers array.
    //   5. Capture `observed_before` from the live workload (the resources
    //      block of the targeted container).
    //   6. Send the patch.
    //        - preview: with `?dryRun=All` query param. Returns the would-be
    //          state from the apiserver, including admission webhook output.
    //        - apply: without dryRun.
    //   7. Capture `observed_after` from the response.
    //   8. Return CommandResult with applied_patch, observed_before,
    //      observed_after, warnings.
    //
    // For the apply case, the rolling restart that follows is the workload
    // controller's responsibility — we do not wait for it.

    async fn preview_workload_resources(
        &self,
        command_id: &str,
        spec: WorkloadResourcesSpec,
    ) -> CommandResult {
        info!(
            command_id,
            "preview_workload_resources: dry-run patching workload"
        );

        match self.execute_workload_resources_inner(&spec, true).await {
            Ok(preview) => {
                let mut r = CommandResult::simple(command_id.to_string(), "ok", None);
                r.dry_run = Some(true);
                r.applied_patch = Some(preview.patch);
                r.observed_before = Some(preview.observed_before);
                r.observed_after = Some(preview.observed_after);
                r.warnings = preview.warnings;
                r
            }
            Err(message) => {
                warn!(command_id, "preview_workload_resources failed: {message}");
                let mut r = CommandResult::simple(command_id.to_string(), "error", Some(message));
                r.dry_run = Some(true);
                r
            }
        }
    }

    async fn execute_workload_resources_inner(
        &self,
        spec: &WorkloadResourcesSpec,
        dry_run: bool,
    ) -> Result<ResourcePreview, String> {
        self.ensure_effective_policy_allows(
            &spec.namespace,
            if dry_run {
                "preview_workload_resources"
            } else {
                "apply_workload_resources"
            },
            Some(spec.workload_kind.as_str()),
            Some(spec),
        )
        .await?;

        let patch = build_workload_resources_patch(spec)?;
        let pp = if dry_run {
            PatchParams::default().dry_run()
        } else {
            PatchParams::default()
        };
        let op_name = if dry_run {
            "dry-run patch"
        } else {
            "apply patch"
        };

        match spec.workload_kind.as_str() {
            "Deployment" => {
                let api: Api<Deployment> = Api::namespaced(self.client.clone(), &spec.namespace);
                let before = api
                    .get(&spec.name)
                    .await
                    .map_err(|e| format!("failed to get Deployment {}: {}", spec.name, e))?;
                let observed_before = deployment_container_resources(&before, &spec.container)?;
                let pod_labels = deployment_pod_labels(&before);
                let warnings = self.collect_preflight_warnings(spec, &pod_labels).await;
                let after = api
                    .patch(&spec.name, &pp, &Patch::Strategic(&patch))
                    .await
                    .map_err(|e| {
                        format!("{} failed for Deployment {}: {}", op_name, spec.name, e)
                    })?;
                let observed_after = deployment_container_resources(&after, &spec.container)?;
                Ok(ResourcePreview::new(
                    patch,
                    observed_before,
                    observed_after,
                    warnings,
                ))
            }
            "StatefulSet" => {
                let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &spec.namespace);
                let before = api
                    .get(&spec.name)
                    .await
                    .map_err(|e| format!("failed to get StatefulSet {}: {}", spec.name, e))?;
                let observed_before = statefulset_container_resources(&before, &spec.container)?;
                let pod_labels = statefulset_pod_labels(&before);
                let warnings = self.collect_preflight_warnings(spec, &pod_labels).await;
                let after = api
                    .patch(&spec.name, &pp, &Patch::Strategic(&patch))
                    .await
                    .map_err(|e| {
                        format!("{} failed for StatefulSet {}: {}", op_name, spec.name, e)
                    })?;
                let observed_after = statefulset_container_resources(&after, &spec.container)?;
                Ok(ResourcePreview::new(
                    patch,
                    observed_before,
                    observed_after,
                    warnings,
                ))
            }
            "DaemonSet" => {
                let api: Api<DaemonSet> = Api::namespaced(self.client.clone(), &spec.namespace);
                let before = api
                    .get(&spec.name)
                    .await
                    .map_err(|e| format!("failed to get DaemonSet {}: {}", spec.name, e))?;
                let observed_before = daemonset_container_resources(&before, &spec.container)?;
                let pod_labels = daemonset_pod_labels(&before);
                let warnings = self.collect_preflight_warnings(spec, &pod_labels).await;
                let after = api
                    .patch(&spec.name, &pp, &Patch::Strategic(&patch))
                    .await
                    .map_err(|e| {
                        format!("{} failed for DaemonSet {}: {}", op_name, spec.name, e)
                    })?;
                let observed_after = daemonset_container_resources(&after, &spec.container)?;
                Ok(ResourcePreview::new(
                    patch,
                    observed_before,
                    observed_after,
                    warnings,
                ))
            }
            other => Err(format!(
                "unsupported workload_kind {}; expected Deployment, StatefulSet, or DaemonSet",
                other
            )),
        }
    }

    async fn ensure_namespace_action_mode_enabled(&self, namespace: &str) -> Result<(), String> {
        let api: Api<Namespace> = Api::all(self.client.clone());
        let namespace = api
            .get(namespace)
            .await
            .map_err(|e| format!("failed to get Namespace {}: {}", namespace, e))?;

        namespace_action_mode_enabled(&namespace)
    }

    async fn collect_preflight_warnings(
        &self,
        spec: &WorkloadResourcesSpec,
        pod_labels: &BTreeMap<String, String>,
    ) -> Vec<String> {
        let mut warnings = Vec::new();

        merge_check(&mut warnings, "hpa", self.preflight_check_hpa(spec).await);
        merge_check(&mut warnings, "vpa", self.preflight_check_vpa(spec).await);
        merge_check(
            &mut warnings,
            "limitrange",
            self.preflight_check_limitrange(spec).await,
        );
        merge_check(
            &mut warnings,
            "resourcequota",
            self.preflight_check_resourcequota(spec).await,
        );
        merge_check(
            &mut warnings,
            "pdb",
            self.preflight_check_pdb(spec, pod_labels).await,
        );

        warnings
    }

    async fn preflight_check_hpa(
        &self,
        spec: &WorkloadResourcesSpec,
    ) -> Result<Vec<String>, String> {
        let api: Api<HorizontalPodAutoscaler> =
            Api::namespaced(self.client.clone(), &spec.namespace);
        let list = api
            .list(&ListParams::default())
            .await
            .map_err(|e| e.to_string())?;

        let mut names: Vec<String> = list
            .into_iter()
            .filter_map(|hpa| {
                let target = hpa.spec.as_ref().map(|s| &s.scale_target_ref)?;
                if target.kind == spec.workload_kind && target.name == spec.name {
                    Some(hpa.metadata.name.unwrap_or_else(|| "<unknown>".into()))
                } else {
                    None
                }
            })
            .collect();
        names.sort();

        Ok(names
            .into_iter()
            .map(|name| {
                format!(
                    "preflight.hpa.targeted: HPA {} targets {}/{}; resource changes may affect autoscaling behavior",
                    name, spec.workload_kind, spec.name
                )
            })
            .collect())
    }

    async fn preflight_check_vpa(
        &self,
        spec: &WorkloadResourcesSpec,
    ) -> Result<Vec<String>, String> {
        let gvk = GroupVersionKind::gvk("autoscaling.k8s.io", "v1", "VerticalPodAutoscaler");
        let ar = ApiResource::from_gvk(&gvk);
        let api: Api<DynamicObject> =
            Api::namespaced_with(self.client.clone(), &spec.namespace, &ar);
        let list = api
            .list(&ListParams::default())
            .await
            .map_err(|e| e.to_string())?;

        let mut warnings = Vec::new();
        for vpa in list {
            let name = vpa
                .metadata
                .name
                .as_deref()
                .unwrap_or("<unknown>")
                .to_string();
            let as_value = serde_json::to_value(&vpa).map_err(|e| e.to_string())?;

            let target_kind = as_value
                .pointer("/spec/targetRef/kind")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let target_name = as_value
                .pointer("/spec/targetRef/name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if target_kind != spec.workload_kind || target_name != spec.name {
                continue;
            }

            let mode = as_value
                .pointer("/spec/updatePolicy/updateMode")
                .and_then(Value::as_str);
            if vpa_mode_is_conflicting(mode) {
                let mode = mode.unwrap_or("Auto");
                warnings.push(format!(
                    "preflight.vpa.auto_mode: VPA {} targets {}/{} with updateMode={}; manual resources may be overwritten",
                    name, spec.workload_kind, spec.name, mode
                ));
            }
        }

        warnings.sort();
        Ok(warnings)
    }

    async fn preflight_check_limitrange(
        &self,
        spec: &WorkloadResourcesSpec,
    ) -> Result<Vec<String>, String> {
        let api: Api<LimitRange> = Api::namespaced(self.client.clone(), &spec.namespace);
        let list = api
            .list(&ListParams::default())
            .await
            .map_err(|e| e.to_string())?;
        if list.items.is_empty() {
            return Ok(Vec::new());
        }

        Ok(vec![format!(
            "preflight.limitrange.present: namespace {} has {} LimitRange object(s); requested values may be constrained",
            spec.namespace,
            list.items.len()
        )])
    }

    async fn preflight_check_resourcequota(
        &self,
        spec: &WorkloadResourcesSpec,
    ) -> Result<Vec<String>, String> {
        let api: Api<ResourceQuota> = Api::namespaced(self.client.clone(), &spec.namespace);
        let list = api
            .list(&ListParams::default())
            .await
            .map_err(|e| e.to_string())?;
        if list.items.is_empty() {
            return Ok(Vec::new());
        }

        Ok(vec![format!(
            "preflight.resourcequota.present: namespace {} has {} ResourceQuota object(s); requested values may exceed quota",
            spec.namespace,
            list.items.len()
        )])
    }

    async fn preflight_check_pdb(
        &self,
        spec: &WorkloadResourcesSpec,
        pod_labels: &BTreeMap<String, String>,
    ) -> Result<Vec<String>, String> {
        let api: Api<PodDisruptionBudget> = Api::namespaced(self.client.clone(), &spec.namespace);
        let list = api
            .list(&ListParams::default())
            .await
            .map_err(|e| e.to_string())?;

        let mut names: Vec<String> = list
            .into_iter()
            .filter_map(|pdb| {
                let selector = pdb.spec.as_ref().and_then(|s| s.selector.as_ref());
                if label_selector_matches(selector, pod_labels) {
                    Some(pdb.metadata.name.unwrap_or_else(|| "<unknown>".into()))
                } else {
                    None
                }
            })
            .collect();
        names.sort();

        Ok(names
            .into_iter()
            .map(|name| {
                format!(
                    "preflight.pdb.selector_overlap: PDB {} selector matches {}/{} pod labels; rollout may be constrained",
                    name, spec.workload_kind, spec.name
                )
            })
            .collect())
    }

    async fn apply_workload_resources(
        &self,
        command_id: &str,
        spec: WorkloadResourcesSpec,
    ) -> CommandResult {
        info!(command_id, "apply_workload_resources: patching workload");

        match self.execute_workload_resources_inner(&spec, false).await {
            Ok(applied) => {
                let mut r = CommandResult::simple(command_id.to_string(), "ok", None);
                r.dry_run = Some(false);
                r.applied_patch = Some(applied.patch);
                r.observed_before = Some(applied.observed_before);
                r.observed_after = Some(applied.observed_after);
                r.warnings = applied.warnings;
                r
            }
            Err(message) => {
                warn!(command_id, "apply_workload_resources failed: {message}");
                let mut r = CommandResult::simple(command_id.to_string(), "error", Some(message));
                r.dry_run = Some(false);
                r
            }
        }
    }

    async fn rollout_restart(&self, command_id: &str, spec: RolloutRestartSpec) -> CommandResult {
        if let Err(message) = self
            .ensure_effective_policy_allows(
                &spec.target.namespace,
                "rollout_restart",
                Some(spec.target.kind.as_str()),
                None,
            )
            .await
        {
            return command_error(command_id, spec.execution.mode, message);
        }

        let restart_at = now_rfc3339();
        let patch = rollout_restart_patch(&spec.target, &restart_at);

        let mut result = CommandResult::simple(command_id.to_string(), "ok", None);
        result.requested_state = Some(json!({
            "execution": {"mode": spec.execution.mode},
            "target": {
                "kind": spec.target.kind,
                "namespace": spec.target.namespace,
                "name": spec.target.name,
            },
            "restartedAt": restart_at,
        }));

        let workload = spec.target.kind.as_str();
        let dry_run = self
            .apply_rollout_restart_patch(
                workload,
                &spec.target.namespace,
                &spec.target.name,
                &patch,
                true,
            )
            .await;
        let observed_before = match dry_run {
            Ok(before) => before,
            Err(message) => return command_error(command_id, spec.execution.mode, message),
        };

        result.dry_run = Some(true);
        result.applied_patch = Some(patch.clone());
        result.observed_before = Some(observed_before.clone());
        result.observed_after = Some(observed_before.clone());

        if matches!(spec.execution.mode, ExecutionMode::Preview) {
            return result;
        }

        let applied = match self
            .apply_rollout_restart_patch(
                workload,
                &spec.target.namespace,
                &spec.target.name,
                &patch,
                false,
            )
            .await
        {
            Ok(after) => after,
            Err(message) => return command_error(command_id, spec.execution.mode, message),
        };

        result.dry_run = Some(false);
        result.observed_after = Some(applied.clone());

        match self
            .wait_for_rollout_completion(workload, &spec.target.namespace, &spec.target.name)
            .await
        {
            Ok(verification) => {
                result.verification = Some(verification);
                result
            }
            Err(message) => {
                result.status = "error";
                result.message = Some(message);
                result.verification = Some(ActionVerification {
                    status: "failed".into(),
                    message: Some("rollout verification failed".into()),
                    observed_state: None,
                });
                result
            }
        }
    }

    async fn scale_workload(&self, command_id: &str, spec: ScaleSpec) -> CommandResult {
        if let Err(message) = self
            .ensure_effective_policy_allows(
                &spec.target.namespace,
                "scale",
                Some(spec.target.kind.as_str()),
                None,
            )
            .await
        {
            return command_error(command_id, spec.execution.mode, message);
        }

        if spec.replicas < 0 {
            return command_error(
                command_id,
                spec.execution.mode,
                "scale replicas must be greater than or equal to 0".into(),
            );
        }

        if spec.target.kind != "Deployment" {
            return command_error(
                command_id,
                spec.execution.mode,
                format!(
                    "unsupported scale target kind {}; expected Deployment",
                    spec.target.kind
                ),
            );
        }

        let api: Api<Deployment> = Api::namespaced(self.client.clone(), &spec.target.namespace);
        let before = match api.get(&spec.target.name).await {
            Ok(workload) => workload,
            Err(e) => {
                return command_error(
                    command_id,
                    spec.execution.mode,
                    format!("failed to get Deployment {}: {}", spec.target.name, e),
                );
            }
        };
        let before_scale = match api.get_scale(&spec.target.name).await {
            Ok(scale) => scale,
            Err(e) => {
                return command_error(
                    command_id,
                    spec.execution.mode,
                    format!(
                        "failed to get scale for Deployment {}: {}",
                        spec.target.name, e
                    ),
                );
            }
        };

        let mut result = CommandResult::simple(command_id.to_string(), "ok", None);
        result.requested_state = Some(json!({
            "execution": {"mode": spec.execution.mode},
            "target": {
                "kind": spec.target.kind,
                "namespace": spec.target.namespace,
                "name": spec.target.name,
            },
            "replicas": spec.replicas,
        }));
        result.dry_run = Some(matches!(spec.execution.mode, ExecutionMode::Preview));
        result.observed_before = Some(json!({
            "replicas": before_scale.spec.as_ref().map(|s| s.replicas).unwrap_or_default(),
            "ready_replicas": before.status.as_ref().map(|s| s.ready_replicas).unwrap_or_default(),
            "available_replicas": before.status.as_ref().map(|s| s.available_replicas).unwrap_or_default(),
        }));

        let patch = json!({"spec": {"replicas": spec.replicas}});
        let dry_run = match api
            .patch_scale(
                &spec.target.name,
                &PatchParams::default().dry_run(),
                &Patch::Merge(&patch),
            )
            .await
        {
            Ok(scale) => scale,
            Err(e) => {
                return command_error(
                    command_id,
                    spec.execution.mode,
                    format!(
                        "scale dry-run failed for Deployment {}: {}",
                        spec.target.name, e
                    ),
                );
            }
        };
        result.applied_patch = Some(json!({"spec": {"replicas": spec.replicas}}));
        result.observed_after = Some(json!({
            "desired_replicas": dry_run.spec.as_ref().and_then(|s| s.replicas).unwrap_or_default(),
            "current_replicas": dry_run.status.as_ref().map(|s| s.replicas).unwrap_or_default(),
        }));

        if matches!(spec.execution.mode, ExecutionMode::Preview) {
            return result;
        }

        let applied = match api
            .patch_scale(
                &spec.target.name,
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await
        {
            Ok(scale) => scale,
            Err(e) => {
                return command_error(
                    command_id,
                    spec.execution.mode,
                    format!(
                        "scale apply failed for Deployment {}: {}",
                        spec.target.name, e
                    ),
                );
            }
        };

        result.dry_run = Some(false);
        result.observed_after = Some(json!({
            "desired_replicas": applied.spec.as_ref().and_then(|s| s.replicas).unwrap_or_default(),
            "current_replicas": applied.status.as_ref().map(|s| s.replicas).unwrap_or_default(),
        }));
        result.verification = Some(ActionVerification {
            status: "accepted".into(),
            message: Some("scale request applied; stabilization is tracked separately".into()),
            observed_state: Some(json!({
                "desired_replicas": applied.spec.as_ref().and_then(|s| s.replicas).unwrap_or_default(),
                "current_replicas": applied.status.as_ref().map(|s| s.replicas).unwrap_or_default(),
            })),
        });
        result
    }

    async fn ensure_effective_policy_ready(
        &self,
        namespace: &str,
    ) -> Result<EffectivePolicy, String> {
        let policies = self.list_action_policies().await?;
        let stale_after = self
            .cfg
            .action_operator_poll_interval
            .checked_mul(3)
            .unwrap_or(self.cfg.action_operator_poll_interval);
        let stale_after_ms = stale_after.as_millis();
        let now = now_ms();

        let mut reasons = Vec::new();
        for policy in policies {
            let Some(policy_name) = policy.metadata.name.clone() else {
                continue;
            };
            let Some(status) = policy.status.clone() else {
                reasons.push(format!("policy {} has no status", policy_name));
                continue;
            };

            if let Some(reason) =
                Self::policy_status_rejection_reason(&policy_name, &status, now, stale_after_ms)
            {
                reasons.push(reason);
                continue;
            }

            if !condition_true(&status.conditions, "Ready") {
                reasons.push(format!("policy {} is not Ready", policy_name));
                continue;
            }

            if !status
                .effective_namespaces
                .iter()
                .any(|candidate| candidate == namespace)
            {
                continue;
            }

            return Ok(EffectivePolicy {
                name: policy_name,
                policy,
                status,
            });
        }

        Err(format!(
            "namespace {} is not eligible for action: {}",
            namespace,
            if reasons.is_empty() {
                "no Ready policy matched the namespace".into()
            } else {
                reasons.join("; ")
            }
        ))
    }

    async fn ensure_effective_policy_allows(
        &self,
        namespace: &str,
        command_kind: &str,
        target_kind: Option<&str>,
        resource_spec: Option<&WorkloadResourcesSpec>,
    ) -> Result<EffectivePolicy, String> {
        let policies = self.list_action_policies().await?;
        let stale_after = self
            .cfg
            .action_operator_poll_interval
            .checked_mul(3)
            .unwrap_or(self.cfg.action_operator_poll_interval);
        let stale_after_ms = stale_after.as_millis();
        let now = now_ms();

        let mut reasons = Vec::new();
        for policy in policies {
            let Some(policy_name) = policy.metadata.name.clone() else {
                continue;
            };
            let Some(status) = policy.status.clone() else {
                reasons.push(format!("policy {} has no status", policy_name));
                continue;
            };

            let Some(last_reconciled_at_ms) = status.last_reconciled_at_ms else {
                reasons.push(format!(
                    "policy {} status has no freshness timestamp",
                    policy_name
                ));
                continue;
            };

            if policy_status_is_stale(Some(last_reconciled_at_ms), now, stale_after_ms) {
                reasons.push(format!("policy {} status is stale", policy_name));
                continue;
            }

            if !condition_true(&status.conditions, "Ready") {
                reasons.push(format!("policy {} is not Ready", policy_name));
                continue;
            }

            if !status
                .effective_namespaces
                .iter()
                .any(|candidate| candidate == namespace)
            {
                continue;
            }

            if let Some(reason) = Self::policy_rejection_reason_for_command(
                &policy,
                command_kind,
                target_kind,
                resource_spec,
            ) {
                reasons.push(format!("policy {} {}", policy_name, reason));
                continue;
            }

            return Ok(EffectivePolicy {
                name: policy_name,
                policy,
                status,
            });
        }

        Err(format!(
            "namespace {} is not eligible for action: {}",
            namespace,
            if reasons.is_empty() {
                "no Ready policy matched the namespace".into()
            } else {
                reasons.join("; ")
            }
        ))
    }

    async fn list_action_policies(&self) -> Result<Vec<SentinellaHubActionPolicy>, String> {
        let gvk = GroupVersionKind::gvk(
            ACTION_POLICY_GROUP,
            ACTION_POLICY_VERSION,
            ACTION_POLICY_KIND,
        );
        let ar = ApiResource::from_gvk(&gvk);
        let api: Api<DynamicObject> = Api::all_with(self.client.clone(), &ar);
        let list = api
            .list(&ListParams::default())
            .await
            .map_err(|e| format!("failed to list {}: {}", ACTION_POLICY_KIND, e))?;

        let mut out = Vec::new();
        for policy in list {
            let value = serde_json::to_value(&policy).map_err(|e| e.to_string())?;
            let policy: SentinellaHubActionPolicy = serde_json::from_value(value)
                .map_err(|e| format!("failed to deserialize {}: {}", ACTION_POLICY_KIND, e))?;
            out.push(policy);
        }

        Ok(out)
    }

    fn policy_rejection_reason_for_command(
        policy: &SentinellaHubActionPolicy,
        command_kind: &str,
        target_kind: Option<&str>,
        resource_spec: Option<&WorkloadResourcesSpec>,
    ) -> Option<String> {
        if !policy
            .spec
            .allowed_actions
            .iter()
            .all(|action| policy_action_is_supported(action))
            || policy.spec.allowed_actions.is_empty()
        {
            return Some("does not declare any supported actions".into());
        }

        if !policy
            .spec
            .allowed_actions
            .iter()
            .any(|action| action == command_kind)
        {
            return Some(format!("does not allow action {}", command_kind));
        }

        if policy_action_targets_workload(command_kind) {
            let Some(target_kind) = target_kind else {
                return Some("is missing a workload target kind".into());
            };

            if !policy
                .spec
                .allowed_resources
                .iter()
                .all(|resource| policy_resource_is_supported(resource))
                || policy.spec.allowed_resources.is_empty()
            {
                return Some("does not declare any supported workload resources".into());
            }

            if !policy
                .spec
                .allowed_resources
                .iter()
                .any(|resource| resource == target_kind)
            {
                return Some(format!("does not allow resource {}", target_kind));
            }
        }

        if matches!(
            command_kind,
            "preview_workload_resources" | "apply_workload_resources"
        ) {
            let Some(spec) = resource_spec else {
                return Some("is missing a workload resources spec".into());
            };

            if let Some(reason) =
                Self::policy_limits_violation_reason(policy.spec.limits.as_ref(), spec)
            {
                return Some(reason);
            }
        }

        None
    }

    fn policy_limits_violation_reason(
        limits: Option<&SentinellaHubActionPolicyLimits>,
        spec: &WorkloadResourcesSpec,
    ) -> Option<String> {
        let limits = limits?;

        let mut violations = Vec::new();
        if let Some(cpu_limit) = limits.max_cpu_limit.as_deref() {
            if let Some(requests) = &spec.requests {
                if let Some(cpu) = requests.cpu.as_deref() {
                    if let Some(reason) = Self::quantity_exceeds_limit(
                        "cpu request",
                        cpu,
                        cpu_limit,
                        parse_cpu_quantity,
                    ) {
                        violations.push(reason);
                    }
                }
            }
            if let Some(limits_map) = &spec.limits {
                if let Some(cpu) = limits_map.cpu.as_deref() {
                    if let Some(reason) = Self::quantity_exceeds_limit(
                        "cpu limit",
                        cpu,
                        cpu_limit,
                        parse_cpu_quantity,
                    ) {
                        violations.push(reason);
                    }
                }
            }
        }

        if let Some(memory_limit) = limits.max_memory_limit.as_deref() {
            if let Some(requests) = &spec.requests {
                if let Some(memory) = requests.memory.as_deref() {
                    if let Some(reason) = Self::quantity_exceeds_limit(
                        "memory request",
                        memory,
                        memory_limit,
                        parse_memory_quantity,
                    ) {
                        violations.push(reason);
                    }
                }
            }
            if let Some(limits_map) = &spec.limits {
                if let Some(memory) = limits_map.memory.as_deref() {
                    if let Some(reason) = Self::quantity_exceeds_limit(
                        "memory limit",
                        memory,
                        memory_limit,
                        parse_memory_quantity,
                    ) {
                        violations.push(reason);
                    }
                }
            }
        }

        if violations.is_empty() {
            None
        } else {
            Some(format!("limits exceeded: {}", violations.join(", ")))
        }
    }

    fn quantity_exceeds_limit(
        label: &str,
        quantity: &str,
        limit: &str,
        parse: fn(&str) -> Option<i128>,
    ) -> Option<String> {
        let quantity_value = parse(quantity).or(Some(i128::MIN))?;
        let limit_value = parse(limit).or(Some(i128::MIN))?;

        if quantity_value == i128::MIN {
            Some(format!("{} {} is not a valid quantity", label, quantity))
        } else if limit_value == i128::MIN {
            Some(format!("policy limit {} is not a valid quantity", limit))
        } else if quantity_value > limit_value {
            Some(format!("{} {} exceeds max {}", label, quantity, limit))
        } else {
            None
        }
    }

    fn policy_status_rejection_reason(
        policy_name: &str,
        status: &SentinellaHubActionPolicyStatus,
        now_ms: u128,
        stale_after_ms: u128,
    ) -> Option<String> {
        if status.stale {
            return Some(format!("policy {} status is stale", policy_name));
        }

        let Some(last_reconciled_at_ms) = status.last_reconciled_at_ms else {
            return Some(format!(
                "policy {} status has no freshness timestamp",
                policy_name
            ));
        };

        if policy_status_is_stale(Some(last_reconciled_at_ms), now_ms, stale_after_ms) {
            return Some(format!("policy {} status is stale", policy_name));
        }

        None
    }

    async fn apply_rollout_restart_patch(
        &self,
        kind: &str,
        namespace: &str,
        name: &str,
        patch: &Value,
        dry_run: bool,
    ) -> Result<Value, String> {
        let pp = if dry_run {
            PatchParams::default().dry_run()
        } else {
            PatchParams::default()
        };

        match kind {
            "Deployment" => {
                let api: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);
                let before = api
                    .get(name)
                    .await
                    .map_err(|e| format!("failed to get Deployment {}: {}", name, e))?;
                let observed_before = json!({
                    "restartedAt": before
                        .spec
                        .as_ref()
                        .and_then(|s| s.template.metadata.as_ref())
                        .and_then(|m| m.annotations.as_ref())
                        .and_then(|ann| ann.get("kubectl.kubernetes.io/restartedAt"))
                        .cloned(),
                });
                let _ = api
                    .patch(name, &pp, &Patch::Strategic(patch))
                    .await
                    .map_err(|e| {
                        format!(
                            "{} patch failed for Deployment {}: {}",
                            if dry_run { "dry-run" } else { "apply" },
                            name,
                            e
                        )
                    })?;
                Ok(observed_before)
            }
            "StatefulSet" => {
                let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), namespace);
                let before = api
                    .get(name)
                    .await
                    .map_err(|e| format!("failed to get StatefulSet {}: {}", name, e))?;
                let observed_before = json!({
                    "restartedAt": before
                        .spec
                        .as_ref()
                        .and_then(|s| s.template.metadata.as_ref())
                        .and_then(|m| m.annotations.as_ref())
                        .and_then(|ann| ann.get("kubectl.kubernetes.io/restartedAt"))
                        .cloned(),
                });
                let _ = api
                    .patch(name, &pp, &Patch::Strategic(patch))
                    .await
                    .map_err(|e| {
                        format!(
                            "{} patch failed for StatefulSet {}: {}",
                            if dry_run { "dry-run" } else { "apply" },
                            name,
                            e
                        )
                    })?;
                Ok(observed_before)
            }
            "DaemonSet" => {
                let api: Api<DaemonSet> = Api::namespaced(self.client.clone(), namespace);
                let before = api
                    .get(name)
                    .await
                    .map_err(|e| format!("failed to get DaemonSet {}: {}", name, e))?;
                let observed_before = json!({
                    "restartedAt": before
                        .spec
                        .as_ref()
                        .and_then(|s| s.template.metadata.as_ref())
                        .and_then(|m| m.annotations.as_ref())
                        .and_then(|ann| ann.get("kubectl.kubernetes.io/restartedAt"))
                        .cloned(),
                });
                let _ = api
                    .patch(name, &pp, &Patch::Strategic(patch))
                    .await
                    .map_err(|e| {
                        format!(
                            "{} patch failed for DaemonSet {}: {}",
                            if dry_run { "dry-run" } else { "apply" },
                            name,
                            e
                        )
                    })?;
                Ok(observed_before)
            }
            other => Err(format!(
                "unsupported rollout_restart target kind {}; expected Deployment, StatefulSet, or DaemonSet",
                other
            )),
        }
    }

    async fn wait_for_rollout_completion(
        &self,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> Result<ActionVerification, String> {
        let timeout = Duration::from_secs(120);
        let deadline = SystemTime::now()
            .checked_add(timeout)
            .ok_or_else(|| "failed to compute rollout deadline".to_string())?;

        loop {
            if SystemTime::now() > deadline {
                return Err(format!(
                    "rollout verification timed out for {}/{} {}",
                    namespace, kind, name
                ));
            }

            let observed = match kind {
                "Deployment" => self.verify_deployment_rollout(namespace, name).await?,
                "StatefulSet" => self.verify_statefulset_rollout(namespace, name).await?,
                "DaemonSet" => self.verify_daemonset_rollout(namespace, name).await?,
                other => {
                    return Err(format!(
                        "unsupported rollout verification target kind {}; expected Deployment, StatefulSet, or DaemonSet",
                        other
                    ));
                }
            };

            if observed["ready"].as_bool().unwrap_or(false) {
                return Ok(ActionVerification {
                    status: "ready".into(),
                    message: Some("workload rollout completed successfully".into()),
                    observed_state: Some(observed),
                });
            }

            sleep(Duration::from_secs(2)).await;
        }
    }

    async fn verify_deployment_rollout(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Value, String> {
        let api: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);
        let workload = api.get(name).await.map_err(|e| {
            format!(
                "failed to read Deployment {} for rollout verification: {}",
                name, e
            )
        })?;
        let spec_replicas = workload.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
        let status = workload.status.as_ref();
        let ready = status.and_then(|s| s.updated_replicas).unwrap_or_default() >= spec_replicas
            && status
                .and_then(|s| s.available_replicas)
                .unwrap_or_default()
                >= spec_replicas
            && status
                .and_then(|s| s.observed_generation)
                .unwrap_or_default()
                >= workload.metadata.generation.unwrap_or_default();

        Ok(json!({
            "ready": ready,
            "replicas": status.map(|s| s.replicas).unwrap_or_default(),
            "updated_replicas": status.and_then(|s| s.updated_replicas).unwrap_or_default(),
            "available_replicas": status.and_then(|s| s.available_replicas).unwrap_or_default(),
            "ready_replicas": status.and_then(|s| s.ready_replicas).unwrap_or_default(),
            "observed_generation": status.and_then(|s| s.observed_generation).unwrap_or_default(),
        }))
    }

    async fn verify_statefulset_rollout(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Value, String> {
        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), namespace);
        let workload = api.get(name).await.map_err(|e| {
            format!(
                "failed to read StatefulSet {} for rollout verification: {}",
                name, e
            )
        })?;
        let spec_replicas = workload.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
        let status = workload.status.as_ref();
        let ready = status.and_then(|s| s.updated_replicas).unwrap_or_default() >= spec_replicas
            && status.and_then(|s| s.ready_replicas).unwrap_or_default() >= spec_replicas
            && status
                .and_then(|s| s.observed_generation)
                .unwrap_or_default()
                >= workload.metadata.generation.unwrap_or_default();

        Ok(json!({
            "ready": ready,
            "replicas": status.map(|s| s.replicas).unwrap_or_default(),
            "updated_replicas": status.and_then(|s| s.updated_replicas).unwrap_or_default(),
            "ready_replicas": status.and_then(|s| s.ready_replicas).unwrap_or_default(),
            "current_replicas": status.and_then(|s| s.current_replicas).unwrap_or_default(),
            "observed_generation": status.and_then(|s| s.observed_generation).unwrap_or_default(),
        }))
    }

    async fn verify_daemonset_rollout(&self, namespace: &str, name: &str) -> Result<Value, String> {
        let api: Api<DaemonSet> = Api::namespaced(self.client.clone(), namespace);
        let workload = api.get(name).await.map_err(|e| {
            format!(
                "failed to read DaemonSet {} for rollout verification: {}",
                name, e
            )
        })?;
        let status = workload.status.as_ref();
        let ready = status
            .map(|s| s.desired_number_scheduled)
            .unwrap_or_default()
            > 0
            && status
                .and_then(|s| s.updated_number_scheduled)
                .unwrap_or_default()
                >= status
                    .map(|s| s.desired_number_scheduled)
                    .unwrap_or_default()
            && status.and_then(|s| s.number_available).unwrap_or_default()
                >= status
                    .map(|s| s.desired_number_scheduled)
                    .unwrap_or_default()
            && status
                .and_then(|s| s.observed_generation)
                .unwrap_or_default()
                >= workload.metadata.generation.unwrap_or_default();

        Ok(json!({
            "ready": ready,
            "desired_number_scheduled": status.map(|s| s.desired_number_scheduled).unwrap_or_default(),
            "current_number_scheduled": status.map(|s| s.current_number_scheduled).unwrap_or_default(),
            "number_ready": status.map(|s| s.number_ready).unwrap_or_default(),
            "updated_number_scheduled": status.map(|s| s.updated_number_scheduled).unwrap_or_default(),
            "number_available": status.map(|s| s.number_available).unwrap_or_default(),
            "observed_generation": status.map(|s| s.observed_generation).unwrap_or_default(),
        }))
    }

    async fn self_update(&self, command_id: &str, spec: SelfUpdateSpec) -> CommandResult {
        let namespace = UPDATE_AGENT_NAMESPACE;
        if let Err(message) = self
            .ensure_effective_policy_allows(namespace, "self_update", None, None)
            .await
        {
            return command_error(command_id, ExecutionMode::Execute, message);
        }

        info!(command_id, "self_update: restart requested");
        let strategy = spec.strategy.unwrap_or_else(|| "restart_pod".into());
        let target = spec.target_version.as_deref().unwrap_or("<unspecified>");
        let reason = spec.reason.as_deref().unwrap_or("<none>");

        let mut r = CommandResult::simple(
            command_id.to_string(),
            "ok",
            Some(format!(
                "self_update accepted: strategy={}, target_version={}, reason={}",
                strategy, target, reason
            )),
        );
        r.restart_requested = Some(true);
        r
    }

    async fn update_agent(&self, command_id: &str, spec: UpdateAgentSpec) -> CommandResult {
        if let Err(message) = self
            .ensure_effective_policy_allows(UPDATE_AGENT_NAMESPACE, "update_agent", None, None)
            .await
        {
            return update_agent_error(command_id, message);
        }

        let image = match validate_update_agent_image(&spec.image) {
            Ok(image) => image,
            Err(message) => {
                return update_agent_error(command_id, message);
            }
        };

        let api: Api<DaemonSet> = Api::namespaced(self.client.clone(), UPDATE_AGENT_NAMESPACE);
        let before = match api.get(UPDATE_AGENT_DAEMONSET).await {
            Ok(ds) => ds,
            Err(e) => {
                return update_agent_error(
                    command_id,
                    format!(
                        "failed to get target DaemonSet {}/{}: {}",
                        UPDATE_AGENT_NAMESPACE, UPDATE_AGENT_DAEMONSET, e
                    ),
                );
            }
        };

        let before_image = match daemonset_container_image(&before, UPDATE_AGENT_CONTAINER) {
            Ok(current) => current,
            Err(message) => {
                return update_agent_error(command_id, message);
            }
        };

        let warnings = self
            .collect_preflight_warnings(
                &WorkloadResourcesSpec {
                    workload_kind: "DaemonSet".into(),
                    namespace: UPDATE_AGENT_NAMESPACE.into(),
                    name: UPDATE_AGENT_DAEMONSET.into(),
                    container: UPDATE_AGENT_CONTAINER.into(),
                    requests: None,
                    limits: None,
                },
                &daemonset_pod_labels(&before),
            )
            .await;

        let patch = json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "name": UPDATE_AGENT_CONTAINER,
                            "image": image,
                        }],
                    },
                },
            },
        });

        if let Err(e) = api
            .patch(
                UPDATE_AGENT_DAEMONSET,
                &PatchParams::default().dry_run(),
                &Patch::Strategic(&patch),
            )
            .await
        {
            return update_agent_error(
                command_id,
                format!(
                    "update_agent dry-run failed for target DaemonSet {}/{}: {}",
                    UPDATE_AGENT_NAMESPACE, UPDATE_AGENT_DAEMONSET, e
                ),
            );
        }

        let after = match api
            .patch(
                UPDATE_AGENT_DAEMONSET,
                &PatchParams::default(),
                &Patch::Strategic(&patch),
            )
            .await
        {
            Ok(ds) => ds,
            Err(e) => {
                return update_agent_error(
                    command_id,
                    format!(
                        "failed to patch target DaemonSet {}/{}: {}",
                        UPDATE_AGENT_NAMESPACE, UPDATE_AGENT_DAEMONSET, e
                    ),
                );
            }
        };

        let after_image = match daemonset_container_image(&after, UPDATE_AGENT_CONTAINER) {
            Ok(updated) => updated,
            Err(message) => {
                return update_agent_error(command_id, message);
            }
        };

        let mut result = CommandResult::simple(
            command_id.to_string(),
            "ok",
            Some(format!(
                "update_agent applied on {}/{} container {}: {} -> {}",
                UPDATE_AGENT_NAMESPACE,
                UPDATE_AGENT_DAEMONSET,
                UPDATE_AGENT_CONTAINER,
                before_image,
                after_image
            )),
        );
        result.dry_run = Some(false);
        result.applied_patch = Some(patch);
        result.observed_before = Some(json!({"image": before_image}));
        result.observed_after = Some(json!({"image": after_image}));
        result.warnings = warnings;
        result
    }

    async fn diagnose_postgresql(
        &self,
        command_id: &str,
        spec: PostgresqlDiagnosticSpec,
    ) -> CommandResult {
        if let Err(message) = self
            .ensure_effective_policy_allows(&spec.namespace, "diagnose_postgresql", None, None)
            .await
        {
            return command_error(command_id, ExecutionMode::Execute, message);
        }

        info!(command_id, namespace = %spec.namespace, "diagnose_postgresql: collecting read-only diagnostics");
        let report = crate::plugins::diagnose_postgresql(&self.client, &self.cfg, &spec).await;
        let mut result = CommandResult::simple(command_id.to_string(), "ok", None);
        result.diagnostic = Some(report);
        result
    }
}

fn update_agent_error(command_id: &str, message: String) -> CommandResult {
    let mut result = CommandResult::simple(command_id.to_string(), "error", Some(message));
    result.dry_run = Some(false);
    result
}

fn namespace_action_mode_enabled(namespace: &Namespace) -> Result<(), String> {
    let name = namespace.metadata.name.as_deref().unwrap_or("<unknown>");
    let value = namespace
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(ACTION_MODE_NAMESPACE_LABEL))
        .map(String::as_str);

    if value == Some(ACTION_MODE_NAMESPACE_LABEL_ENABLED) {
        return Ok(());
    }

    match value {
        Some(found) => Err(format!(
            "namespace {} is not enabled for Sentinella action mode: label {}={} is required",
            name, ACTION_MODE_NAMESPACE_LABEL, found
        )),
        None => Err(format!(
            "namespace {} is not enabled for Sentinella action mode: add label {}={}",
            name, ACTION_MODE_NAMESPACE_LABEL, ACTION_MODE_NAMESPACE_LABEL_ENABLED
        )),
    }
}

fn validate_update_agent_image(image: &str) -> Result<String, String> {
    let image = image.trim();
    if image.is_empty() {
        return Err("update_agent image must be non-empty".into());
    }

    if !image.starts_with(UPDATE_AGENT_ALLOWED_PREFIX) {
        return Err(format!(
            "update_agent image must start with allowed prefix {}",
            UPDATE_AGENT_ALLOWED_PREFIX
        ));
    }

    let suffix = &image[UPDATE_AGENT_ALLOWED_PREFIX.len()..];
    if suffix.is_empty() || suffix == "/" {
        return Err("update_agent image must include an image name after allowed prefix".into());
    }

    if let Some((_, digest)) = suffix.rsplit_once("@sha256:") {
        if digest.len() == 64 && digest.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return Ok(image.to_string());
        }
        return Err("update_agent image has invalid sha256 digest format".into());
    }

    if let Some((name, tag)) = suffix.rsplit_once(':') {
        if name.is_empty() || tag.is_empty() {
            return Err("update_agent image must include non-empty image name and tag".into());
        }
        return Ok(image.to_string());
    }

    Err("update_agent image must include either :<tag> or @sha256:<digest>".into())
}

struct ResourcePreview {
    patch: Value,
    observed_before: Value,
    observed_after: Value,
    warnings: Vec<String>,
}

impl ResourcePreview {
    fn new(
        patch: Value,
        observed_before: Value,
        observed_after: Value,
        warnings: Vec<String>,
    ) -> Self {
        Self {
            patch,
            observed_before,
            observed_after,
            warnings,
        }
    }
}

fn merge_check(warnings: &mut Vec<String>, check_name: &str, result: Result<Vec<String>, String>) {
    match result {
        Ok(mut check_warnings) => warnings.append(&mut check_warnings),
        Err(reason) => warnings.push(format!(
            "preflight.check.unavailable: {} unavailable: {}",
            check_name, reason
        )),
    }
}

fn build_workload_resources_patch(spec: &WorkloadResourcesSpec) -> Result<Value, String> {
    if spec.requests.is_none() && spec.limits.is_none() {
        return Err("at least one of requests or limits must be provided".into());
    }

    let mut resources = Map::new();
    if let Some(requests) = &spec.requests {
        resources.insert("requests".into(), resource_map_to_value(requests));
    }
    if let Some(limits) = &spec.limits {
        resources.insert("limits".into(), resource_map_to_value(limits));
    }

    Ok(json!({
        "spec": {
            "template": {
                "spec": {
                    "containers": [{
                        "name": spec.container,
                        "resources": Value::Object(resources),
                    }],
                },
            },
        },
    }))
}

fn resource_map_to_value(resources: &ResourceMap) -> Value {
    if resources.cpu.is_none() && resources.memory.is_none() {
        return Value::Null;
    }

    let mut out = Map::new();
    if let Some(cpu) = &resources.cpu {
        out.insert("cpu".into(), Value::String(cpu.clone()));
    }
    if let Some(memory) = &resources.memory {
        out.insert("memory".into(), Value::String(memory.clone()));
    }
    Value::Object(out)
}

fn deployment_container_resources(workload: &Deployment, container: &str) -> Result<Value, String> {
    let containers = workload
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .map(|pod_spec| pod_spec.containers.as_slice())
        .ok_or_else(|| format!("Deployment has no pod template spec: {container}"))?;
    container_resources(containers, container)
}

fn statefulset_container_resources(
    workload: &StatefulSet,
    container: &str,
) -> Result<Value, String> {
    let containers = workload
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .map(|pod_spec| pod_spec.containers.as_slice())
        .ok_or_else(|| format!("StatefulSet has no pod template spec: {container}"))?;
    container_resources(containers, container)
}

fn daemonset_container_resources(workload: &DaemonSet, container: &str) -> Result<Value, String> {
    let containers = workload
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .map(|pod_spec| pod_spec.containers.as_slice())
        .ok_or_else(|| format!("DaemonSet has no pod template spec: {container}"))?;
    container_resources(containers, container)
}

fn daemonset_container_image(workload: &DaemonSet, container: &str) -> Result<String, String> {
    let containers = workload
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .map(|pod_spec| pod_spec.containers.as_slice())
        .ok_or_else(|| {
            format!(
                "target DaemonSet has no pod template spec for container {}",
                container
            )
        })?;

    let matched = containers
        .iter()
        .find(|candidate| candidate.name == container)
        .ok_or_else(|| format!("target DaemonSet missing expected container {}", container))?;

    matched.image.clone().ok_or_else(|| {
        format!(
            "target DaemonSet container {} has no image field",
            container
        )
    })
}

fn deployment_pod_labels(workload: &Deployment) -> BTreeMap<String, String> {
    workload
        .spec
        .as_ref()
        .and_then(|spec| spec.template.metadata.as_ref())
        .and_then(|metadata| metadata.labels.clone())
        .unwrap_or_default()
}

fn statefulset_pod_labels(workload: &StatefulSet) -> BTreeMap<String, String> {
    workload
        .spec
        .as_ref()
        .and_then(|spec| spec.template.metadata.as_ref())
        .and_then(|metadata| metadata.labels.clone())
        .unwrap_or_default()
}

fn daemonset_pod_labels(workload: &DaemonSet) -> BTreeMap<String, String> {
    workload
        .spec
        .as_ref()
        .and_then(|spec| spec.template.metadata.as_ref())
        .and_then(|metadata| metadata.labels.clone())
        .unwrap_or_default()
}

pub(crate) fn label_selector_matches(
    selector: Option<&LabelSelector>,
    labels: &BTreeMap<String, String>,
) -> bool {
    let Some(selector) = selector else {
        return true;
    };

    if let Some(match_labels) = &selector.match_labels {
        for (key, expected) in match_labels {
            if labels.get(key) != Some(expected) {
                return false;
            }
        }
    }

    if let Some(expressions) = &selector.match_expressions {
        for expression in expressions {
            let value = labels.get(&expression.key);
            let values = expression.values.as_deref().unwrap_or_default();
            let matches = match expression.operator.as_str() {
                "In" => value
                    .map(|v| values.iter().any(|candidate| candidate == v))
                    .unwrap_or(false),
                "NotIn" => value
                    .map(|v| values.iter().all(|candidate| candidate != v))
                    .unwrap_or(true),
                "Exists" => value.is_some(),
                "DoesNotExist" => value.is_none(),
                _ => false,
            };
            if !matches {
                return false;
            }
        }
    }

    true
}

fn vpa_mode_is_conflicting(mode: Option<&str>) -> bool {
    matches!(mode.unwrap_or("Auto"), "Auto" | "Recreate")
}

fn container_resources(
    containers: &[k8s_openapi::api::core::v1::Container],
    container: &str,
) -> Result<Value, String> {
    let found = containers
        .iter()
        .find(|candidate| candidate.name == container)
        .ok_or_else(|| format!("container not found in workload pod template: {container}"))?;
    Ok(resource_requirements_to_value(found.resources.as_ref()))
}

fn resource_requirements_to_value(resources: Option<&ResourceRequirements>) -> Value {
    let mut out = Map::new();
    if let Some(resources) = resources {
        if let Some(requests) = &resources.requests {
            out.insert("requests".into(), quantity_map_to_value(requests));
        }
        if let Some(limits) = &resources.limits {
            out.insert("limits".into(), quantity_map_to_value(limits));
        }
    }
    Value::Object(out)
}

fn quantity_map_to_value(resources: &BTreeMap<String, Quantity>) -> Value {
    resources
        .iter()
        .map(|(name, quantity)| (name.clone(), Value::String(quantity.0.clone())))
        .collect::<Map<_, _>>()
        .into()
}

fn parse_spec<T: serde::de::DeserializeOwned>(cmd: &Command) -> Result<T, String> {
    serde_json::from_value(cmd.spec.clone())
        .map_err(|e| format!("invalid spec for kind {}: {}", cmd.kind, e))
}

fn command_error(command_id: &str, mode: ExecutionMode, message: String) -> CommandResult {
    let mut result = CommandResult::simple(command_id.to_string(), "error", Some(message));
    result.dry_run = Some(matches!(mode, ExecutionMode::Preview));
    result
}

fn dedup_begin(cmd: &Command) -> Option<CommandResult> {
    let mut state = COMMAND_DEDUP.lock().expect("command dedup mutex poisoned");

    if let Some(result) = state.completed.get(&cmd.id).cloned() {
        return Some(result);
    }

    if !state.running.insert(cmd.id.clone()) {
        return Some(CommandResult::simple(
            cmd.id.clone(),
            "skipped",
            Some("command already running".into()),
        ));
    }

    None
}

fn dedup_finish(result: &CommandResult) {
    let mut state = COMMAND_DEDUP.lock().expect("command dedup mutex poisoned");
    state.running.remove(&result.command_id);
    state
        .completed
        .insert(result.command_id.clone(), result.clone());
}

fn condition_true(conditions: &[SentinellaHubActionPolicyCondition], condition_type: &str) -> bool {
    conditions.iter().any(|condition| {
        condition.type_ == condition_type && condition.status.eq_ignore_ascii_case("true")
    })
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into())
}

fn rollout_restart_patch(_target: &WorkloadTargetRef, restart_at: &str) -> Value {
    json!({
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "kubectl.kubernetes.io/restartedAt": restart_at,
                    }
                }
            }
        }
    })
}

fn spec_error(cmd: &Command, message: String) -> CommandResult {
    warn!(command_id = %cmd.id, kind = %cmd.kind, "{}", message);
    CommandResult::simple(cmd.id.clone(), "error", Some(message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn workload_spec(spec: Value) -> WorkloadResourcesSpec {
        serde_json::from_value(spec).unwrap()
    }

    fn namespace_spec(labels: Option<Value>) -> Namespace {
        let mut namespace: Namespace = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": "app-prod"
            }
        }))
        .unwrap();

        namespace.metadata.labels = labels.and_then(|value| serde_json::from_value(value).ok());
        namespace
    }

    fn policy_for_gating(
        actions: Vec<&str>,
        resources: Vec<&str>,
        limits: Option<Value>,
    ) -> SentinellaHubActionPolicy {
        serde_json::from_value(json!({
            "apiVersion": "sentinella.io/v1alpha1",
            "kind": "SentinellaHubActionPolicy",
            "metadata": { "name": "policy" },
            "spec": {
                "namespaceSelector": {"matchLabels": {"environment": "prod"}},
                "allowedActions": actions,
                "allowedResources": resources,
                "approvalRequired": true,
                "limits": limits
            }
        }))
        .unwrap()
    }

    fn workload_resources_spec(spec: Value) -> WorkloadResourcesSpec {
        serde_json::from_value(spec).unwrap()
    }

    fn policy_status(
        stale: bool,
        last_reconciled_at_ms: Option<u128>,
        ready: bool,
    ) -> SentinellaHubActionPolicyStatus {
        SentinellaHubActionPolicyStatus {
            effective_namespaces: vec!["app-prod".into()],
            conditions: vec![SentinellaHubActionPolicyCondition {
                type_: "Ready".into(),
                status: if ready { "True".into() } else { "False".into() },
                reason: None,
                message: None,
                observed_generation: Some(1),
                last_transition_time_ms: Some(1234),
            }],
            observed_generation: Some(1),
            last_reconciled_at_ms,
            stale,
        }
    }

    #[test]
    fn policy_rejection_reason_for_command_allows_supported_workload_action() {
        let policy = policy_for_gating(vec!["rollout_restart"], vec!["Deployment"], None);
        let spec = workload_resources_spec(json!({
            "workload_kind": "Deployment",
            "namespace": "prod",
            "name": "api",
            "container": "app",
            "requests": {"cpu": "250m", "memory": "256Mi"}
        }));

        assert!(
            Executor::policy_rejection_reason_for_command(
                &policy,
                "rollout_restart",
                Some("Deployment"),
                Some(&spec)
            )
            .is_none()
        );
    }

    #[test]
    fn policy_rejection_reason_for_command_rejects_unsupported_action() {
        let policy = policy_for_gating(vec!["scale"], vec!["Deployment"], None);
        let spec = workload_resources_spec(json!({
            "workload_kind": "Deployment",
            "namespace": "prod",
            "name": "api",
            "container": "app",
            "requests": {"cpu": "250m", "memory": "256Mi"}
        }));

        let reason = Executor::policy_rejection_reason_for_command(
            &policy,
            "rollout_restart",
            Some("Deployment"),
            Some(&spec),
        )
        .expect("expected action to be rejected");

        assert_eq!(reason, "does not allow action rollout_restart");
    }

    #[test]
    fn policy_rejection_reason_for_command_rejects_unsupported_resource() {
        let policy = policy_for_gating(vec!["rollout_restart"], vec!["StatefulSet"], None);
        let spec = workload_resources_spec(json!({
            "workload_kind": "Deployment",
            "namespace": "prod",
            "name": "api",
            "container": "app",
            "requests": {"cpu": "250m", "memory": "256Mi"}
        }));

        let reason = Executor::policy_rejection_reason_for_command(
            &policy,
            "rollout_restart",
            Some("Deployment"),
            Some(&spec),
        )
        .expect("expected resource to be rejected");

        assert_eq!(reason, "does not allow resource Deployment");
    }

    #[test]
    fn policy_rejection_reason_for_command_rejects_limits_exceeded() {
        let policy = policy_for_gating(
            vec!["apply_workload_resources"],
            vec!["Deployment"],
            Some(json!({"maxCpuLimit": "500m", "maxMemoryLimit": "512Mi"})),
        );
        let spec = workload_resources_spec(json!({
            "workload_kind": "Deployment",
            "namespace": "prod",
            "name": "api",
            "container": "app",
            "requests": {"cpu": "750m", "memory": "256Mi"}
        }));

        let reason = Executor::policy_rejection_reason_for_command(
            &policy,
            "apply_workload_resources",
            Some("Deployment"),
            Some(&spec),
        )
        .expect("expected limits to be rejected");

        assert!(reason.contains("limits exceeded"));
    }

    #[test]
    fn policy_rejection_reason_for_command_allows_self_update_without_resources() {
        let policy = policy_for_gating(vec!["self_update"], Vec::new(), None);

        assert!(
            Executor::policy_rejection_reason_for_command(&policy, "self_update", None, None)
                .is_none()
        );
    }

    #[test]
    fn build_workload_resources_patch_includes_named_container_only() {
        let spec = workload_spec(json!({
            "workload_kind": "Deployment",
            "namespace": "default",
            "name": "api",
            "container": "app",
            "requests": {"cpu": "500m", "memory": "256Mi"},
            "limits": {"memory": "512Mi"}
        }));

        let patch = build_workload_resources_patch(&spec).unwrap();

        assert_eq!(
            patch,
            json!({
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{
                                "name": "app",
                                "resources": {
                                    "requests": {"cpu": "500m", "memory": "256Mi"},
                                    "limits": {"memory": "512Mi"}
                                }
                            }]
                        }
                    }
                }
            })
        );
    }

    #[test]
    fn build_workload_resources_patch_rejects_empty_resource_change() {
        let spec = workload_spec(json!({
            "workload_kind": "Deployment",
            "namespace": "default",
            "name": "api",
            "container": "app"
        }));

        let err = build_workload_resources_patch(&spec).unwrap_err();

        assert_eq!(err, "at least one of requests or limits must be provided");
    }

    #[test]
    fn policy_status_rejection_reason_rejects_stale_even_when_ready() {
        let status = policy_status(true, Some(1234), true);

        let reason = Executor::policy_status_rejection_reason("workload-tuning", &status, 1300, 50)
            .expect("expected stale status to be rejected");

        assert_eq!(reason, "policy workload-tuning status is stale");
    }

    #[test]
    fn policy_status_rejection_reason_rejects_missing_freshness_timestamp() {
        let status = policy_status(false, None, true);

        let reason = Executor::policy_status_rejection_reason("workload-tuning", &status, 1300, 50)
            .expect("expected missing timestamp to be rejected");

        assert_eq!(
            reason,
            "policy workload-tuning status has no freshness timestamp"
        );
    }

    #[test]
    fn build_workload_resources_patch_uses_null_to_clear_empty_side() {
        let spec = workload_spec(json!({
            "workload_kind": "Deployment",
            "namespace": "default",
            "name": "api",
            "container": "app",
            "limits": {}
        }));

        let patch = build_workload_resources_patch(&spec).unwrap();

        assert_eq!(
            patch["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"],
            Value::Null
        );
    }

    #[test]
    fn namespace_action_mode_enabled_accepts_enabled_label() {
        let namespace = namespace_spec(Some(json!({
            "sentinella.io/action-mode": "enabled"
        })));

        assert_eq!(namespace_action_mode_enabled(&namespace), Ok(()));
    }

    #[test]
    fn namespace_action_mode_enabled_rejects_missing_label() {
        let namespace = namespace_spec(None);

        let err = namespace_action_mode_enabled(&namespace).unwrap_err();

        assert_eq!(
            err,
            "namespace app-prod is not enabled for Sentinella action mode: add label sentinella.io/action-mode=enabled"
        );
    }

    #[test]
    fn namespace_action_mode_enabled_rejects_wrong_label_value() {
        let namespace = namespace_spec(Some(json!({
            "sentinella.io/action-mode": "disabled"
        })));

        let err = namespace_action_mode_enabled(&namespace).unwrap_err();

        assert_eq!(
            err,
            "namespace app-prod is not enabled for Sentinella action mode: label sentinella.io/action-mode=disabled is required"
        );
    }

    #[test]
    fn deployment_container_resources_returns_existing_resources() {
        let deployment: Deployment = serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "api", "namespace": "default"},
            "spec": {
                "selector": {"matchLabels": {"app": "api"}},
                "template": {
                    "metadata": {"labels": {"app": "api"}},
                    "spec": {
                        "containers": [{
                            "name": "app",
                            "image": "example/api:latest",
                            "resources": {
                                "requests": {"cpu": "250m"},
                                "limits": {"memory": "512Mi"}
                            }
                        }]
                    }
                }
            }
        }))
        .unwrap();

        let resources = deployment_container_resources(&deployment, "app").unwrap();

        assert_eq!(
            resources,
            json!({
                "requests": {"cpu": "250m"},
                "limits": {"memory": "512Mi"}
            })
        );
    }

    #[test]
    fn deployment_container_resources_rejects_missing_container() {
        let deployment: Deployment = serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "api", "namespace": "default"},
            "spec": {
                "selector": {"matchLabels": {"app": "api"}},
                "template": {
                    "metadata": {"labels": {"app": "api"}},
                    "spec": {
                        "containers": [{"name": "app", "image": "example/api:latest"}]
                    }
                }
            }
        }))
        .unwrap();

        let err = deployment_container_resources(&deployment, "sidecar").unwrap_err();

        assert_eq!(err, "container not found in workload pod template: sidecar");
    }

    #[test]
    fn statefulset_container_resources_returns_existing_resources() {
        let statefulset: StatefulSet = serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {"name": "db", "namespace": "default"},
            "spec": {
                "selector": {"matchLabels": {"app": "db"}},
                "serviceName": "db",
                "template": {
                    "metadata": {"labels": {"app": "db"}},
                    "spec": {
                        "containers": [{
                            "name": "postgres",
                            "image": "postgres:16",
                            "resources": {"requests": {"memory": "1Gi"}}
                        }]
                    }
                }
            }
        }))
        .unwrap();

        let resources = statefulset_container_resources(&statefulset, "postgres").unwrap();

        assert_eq!(resources, json!({"requests": {"memory": "1Gi"}}));
    }

    #[test]
    fn daemonset_container_resources_returns_empty_resources_when_absent() {
        let daemonset: DaemonSet = serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "metadata": {"name": "agent", "namespace": "default"},
            "spec": {
                "selector": {"matchLabels": {"app": "agent"}},
                "template": {
                    "metadata": {"labels": {"app": "agent"}},
                    "spec": {
                        "containers": [{"name": "agent", "image": "example/agent:latest"}]
                    }
                }
            }
        }))
        .unwrap();

        let resources = daemonset_container_resources(&daemonset, "agent").unwrap();

        assert_eq!(resources, json!({}));
    }

    #[test]
    fn vpa_mode_is_conflicting_defaults_to_auto() {
        assert!(vpa_mode_is_conflicting(None));
        assert!(vpa_mode_is_conflicting(Some("Auto")));
        assert!(vpa_mode_is_conflicting(Some("Recreate")));
        assert!(!vpa_mode_is_conflicting(Some("Off")));
    }

    #[test]
    fn label_selector_matches_handles_match_labels_and_expressions() {
        let selector: LabelSelector = serde_json::from_value(json!({
            "matchLabels": {"app": "api"},
            "matchExpressions": [
                {"key": "tier", "operator": "In", "values": ["backend"]},
                {"key": "region", "operator": "DoesNotExist"}
            ]
        }))
        .unwrap();

        let labels = BTreeMap::from([
            ("app".to_string(), "api".to_string()),
            ("tier".to_string(), "backend".to_string()),
        ]);

        assert!(label_selector_matches(Some(&selector), &labels));
    }

    #[test]
    fn label_selector_matches_rejects_non_matching_expression() {
        let selector: LabelSelector = serde_json::from_value(json!({
            "matchExpressions": [
                {"key": "tier", "operator": "NotIn", "values": ["backend"]}
            ]
        }))
        .unwrap();

        let labels = BTreeMap::from([("tier".to_string(), "backend".to_string())]);

        assert!(!label_selector_matches(Some(&selector), &labels));
    }

    #[test]
    fn merge_check_includes_unavailable_prefix() {
        let mut warnings = Vec::new();
        merge_check(&mut warnings, "hpa", Err("forbidden".into()));

        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].starts_with("preflight.check.unavailable: hpa unavailable:"));
    }

    #[test]
    fn validate_update_agent_image_rejects_empty() {
        let err = validate_update_agent_image("").unwrap_err();
        assert_eq!(err, "update_agent image must be non-empty");
    }

    #[test]
    fn validate_update_agent_image_rejects_whitespace() {
        let err = validate_update_agent_image("   ").unwrap_err();
        assert_eq!(err, "update_agent image must be non-empty");
    }

    #[test]
    fn validate_update_agent_image_rejects_wrong_registry() {
        let err = validate_update_agent_image("ghcr.io/sentinella/agent:v1.2.3").unwrap_err();
        assert!(err.starts_with("update_agent image must start with allowed prefix"));
    }

    #[test]
    fn validate_update_agent_image_rejects_wrong_repo_path() {
        let err = validate_update_agent_image(
            "us-east1-docker.pkg.dev/sentinella-hub/other-repo/agent:v1.2.3",
        )
        .unwrap_err();
        assert!(err.starts_with("update_agent image must start with allowed prefix"));
    }

    #[test]
    fn validate_update_agent_image_rejects_prefix_only() {
        let err = validate_update_agent_image(UPDATE_AGENT_ALLOWED_PREFIX).unwrap_err();
        assert_eq!(
            err,
            "update_agent image must include an image name after allowed prefix"
        );
    }

    #[test]
    fn validate_update_agent_image_rejects_missing_tag_or_digest() {
        let err = validate_update_agent_image(
            "us-east1-docker.pkg.dev/sentinella-hub/kubernetes-agent/sentinella-hub-k8s-agent",
        )
        .unwrap_err();
        assert_eq!(
            err,
            "update_agent image must include either :<tag> or @sha256:<digest>"
        );
    }

    #[test]
    fn validate_update_agent_image_accepts_tagged_image() {
        let image = validate_update_agent_image(
            "us-east1-docker.pkg.dev/sentinella-hub/kubernetes-agent/sentinella-hub-k8s-agent:v1.2.3",
        )
        .unwrap();
        assert_eq!(
            image,
            "us-east1-docker.pkg.dev/sentinella-hub/kubernetes-agent/sentinella-hub-k8s-agent:v1.2.3"
        );
    }

    #[test]
    fn validate_update_agent_image_allows_latest_tag() {
        let image = validate_update_agent_image(
            "us-east1-docker.pkg.dev/sentinella-hub/kubernetes-agent/sentinella-hub-k8s-agent:latest",
        )
        .unwrap();
        assert_eq!(
            image,
            "us-east1-docker.pkg.dev/sentinella-hub/kubernetes-agent/sentinella-hub-k8s-agent:latest"
        );
    }

    #[test]
    fn validate_update_agent_image_accepts_valid_digest() {
        let image = validate_update_agent_image(
            "us-east1-docker.pkg.dev/sentinella-hub/kubernetes-agent/sentinella-hub-k8s-agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        assert_eq!(
            image,
            "us-east1-docker.pkg.dev/sentinella-hub/kubernetes-agent/sentinella-hub-k8s-agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[test]
    fn validate_update_agent_image_rejects_bad_digest() {
        let err = validate_update_agent_image(
            "us-east1-docker.pkg.dev/sentinella-hub/kubernetes-agent/sentinella-hub-k8s-agent@sha256:abc",
        )
        .unwrap_err();
        assert_eq!(err, "update_agent image has invalid sha256 digest format");
    }
}
