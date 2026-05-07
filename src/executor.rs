//! Command executor.
//!
//! `actions_enabled = false` is the default and the executor refuses to
//! execute any command while in read-only mode.

use crate::config::Config;
use crate::model::{Command, CommandResult, ResourceMap, SelfUpdateSpec, WorkloadResourcesSpec};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::ResourceRequirements;
use k8s_openapi::api::core::v1::{LimitRange, ResourceQuota};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::api::{ApiResource, DynamicObject, ListParams, Patch, PatchParams};
use kube::core::GroupVersionKind;
use kube::{Api, Client};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use tracing::{info, warn};

pub struct Executor {
    cfg: Config,
    client: Client,
}

impl Executor {
    pub fn new(cfg: Config, client: Client) -> Self {
        Self { cfg, client }
    }

    pub async fn execute(&self, cmd: &Command) -> CommandResult {
        // Master switch: when actions are disabled the agent does not even
        // attempt to parse the spec. This keeps the read-only release truly
        // read-only.
        if !self.cfg.actions_enabled {
            warn!(command_id = %cmd.id, kind = %cmd.kind, "actions disabled; skipping");
            return CommandResult::simple(
                cmd.id.clone(),
                "skipped",
                Some("agent in read-only mode (ACTIONS_ENABLED=false)".into()),
            );
        }

        // Dispatch by kind. Known kinds parse their spec and route to a
        // handler; unknown kinds short-circuit with `unknown`.
        match cmd.kind.as_str() {
            "preview_workload_resources" => match parse_spec::<WorkloadResourcesSpec>(cmd) {
                Ok(spec) => self.preview_workload_resources(&cmd.id, spec).await,
                Err(e) => spec_error(cmd, e),
            },
            "apply_workload_resources" => match parse_spec::<WorkloadResourcesSpec>(cmd) {
                Ok(spec) => self.apply_workload_resources(&cmd.id, spec).await,
                Err(e) => spec_error(cmd, e),
            },
            "self_update" => match parse_spec::<SelfUpdateSpec>(cmd) {
                Ok(spec) => self.self_update(&cmd.id, spec).await,
                Err(e) => spec_error(cmd, e),
            },
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

    async fn self_update(&self, command_id: &str, spec: SelfUpdateSpec) -> CommandResult {
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

fn label_selector_matches(
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn workload_spec(spec: Value) -> WorkloadResourcesSpec {
        serde_json::from_value(spec).unwrap()
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
}

fn parse_spec<T: serde::de::DeserializeOwned>(cmd: &Command) -> Result<T, String> {
    serde_json::from_value(cmd.spec.clone())
        .map_err(|e| format!("invalid spec for kind {}: {}", cmd.kind, e))
}

fn spec_error(cmd: &Command, message: String) -> CommandResult {
    warn!(command_id = %cmd.id, kind = %cmd.kind, "{}", message);
    CommandResult::simple(cmd.id.clone(), "error", Some(message))
}
