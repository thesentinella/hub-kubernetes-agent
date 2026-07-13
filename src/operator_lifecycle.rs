//! Dynamic operator lifecycle management.
//!
//! The agent leader watches the `sentinella-hub-k8s-agent-config` ConfigMap
//! for changes to `ACTION_OPERATOR_ENABLED`. When the flag is `true`, the
//! leader creates the operator `ServiceAccount` and `Deployment` in the
//! agent's namespace, ensures the default action policy exists, and stamps
//! `sentinella.io/action-mode=true` onto eligible namespaces. When the flag
//! is `false` (or the ConfigMap is deleted), the leader removes those
//! operator-owned resources and clears the labels it previously added.
//!
//! This replaces the static operator Deployment that used to live in the
//! install manifest, so that `kubectl get pods` shows no operator pod while
//! the feature is disabled.

use crate::executor::{ACTION_MODE_NAMESPACE_LABEL, ACTION_MODE_NAMESPACE_LABEL_ENABLED};
use crate::leader::LeaderState;
use crate::operator::{combined_excluded_namespaces, namespace_is_excluded};
use anyhow::{Context, Result};
use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Pod, ServiceAccount};
use kube::Client;
use kube::api::{
    Api, ApiResource, DeleteParams, DynamicObject, Patch, PatchParams, PostParams, WatchEvent,
    WatchParams,
};
use kube::core::GroupVersionKind;
use serde_json::json;
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info, warn};

const CONFIG_MAP_NAME: &str = "sentinella-hub-k8s-agent-config";
const OPERATOR_NAME: &str = "sentinella-hub-k8s-operator";
const ACTION_OPERATOR_ENABLED_KEY: &str = "ACTION_OPERATOR_ENABLED";
const RECONCILE_TICK_SECS: u64 = 60;
const ACTION_POLICY_GROUP: &str = "sentinella.io";
const ACTION_POLICY_VERSION: &str = "v1alpha1";
const ACTION_POLICY_KIND: &str = "SentinellaHubActionPolicy";
const DEFAULT_ACTION_POLICY_NAME: &str = "sentinella-default-action-policy";

/// Run the operator lifecycle watcher. Only the leader performs reconciliation;
/// non-leader pods watch silently and act when they acquire leadership.
pub async fn run_operator_lifecycle(
    client: Client,
    namespace: String,
    pod_name: String,
    leader: LeaderState,
) {
    info!(namespace = %namespace, "starting operator lifecycle watcher");

    let mut last_enabled: Option<bool> = None;
    let mut last_excluded: HashSet<String> = HashSet::new();

    // Initial reconcile from a direct GET so we converge immediately on
    // startup without waiting for the watch to deliver the first event.
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), &namespace);
    match cm_api.get(CONFIG_MAP_NAME).await {
        Ok(cm) => {
            let enabled = is_operator_enabled(&cm);
            last_excluded = action_operator_excluded_namespaces(&cm);
            last_enabled = Some(enabled);
            if leader.is_leader() {
                if let Err(e) = reconcile_operator_workload(
                    &client,
                    &namespace,
                    &pod_name,
                    enabled,
                    &last_excluded,
                )
                .await
                {
                    warn!(error = %e, "initial operator lifecycle reconcile failed");
                }
            }
        }
        Err(e) => {
            warn!(error = %e, "failed to read ConfigMap for initial reconcile");
        }
    }

    let wp = WatchParams::default()
        .fields(&format!("metadata.name={CONFIG_MAP_NAME}"))
        .timeout(290);

    let mut ticker = interval(Duration::from_secs(RECONCILE_TICK_SECS));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // skip the immediate first tick — we already did an initial reconcile
    ticker.tick().await;

    let mut resource_version: String = "0".into();

    loop {
        let stream = match cm_api.watch(&wp, &resource_version).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to start ConfigMap watch; retrying");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        tokio::pin!(stream);

        loop {
            tokio::select! {
                event = stream.next() => {
                    let Some(event) = event else {
                        debug!("ConfigMap watch stream ended; reconnecting");
                        break;
                    };

                    match event {
                        Ok(WatchEvent::Added(cm)) | Ok(WatchEvent::Modified(cm)) => {
                            if let Some(rv) = cm.metadata.resource_version.as_deref() {
                                resource_version = rv.to_string();
                            }
                            let enabled = is_operator_enabled(&cm);
                            last_excluded = action_operator_excluded_namespaces(&cm);
                            let changed = last_enabled != Some(enabled);
                            last_enabled = Some(enabled);
                            if changed {
                                info!(enabled, "ACTION_OPERATOR_ENABLED changed");
                            }
                            if leader.is_leader() {
                                if let Err(e) = reconcile_operator_workload(
                                    &client,
                                    &namespace,
                                    &pod_name,
                                    enabled,
                                    &last_excluded,
                                )
                                .await
                                {
                                    warn!(error = %e, "operator lifecycle reconcile failed");
                                }
                            }
                        }
                        Ok(WatchEvent::Deleted(_)) => {
                            last_enabled = Some(false);
                            last_excluded = HashSet::new();
                            if leader.is_leader() {
                                if let Err(e) = reconcile_operator_workload(
                                    &client,
                                    &namespace,
                                    &pod_name,
                                    false,
                                    &last_excluded,
                                )
                                .await
                                {
                                    warn!(error = %e, "operator lifecycle reconcile failed after ConfigMap deletion");
                                }
                            }
                        }
                        Ok(WatchEvent::Bookmark(b)) => {
                            resource_version = b.metadata.resource_version;
                        }
                        Ok(WatchEvent::Error(e)) => {
                            warn!(error = %e, "ConfigMap watch error event; reconnecting");
                            break;
                        }
                        Err(e) => {
                            warn!(error = %e, "ConfigMap stream error; reconnecting");
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    // Periodic safety-net reconcile, mainly for leadership changes.
                    if let Some(enabled) = last_enabled {
                        if leader.is_leader() {
                            if let Err(e) = reconcile_operator_workload(
                                &client,
                                &namespace,
                                &pod_name,
                                enabled,
                                &last_excluded,
                            )
                            .await
                            {
                                warn!(error = %e, "periodic operator lifecycle reconcile failed");
                            }
                        } else if !enabled {
                            // Non-leader: nothing to clean up.
                        }
                    }
                }
            }
        }
    }
}

async fn reconcile_operator_workload(
    client: &Client,
    namespace: &str,
    agent_pod_name: &str,
    enabled: bool,
    excluded_namespaces: &HashSet<String>,
) -> Result<()> {
    if enabled {
        ensure_operator_resources(client, namespace, agent_pod_name).await?;
        ensure_default_action_policy(client).await?;
        reconcile_action_mode_namespaces(client, true, excluded_namespaces).await?;
        info!("operator workload ensured (ACTION_OPERATOR_ENABLED=true)");
    } else {
        reconcile_action_mode_namespaces(client, false, excluded_namespaces).await?;
        remove_default_action_policy(client).await?;
        remove_operator_resources(client, namespace).await?;
        info!("operator workload removed (ACTION_OPERATOR_ENABLED=false)");
    }
    Ok(())
}

async fn ensure_operator_resources(
    client: &Client,
    namespace: &str,
    agent_pod_name: &str,
) -> Result<()> {
    let image = resolve_operator_image(client, namespace, agent_pod_name).await?;

    let sa_api: Api<ServiceAccount> = Api::namespaced(client.clone(), namespace);
    let sa = desired_operator_service_account(namespace);
    match sa_api.get_opt(OPERATOR_NAME).await? {
        Some(_) => {
            let patch = serde_json::to_value(&sa)?;
            sa_api
                .patch(
                    OPERATOR_NAME,
                    &PatchParams::default(),
                    &Patch::Merge(&patch),
                )
                .await
                .context("patch operator ServiceAccount")?;
        }
        None => {
            sa_api
                .create(&PostParams::default(), &sa)
                .await
                .context("create operator ServiceAccount")?;
            info!("created operator ServiceAccount");
        }
    }

    let deploy_api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let deploy = desired_operator_deployment(namespace, &image)?;
    match deploy_api.get_opt(OPERATOR_NAME).await? {
        Some(_) => {
            let patch = serde_json::to_value(&deploy)?;
            deploy_api
                .patch(
                    OPERATOR_NAME,
                    &PatchParams::default(),
                    &Patch::Merge(&patch),
                )
                .await
                .context("patch operator Deployment")?;
            debug!("operator Deployment already exists; patched");
        }
        None => {
            deploy_api
                .create(&PostParams::default(), &deploy)
                .await
                .context("create operator Deployment")?;
            info!("created operator Deployment");
        }
    }

    Ok(())
}

async fn remove_operator_resources(client: &Client, namespace: &str) -> Result<()> {
    let deploy_api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    if deploy_api.get_opt(OPERATOR_NAME).await?.is_some() {
        deploy_api
            .delete(OPERATOR_NAME, &DeleteParams::default())
            .await
            .context("delete operator Deployment")?;
        info!("deleted operator Deployment");
    }

    let sa_api: Api<ServiceAccount> = Api::namespaced(client.clone(), namespace);
    if sa_api.get_opt(OPERATOR_NAME).await?.is_some() {
        sa_api
            .delete(OPERATOR_NAME, &DeleteParams::default())
            .await
            .context("delete operator ServiceAccount")?;
        info!("deleted operator ServiceAccount");
    }

    Ok(())
}

async fn reconcile_action_mode_namespaces(
    client: &Client,
    enabled: bool,
    excluded_namespaces: &HashSet<String>,
) -> Result<()> {
    let api: Api<Namespace> = Api::all(client.clone());
    let namespaces = api.list(&Default::default()).await?.items;

    for namespace in namespaces {
        let Some(name) = namespace.metadata.name.as_deref() else {
            continue;
        };

        if namespace_is_excluded(name, excluded_namespaces) {
            if namespace_action_mode_value(&namespace) != Some("false")
                && namespace_action_mode_value(&namespace).is_some()
            {
                remove_action_mode_label(client, name).await?;
            }
            continue;
        }

        match (enabled, namespace_action_mode_value(&namespace)) {
            (true, Some(ACTION_MODE_NAMESPACE_LABEL_ENABLED)) => continue,
            (true, Some("false")) => continue,
            (true, _) => set_action_mode_label(client, name).await?,
            (false, Some(ACTION_MODE_NAMESPACE_LABEL_ENABLED)) => {
                remove_action_mode_label(client, name).await?
            }
            (false, _) => continue,
        }
    }

    Ok(())
}

fn namespace_action_mode_value(namespace: &Namespace) -> Option<&str> {
    namespace
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(ACTION_MODE_NAMESPACE_LABEL))
        .map(String::as_str)
}

async fn set_action_mode_label(client: &Client, namespace: &str) -> Result<()> {
    let api: Api<Namespace> = Api::all(client.clone());
    let patch = json!({
        "metadata": {
            "labels": {
                ACTION_MODE_NAMESPACE_LABEL: ACTION_MODE_NAMESPACE_LABEL_ENABLED,
            }
        }
    });

    api.patch(namespace, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .with_context(|| format!("set action-mode label on namespace {namespace}"))?;
    Ok(())
}

async fn remove_action_mode_label(client: &Client, namespace: &str) -> Result<()> {
    let api: Api<Namespace> = Api::all(client.clone());
    let patch = json!({
        "metadata": {
            "labels": {
                ACTION_MODE_NAMESPACE_LABEL: serde_json::Value::Null,
            }
        }
    });

    api.patch(namespace, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .with_context(|| format!("remove action-mode label from namespace {namespace}"))?;
    Ok(())
}

async fn ensure_default_action_policy(client: &Client) -> Result<()> {
    let gvk = GroupVersionKind::gvk(
        ACTION_POLICY_GROUP,
        ACTION_POLICY_VERSION,
        ACTION_POLICY_KIND,
    );
    let ar = ApiResource::from_gvk(&gvk);
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &ar);
    let desired = desired_default_action_policy()?;

    match api.get_opt(DEFAULT_ACTION_POLICY_NAME).await? {
        Some(_) => {
            let patch = serde_json::to_value(&desired)?;
            api.patch(
                DEFAULT_ACTION_POLICY_NAME,
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await
            .context("patch default action policy")?;
        }
        None => {
            api.create(&PostParams::default(), &desired)
                .await
                .context("create default action policy")?;
        }
    }

    Ok(())
}

async fn remove_default_action_policy(client: &Client) -> Result<()> {
    let gvk = GroupVersionKind::gvk(
        ACTION_POLICY_GROUP,
        ACTION_POLICY_VERSION,
        ACTION_POLICY_KIND,
    );
    let ar = ApiResource::from_gvk(&gvk);
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &ar);

    if api.get_opt(DEFAULT_ACTION_POLICY_NAME).await?.is_some() {
        api.delete(DEFAULT_ACTION_POLICY_NAME, &DeleteParams::default())
            .await
            .context("delete default action policy")?;
    }

    Ok(())
}

fn desired_default_action_policy() -> Result<DynamicObject> {
    Ok(serde_json::from_value(json!({
        "apiVersion": "sentinella.io/v1alpha1",
        "kind": "SentinellaHubActionPolicy",
        "metadata": {
            "name": DEFAULT_ACTION_POLICY_NAME,
            "labels": {
                "app.kubernetes.io/part-of": "sentinella",
                "sentinella.io/managed-by": "sentinella-hub-k8s-agent"
            }
        },
        "spec": {
            "allowedActions": [
                "diagnose_postgresql",
                "preview_workload_resources",
                "apply_workload_resources",
                "get_resource_yaml",
                "rollout_restart",
                "scale",
                "self_update",
                "update_agent"
            ],
            "allowedResources": ["Deployment", "StatefulSet", "DaemonSet"]
        }
    }))?)
}

async fn resolve_operator_image(
    client: &Client,
    namespace: &str,
    pod_name: &str,
) -> Result<String> {
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let pod = pod_api
        .get(pod_name)
        .await
        .with_context(|| format!("get agent pod {pod_name}"))?;
    pod.spec
        .and_then(|spec| spec.containers.into_iter().next())
        .and_then(|c| c.image)
        .context("agent pod has no container image")
}

fn is_operator_enabled(cm: &ConfigMap) -> bool {
    cm.data
        .as_ref()
        .and_then(|data| data.get(ACTION_OPERATOR_ENABLED_KEY))
        .map(|v| {
            let trimmed = v.trim();
            trimmed == "true" || trimmed == "1"
        })
        .unwrap_or(false)
}

fn action_operator_excluded_namespaces(cm: &ConfigMap) -> HashSet<String> {
    let extra = cm
        .data
        .as_ref()
        .and_then(|data| data.get("ACTION_OPERATOR_EXCLUDED_NAMESPACES"))
        .and_then(|value| serde_yaml::from_str::<Vec<String>>(value).ok())
        .unwrap_or_default();

    combined_excluded_namespaces(&extra)
}

fn desired_operator_service_account(namespace: &str) -> ServiceAccount {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": OPERATOR_NAME,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": OPERATOR_NAME,
                "app.kubernetes.io/part-of": "sentinella",
                "sentinella.io/managed-by": "sentinella-hub-k8s-agent"
            }
        }
    }))
    .expect("static operator ServiceAccount JSON is valid")
}

fn desired_operator_deployment(namespace: &str, image: &str) -> Result<Deployment> {
    Ok(serde_json::from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": OPERATOR_NAME,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": OPERATOR_NAME,
                "app.kubernetes.io/part-of": "sentinella",
                "sentinella.io/managed-by": "sentinella-hub-k8s-agent"
            }
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": {
                    "app.kubernetes.io/name": OPERATOR_NAME
                }
            },
            "template": {
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/name": OPERATOR_NAME,
                        "app.kubernetes.io/part-of": "sentinella"
                    },
                    "annotations": {
                        "prometheus.io/scrape": "true",
                        "prometheus.io/port": "9090",
                        "prometheus.io/path": "/metrics"
                    }
                },
                "spec": {
                    "serviceAccountName": OPERATOR_NAME,
                    "securityContext": {
                        "runAsNonRoot": true,
                        "seccompProfile": { "type": "RuntimeDefault" }
                    },
                    "containers": [{
                        "name": "operator",
                        "image": image,
                        "imagePullPolicy": "IfNotPresent",
                        "args": ["--mode", "operator"],
                        "envFrom": [{
                            "configMapRef": {
                                "name": CONFIG_MAP_NAME
                            }
                        }],
                        "env": [
                            {
                                "name": "POD_NAME",
                                "valueFrom": {
                                    "fieldRef": { "fieldPath": "metadata.name" }
                                }
                            },
                            {
                                "name": "POD_NAMESPACE",
                                "valueFrom": {
                                    "fieldRef": { "fieldPath": "metadata.namespace" }
                                }
                            }
                        ],
                        "ports": [{
                            "name": "metrics",
                            "containerPort": 9090,
                            "protocol": "TCP"
                        }],
                        "startupProbe": {
                            "httpGet": { "path": "/livez", "port": "metrics" },
                            "periodSeconds": 5,
                            "timeoutSeconds": 2,
                            "failureThreshold": 12
                        },
                        "livenessProbe": {
                            "httpGet": { "path": "/livez", "port": "metrics" },
                            "periodSeconds": 30,
                            "timeoutSeconds": 2,
                            "failureThreshold": 3
                        },
                        "readinessProbe": {
                            "httpGet": { "path": "/readyz", "port": "metrics" },
                            "initialDelaySeconds": 5,
                            "periodSeconds": 10,
                            "timeoutSeconds": 2,
                            "failureThreshold": 3
                        },
                        "resources": {
                            "requests": { "cpu": "10m", "memory": "32Mi" },
                            "limits": { "cpu": "100m", "memory": "128Mi" }
                        },
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": { "drop": ["ALL"] }
                        }
                    }]
                }
            }
        }
    }))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn configmap_with_data(data: serde_json::Value) -> ConfigMap {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": CONFIG_MAP_NAME, "namespace": "sentinella" },
            "data": data
        }))
        .unwrap()
    }

    #[test]
    fn is_operator_enabled_true() {
        let cm = configmap_with_data(json!({
            ACTION_OPERATOR_ENABLED_KEY: "true"
        }));
        assert!(is_operator_enabled(&cm));
    }

    #[test]
    fn is_operator_enabled_one() {
        let cm = configmap_with_data(json!({
            ACTION_OPERATOR_ENABLED_KEY: "1"
        }));
        assert!(is_operator_enabled(&cm));
    }

    #[test]
    fn is_operator_enabled_false() {
        let cm = configmap_with_data(json!({
            ACTION_OPERATOR_ENABLED_KEY: "false"
        }));
        assert!(!is_operator_enabled(&cm));
    }

    #[test]
    fn is_operator_enabled_missing_key_defaults_false() {
        let cm = configmap_with_data(json!({
            "HUB_URL": "https://hub.example.com"
        }));
        assert!(!is_operator_enabled(&cm));
    }

    #[test]
    fn is_operator_enabled_missing_data_defaults_false() {
        let cm: ConfigMap = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": CONFIG_MAP_NAME }
        }))
        .unwrap();
        assert!(!is_operator_enabled(&cm));
    }

    #[test]
    fn is_operator_enabled_whitespace_trimmed() {
        let cm = configmap_with_data(json!({
            ACTION_OPERATOR_ENABLED_KEY: "  true  "
        }));
        assert!(is_operator_enabled(&cm));
    }

    #[test]
    fn desired_service_account_has_correct_name_and_labels() {
        let sa = desired_operator_service_account("sentinella");
        assert_eq!(sa.metadata.name.as_deref(), Some(OPERATOR_NAME));
        assert_eq!(sa.metadata.namespace.as_deref(), Some("sentinella"));
        let labels = sa.metadata.labels.as_ref().unwrap();
        assert_eq!(
            labels.get("sentinella.io/managed-by").unwrap(),
            "sentinella-hub-k8s-agent"
        );
    }

    #[test]
    fn desired_deployment_has_correct_metadata_and_image() {
        let deploy = desired_operator_deployment("sentinella", "repo/agent:v1.2.3").unwrap();
        assert_eq!(deploy.metadata.name.as_deref(), Some(OPERATOR_NAME));
        assert_eq!(deploy.metadata.namespace.as_deref(), Some("sentinella"));

        let spec = deploy.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(1));

        let template_spec = spec.template.spec.as_ref().unwrap();
        assert_eq!(
            template_spec.service_account_name.as_deref(),
            Some(OPERATOR_NAME)
        );

        let container = template_spec.containers.first().unwrap();
        assert_eq!(container.name, "operator");
        assert_eq!(container.image.as_deref(), Some("repo/agent:v1.2.3"));
        assert_eq!(
            container.args.as_deref(),
            Some(["--mode".to_string(), "operator".to_string()].as_slice())
        );
    }

    #[test]
    fn desired_deployment_has_probes_and_security_context() {
        let deploy = desired_operator_deployment("sentinella", "repo/agent:v1.0.0").unwrap();
        let spec = deploy.spec.as_ref().unwrap();
        let template_spec = spec.template.spec.as_ref().unwrap();

        let sc = template_spec.security_context.as_ref().unwrap();
        assert!(sc.run_as_non_root == Some(true));
        assert_eq!(sc.run_as_user, None);
        assert_eq!(sc.run_as_group, None);

        let container = template_spec.containers.first().unwrap();
        assert!(container.startup_probe.is_some());
        assert!(container.liveness_probe.is_some());
        assert!(container.readiness_probe.is_some());

        let csc = container.security_context.as_ref().unwrap();
        assert!(!csc.allow_privilege_escalation.unwrap_or(true));
        assert!(csc.read_only_root_filesystem.unwrap_or(false));
    }

    #[test]
    fn desired_deployment_uses_configmap_envfrom() {
        let deploy = desired_operator_deployment("sentinella", "repo/agent:v1.0.0").unwrap();
        let spec = deploy.spec.as_ref().unwrap();
        let template_spec = spec.template.spec.as_ref().unwrap();
        let container = template_spec.containers.first().unwrap();

        let env_from = container.env_from.as_ref().unwrap();
        assert_eq!(env_from.len(), 1);
        let cm_ref = env_from[0].config_map_ref.as_ref().unwrap();
        assert_eq!(cm_ref.name, CONFIG_MAP_NAME);
    }

    #[test]
    fn desired_default_action_policy_has_default_scope_and_actions() {
        let policy = desired_default_action_policy().unwrap();
        let value = serde_json::to_value(&policy).unwrap();

        assert_eq!(value["metadata"]["name"], DEFAULT_ACTION_POLICY_NAME);
        assert!(value["spec"]["namespaceSelector"].is_null());
        assert_eq!(
            value["spec"]["allowedActions"],
            json!([
                "diagnose_postgresql",
                "preview_workload_resources",
                "apply_workload_resources",
                "get_resource_yaml",
                "rollout_restart",
                "scale",
                "self_update",
                "update_agent"
            ])
        );
    }

    #[test]
    fn action_operator_excluded_namespaces_parses_yaml_list() {
        let cm = configmap_with_data(json!({
            "ACTION_OPERATOR_EXCLUDED_NAMESPACES": "- kube-system\n- openshift-*\n- custom-ns"
        }));

        let excluded = action_operator_excluded_namespaces(&cm);

        assert!(excluded.contains("kube-system"));
        assert!(excluded.contains("openshift-*"));
        assert!(excluded.contains("custom-ns"));
    }
}
