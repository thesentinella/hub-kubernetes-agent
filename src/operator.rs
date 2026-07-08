use crate::config::Config;
use crate::executor::{
    ACTION_MODE_NAMESPACE_LABEL, ACTION_MODE_NAMESPACE_LABEL_ENABLED, label_selector_matches,
};
use crate::model::SentinellaActionPolicy;
use anyhow::{Context, Result};
use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::api::rbac::v1::RoleBinding;
use kube::api::{
    ApiResource, DeleteParams, DynamicObject, ListParams, Patch, PatchParams, PostParams,
};
use kube::core::GroupVersionKind;
use kube::{Api, Client};
use serde_json::json;
use std::collections::{BTreeMap, HashSet};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info, warn};

const ACTION_POLICY_GROUP: &str = "sentinella.io";
const ACTION_POLICY_VERSION: &str = "v1alpha1";
const ACTION_POLICY_KIND: &str = "SentinellaActionPolicy";
const ACTION_ROLE_BINDING_NAME: &str = "sentinella-hub-k8s-agent-action-mode";
const ACTION_CLUSTER_ROLE_NAME: &str = "sentinella-hub-k8s-agent-action-mode";
const ACTION_SERVICE_ACCOUNT_NAMESPACE: &str = "sentinella";
const ACTION_SERVICE_ACCOUNT_NAME: &str = "sentinella-hub-k8s-agent";

pub async fn run_action_operator(cfg: Config, client: Client) {
    if !cfg.action_operator_enabled {
        return;
    }

    info!(
        poll_interval_secs = cfg.action_operator_poll_interval.as_secs(),
        "action operator enabled"
    );

    let mut ticker = interval(cfg.action_operator_poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        if let Err(err) = reconcile_action_bindings(&client).await {
            warn!(error = %err, "action operator reconcile failed");
        } else {
            debug!("action operator reconcile complete");
        }
    }
}

async fn reconcile_action_bindings(client: &Client) -> Result<()> {
    let namespaces = list_namespaces(client).await?;
    let policies = list_policies(client).await?;

    let mut eligible_namespaces = HashSet::new();
    for namespace in &namespaces {
        if namespace_is_eligible(namespace, &policies) {
            if let Some(name) = namespace.metadata.name.as_ref() {
                eligible_namespaces.insert(name.clone());
            }
        }
    }

    for namespace in namespaces {
        let Some(name) = namespace.metadata.name.as_deref() else {
            continue;
        };

        if eligible_namespaces.contains(name) {
            ensure_action_role_binding(client, name).await?;
        } else {
            remove_action_role_binding(client, name).await?;
        }
    }

    Ok(())
}

async fn list_namespaces(client: &Client) -> Result<Vec<Namespace>> {
    let api: Api<Namespace> = Api::all(client.clone());
    Ok(api.list(&ListParams::default()).await?.items)
}

async fn list_policies(client: &Client) -> Result<Vec<SentinellaActionPolicy>> {
    let gvk = GroupVersionKind::gvk(
        ACTION_POLICY_GROUP,
        ACTION_POLICY_VERSION,
        ACTION_POLICY_KIND,
    );
    let ar = ApiResource::from_gvk(&gvk);
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &ar);
    let list = api.list(&ListParams::default()).await?;

    let mut out = Vec::new();
    for policy in list {
        let value = serde_json::to_value(&policy).context("serialize action policy")?;
        let policy: SentinellaActionPolicy =
            serde_json::from_value(value).context("deserialize action policy")?;
        out.push(policy);
    }

    Ok(out)
}

fn namespace_is_eligible(namespace: &Namespace, policies: &[SentinellaActionPolicy]) -> bool {
    let Some(labels) = namespace.metadata.labels.as_ref() else {
        return false;
    };

    if labels.get(ACTION_MODE_NAMESPACE_LABEL).map(String::as_str)
        != Some(ACTION_MODE_NAMESPACE_LABEL_ENABLED)
    {
        return false;
    }

    policies
        .iter()
        .any(|policy| policy_matches_namespace(policy, labels))
}

fn policy_matches_namespace(
    policy: &SentinellaActionPolicy,
    labels: &BTreeMap<String, String>,
) -> bool {
    let Some(selector) = policy.spec.namespace_selector.as_ref() else {
        return false;
    };

    if selector.match_labels.is_none() && selector.match_expressions.is_none() {
        return false;
    }

    label_selector_matches(Some(selector), labels)
}

async fn ensure_action_role_binding(client: &Client, namespace: &str) -> Result<()> {
    let api: Api<RoleBinding> = Api::namespaced(client.clone(), namespace);
    let desired = desired_action_role_binding(namespace)?;

    let existing = api.get_opt(ACTION_ROLE_BINDING_NAME).await?;
    let desired_value = serde_json::to_value(&desired).context("serialize action RoleBinding")?;

    match existing {
        Some(_) => {
            api.patch(
                ACTION_ROLE_BINDING_NAME,
                &PatchParams::default(),
                &Patch::Merge(&desired_value),
            )
            .await
            .context("patch action RoleBinding")?;
        }
        None => {
            api.create(&PostParams::default(), &desired)
                .await
                .context("create action RoleBinding")?;
        }
    }

    Ok(())
}

async fn remove_action_role_binding(client: &Client, namespace: &str) -> Result<()> {
    let api: Api<RoleBinding> = Api::namespaced(client.clone(), namespace);
    if api.get_opt(ACTION_ROLE_BINDING_NAME).await?.is_some() {
        api.delete(ACTION_ROLE_BINDING_NAME, &DeleteParams::default())
            .await
            .context("delete action RoleBinding")?;
    }

    Ok(())
}

fn desired_action_role_binding(namespace: &str) -> Result<RoleBinding> {
    serde_json::from_value(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {
            "name": ACTION_ROLE_BINDING_NAME,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/part-of": "sentinella",
                "sentinella.io/managed-by": "sentinella-hub-k8s-agent",
                ACTION_MODE_NAMESPACE_LABEL: ACTION_MODE_NAMESPACE_LABEL_ENABLED,
            },
            "annotations": {
                "sentinella.io/policy-scope": "cluster-scoped"
            }
        },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": ACTION_CLUSTER_ROLE_NAME,
        },
        "subjects": [{
            "kind": "ServiceAccount",
            "name": ACTION_SERVICE_ACCOUNT_NAME,
            "namespace": ACTION_SERVICE_ACCOUNT_NAMESPACE,
        }]
    }))
    .map_err(|e| anyhow::anyhow!("failed to build desired RoleBinding: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn namespace(labels: Option<Value>) -> Namespace {
        let mut ns: Namespace = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": "app-prod" }
        }))
        .unwrap();
        ns.metadata.labels = labels.and_then(|value| serde_json::from_value(value).ok());
        ns
    }

    fn policy(selector: Value) -> SentinellaActionPolicy {
        serde_json::from_value(json!({
            "apiVersion": "sentinella.io/v1alpha1",
            "kind": "SentinellaActionPolicy",
            "metadata": { "name": "workload-tuning" },
            "spec": {
                "namespaceSelector": selector,
                "allowedActions": ["patchWorkloadResources"],
                "allowedResources": ["deployments"],
                "approvalRequired": true
            }
        }))
        .unwrap()
    }

    #[test]
    fn namespace_is_eligible_requires_label_and_matching_policy() {
        let labeled = namespace(Some(json!({
            ACTION_MODE_NAMESPACE_LABEL: ACTION_MODE_NAMESPACE_LABEL_ENABLED,
            "environment": "prod"
        })));
        let unlabeled = namespace(Some(json!({
            "environment": "prod"
        })));
        let mismatched = namespace(Some(json!({
            ACTION_MODE_NAMESPACE_LABEL: ACTION_MODE_NAMESPACE_LABEL_ENABLED,
            "environment": "qa"
        })));
        let policies = vec![policy(json!({
            "matchLabels": {"environment": "prod"}
        }))];

        assert!(namespace_is_eligible(&labeled, &policies));
        assert!(!namespace_is_eligible(&unlabeled, &policies));
        assert!(!namespace_is_eligible(&mismatched, &policies));
    }

    #[test]
    fn namespace_is_eligible_rejects_missing_selector() {
        let labeled = namespace(Some(json!({
            ACTION_MODE_NAMESPACE_LABEL: ACTION_MODE_NAMESPACE_LABEL_ENABLED
        })));
        let policies = vec![policy(json!({}))];

        assert!(!namespace_is_eligible(&labeled, &policies));
    }

    #[test]
    fn desired_role_binding_targets_sentinella_service_account() {
        let role_binding = desired_action_role_binding("app-prod").unwrap();

        assert_eq!(
            role_binding.metadata.name.as_deref(),
            Some(ACTION_ROLE_BINDING_NAME)
        );
        assert_eq!(role_binding.metadata.namespace.as_deref(), Some("app-prod"));
        assert_eq!(role_binding.role_ref.kind, "ClusterRole");
        assert_eq!(role_binding.role_ref.name, ACTION_CLUSTER_ROLE_NAME);
        let subject = role_binding.subjects.unwrap().into_iter().next().unwrap();
        assert_eq!(subject.kind, "ServiceAccount");
        assert_eq!(subject.name, ACTION_SERVICE_ACCOUNT_NAME);
        assert_eq!(
            subject.namespace.as_deref(),
            Some(ACTION_SERVICE_ACCOUNT_NAMESPACE)
        );
    }
}
