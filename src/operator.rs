use crate::config::Config;
use crate::executor::{ACTION_MODE_NAMESPACE_LABEL, ACTION_MODE_NAMESPACE_LABEL_ENABLED};
use crate::model::{
    SentinellaHubActionPolicy, SentinellaHubActionPolicyCondition, SentinellaHubActionPolicyStatus,
    policy_action_is_supported, policy_action_targets_workload, policy_limits_are_valid,
    policy_resource_is_supported, policy_status_is_stale,
};
use anyhow::{Context, Result};
use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::api::rbac::v1::RoleBinding;
use kube::api::{
    ApiResource, DeleteParams, DynamicObject, ListParams, Patch, PatchParams, PostParams,
};
use kube::core::GroupVersionKind;
use kube::{Api, Client};
use serde_json::json;
use std::collections::HashSet;
use std::future::Future;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info, warn};

const ACTION_POLICY_GROUP: &str = "sentinella.io";
const ACTION_POLICY_VERSION: &str = "v1alpha1";
const ACTION_POLICY_KIND: &str = "SentinellaHubActionPolicy";
const ACTION_ROLE_BINDING_NAME: &str = "sentinella-hub-k8s-agent-action-mode";
const ACTION_CLUSTER_ROLE_NAME: &str = "sentinella-hub-k8s-agent-action-mode";
const ACTION_SERVICE_ACCOUNT_NAMESPACE: &str = "sentinella";
const ACTION_SERVICE_ACCOUNT_NAME: &str = "sentinella-hub-k8s-agent";
const SYSTEM_EXCLUDED_NAMESPACES: &[&str] = &[
    "kube-system",
    "kube-public",
    "kube-node-lease",
    "sentinella",
    "tetragon",
];

pub async fn run_action_operator(cfg: Config, client: Client) {
    info!(
        poll_interval_secs = cfg.action_operator_poll_interval.as_secs(),
        "action operator enabled"
    );

    let mut ticker = interval(cfg.action_operator_poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        if let Err(err) = reconcile_action_policies(&cfg, &client).await {
            warn!(error = %err, "action operator reconcile failed");
        } else {
            debug!("action operator reconcile complete");
        }
    }
}

async fn reconcile_action_policies(cfg: &Config, client: &Client) -> Result<()> {
    let now = now_ms();
    let stale_after_ms = cfg
        .action_operator_poll_interval
        .checked_mul(3)
        .unwrap_or(cfg.action_operator_poll_interval)
        .as_millis();
    let excluded_namespaces =
        combined_excluded_namespaces(&cfg.action_operator_excluded_namespaces);

    reconcile_action_policies_with(
        now,
        stale_after_ms,
        excluded_namespaces,
        || list_namespaces(client),
        || list_policies(client),
        |namespaces, policies, excluded_namespaces| async move {
            reconcile_action_role_bindings(client, &namespaces, &policies, &excluded_namespaces)
                .await
        },
        |name, status| async move { patch_policy_status(client, &name, status).await },
    )
    .await
}

fn combined_excluded_namespaces(extra: &[String]) -> HashSet<String> {
    SYSTEM_EXCLUDED_NAMESPACES
        .iter()
        .map(|value| (*value).to_string())
        .chain(extra.iter().map(|value| value.trim().to_string()))
        .filter(|value| !value.is_empty())
        .collect()
}

async fn reconcile_action_policies_with<
    ListNamespaces,
    ListPolicies,
    ReconcileBindings,
    PatchStatus,
    NSFut,
    PolFut,
    BindFut,
    PatchFut,
>(
    now_ms: u128,
    stale_after_ms: u128,
    excluded_namespaces: HashSet<String>,
    list_namespaces: ListNamespaces,
    list_policies: ListPolicies,
    reconcile_bindings: ReconcileBindings,
    patch_status: PatchStatus,
) -> Result<()>
where
    ListNamespaces: Fn() -> NSFut,
    NSFut: Future<Output = Result<Vec<Namespace>>>,
    ListPolicies: Fn() -> PolFut,
    PolFut: Future<Output = Result<Vec<SentinellaHubActionPolicy>>>,
    ReconcileBindings:
        Fn(Vec<Namespace>, Vec<SentinellaHubActionPolicy>, HashSet<String>) -> BindFut,
    BindFut: Future<Output = Result<BindingReconcileOutcome>>,
    PatchStatus: Fn(String, SentinellaHubActionPolicyStatus) -> PatchFut,
    PatchFut: Future<Output = Result<()>>,
{
    let namespaces = list_namespaces().await?;
    let policies = list_policies().await?;
    let binding_outcome = reconcile_bindings(
        namespaces.clone(),
        policies.clone(),
        excluded_namespaces.clone(),
    )
    .await?;

    for policy in &policies {
        let Some(name) = policy.metadata.name.as_deref() else {
            continue;
        };

        let status = build_policy_status(
            policy,
            &namespaces,
            &excluded_namespaces,
            &binding_outcome,
            now_ms,
            stale_after_ms,
        );

        if let Err(err) = patch_status(name.to_string(), status).await {
            warn!(policy = %name, error = %err, "failed to update action policy status");
        }
    }

    if let Some(error) = binding_outcome.reconciliation_error {
        warn!(error = %error, "action operator reconciliation completed with errors");
    }

    Ok(())
}

async fn list_namespaces(client: &Client) -> Result<Vec<Namespace>> {
    let api: Api<Namespace> = Api::all(client.clone());
    Ok(api.list(&ListParams::default()).await?.items)
}

async fn list_policies(client: &Client) -> Result<Vec<SentinellaHubActionPolicy>> {
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
        let policy: SentinellaHubActionPolicy =
            serde_json::from_value(value).context("deserialize action policy")?;
        out.push(policy);
    }

    Ok(out)
}

async fn patch_policy_status(
    client: &Client,
    name: &str,
    status: SentinellaHubActionPolicyStatus,
) -> Result<()> {
    let gvk = GroupVersionKind::gvk(
        ACTION_POLICY_GROUP,
        ACTION_POLICY_VERSION,
        ACTION_POLICY_KIND,
    );
    let ar = ApiResource::from_gvk(&gvk);
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &ar);
    let patch = json!({"status": status});

    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .with_context(|| format!("patching status for policy {}", name))?;
    Ok(())
}

fn effective_namespaces(
    namespaces: &[Namespace],
    excluded_namespaces: &HashSet<String>,
) -> Vec<String> {
    let mut effective = Vec::new();

    for namespace in namespaces {
        if let Some(name) = namespace.metadata.name.as_ref() {
            if !namespace_is_excluded(name, excluded_namespaces) {
                effective.push(name.clone());
            }
        }
    }

    effective.sort();
    effective
}
fn namespace_is_excluded(name: &str, excluded_namespaces: &HashSet<String>) -> bool {
    excluded_namespaces.contains(name)
}

struct BindingReconcileOutcome {
    permission_granted: bool,
    reconciliation_error: Option<String>,
}

async fn reconcile_action_role_bindings(
    client: &Client,
    namespaces: &[Namespace],
    policies: &[SentinellaHubActionPolicy],
    excluded_namespaces: &HashSet<String>,
) -> Result<BindingReconcileOutcome> {
    reconcile_action_role_bindings_with(
        namespaces,
        policies,
        excluded_namespaces,
        |namespace| ensure_action_role_binding(client, namespace),
        |namespace| remove_action_role_binding(client, namespace),
    )
    .await
}

async fn reconcile_action_role_bindings_with<Ensure, Remove, EnsureFuture, RemoveFuture>(
    namespaces: &[Namespace],
    _policies: &[SentinellaHubActionPolicy],
    excluded_namespaces: &HashSet<String>,
    ensure: Ensure,
    remove: Remove,
) -> Result<BindingReconcileOutcome>
where
    Ensure: Fn(String) -> EnsureFuture,
    EnsureFuture: Future<Output = Result<()>>,
    Remove: Fn(String) -> RemoveFuture,
    RemoveFuture: Future<Output = Result<()>>,
{
    let mut errors = Vec::new();

    for namespace in namespaces {
        let Some(name) = namespace.metadata.name.as_deref() else {
            continue;
        };
        let name = name.to_string();
        let action_mode_enabled = namespace_action_mode_enabled(namespace);

        if !namespace_is_excluded(&name, excluded_namespaces) && action_mode_enabled {
            if let Err(err) = ensure(name).await {
                errors.push(err.to_string());
            }
        } else if let Err(err) = remove(name).await {
            errors.push(err.to_string());
        }
    }

    Ok(BindingReconcileOutcome {
        permission_granted: errors.is_empty(),
        reconciliation_error: if errors.is_empty() {
            None
        } else {
            Some(errors.join("; "))
        },
    })
}

fn namespace_action_mode_enabled(namespace: &Namespace) -> bool {
    namespace
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(ACTION_MODE_NAMESPACE_LABEL))
        .map(String::as_str)
        == Some(ACTION_MODE_NAMESPACE_LABEL_ENABLED)
}

async fn ensure_action_role_binding(client: &Client, namespace: String) -> Result<()> {
    let api: Api<RoleBinding> = Api::namespaced(client.clone(), &namespace);
    let desired = desired_action_role_binding(&namespace)?;

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

async fn remove_action_role_binding(client: &Client, namespace: String) -> Result<()> {
    let api: Api<RoleBinding> = Api::namespaced(client.clone(), &namespace);
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

fn build_policy_status(
    policy: &SentinellaHubActionPolicy,
    namespaces: &[Namespace],
    excluded_namespaces: &HashSet<String>,
    binding_outcome: &BindingReconcileOutcome,
    now_ms: u128,
    stale_after_ms: u128,
) -> SentinellaHubActionPolicyStatus {
    let effective_namespaces = effective_namespaces(namespaces, excluded_namespaces);
    let policy_matched = true;
    let namespaces_eligible = !effective_namespaces.is_empty();
    let permission_granted = binding_outcome.permission_granted;
    let reconciliation_error = binding_outcome.reconciliation_error.clone();
    let action_allowed = policy
        .spec
        .allowed_actions
        .iter()
        .all(|action| policy_action_is_supported(action))
        && !policy.spec.allowed_actions.is_empty();
    let resource_allowed = if policy
        .spec
        .allowed_actions
        .iter()
        .any(|action| policy_action_targets_workload(action))
    {
        !policy.spec.allowed_resources.is_empty()
            && policy
                .spec
                .allowed_resources
                .iter()
                .all(|resource| policy_resource_is_supported(resource))
    } else {
        true
    };
    let limits_satisfied = policy_limits_are_valid(&policy.spec.limits);
    let stale = policy_status_is_stale(
        policy
            .status
            .as_ref()
            .and_then(|status| status.last_reconciled_at_ms),
        now_ms,
        stale_after_ms,
    );
    let ready = policy_matched
        && namespaces_eligible
        && permission_granted
        && action_allowed
        && resource_allowed
        && limits_satisfied
        && !stale
        && reconciliation_error.is_none();

    SentinellaHubActionPolicyStatus {
        effective_namespaces,
        conditions: build_policy_conditions(
            policy,
            policy_matched,
            namespaces_eligible,
            permission_granted,
            action_allowed,
            resource_allowed,
            limits_satisfied,
            reconciliation_error.as_deref(),
            stale,
            ready,
            now_ms,
        ),
        observed_generation: policy.metadata.generation,
        last_reconciled_at_ms: Some(now_ms),
        stale,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_policy_conditions(
    policy: &SentinellaHubActionPolicy,
    policy_matched: bool,
    namespaces_eligible: bool,
    permission_granted: bool,
    action_allowed: bool,
    resource_allowed: bool,
    limits_satisfied: bool,
    reconciliation_error: Option<&str>,
    stale: bool,
    ready: bool,
    now_ms: u128,
) -> Vec<SentinellaHubActionPolicyCondition> {
    let observed_generation = policy.metadata.generation;
    let mut conditions = vec![
        policy_condition(
            "NamespacesEligible",
            namespaces_eligible,
            if namespaces_eligible {
                Some("eligible_namespaces_found")
            } else {
                Some("all_namespaces_excluded")
            },
            if namespaces_eligible {
                Some(
                    "At least one namespace is eligible after applying the operator exclude list"
                        .into(),
                )
            } else {
                Some("All namespaces are excluded from action-mode reconciliation".into())
            },
            observed_generation,
            now_ms,
        ),
        policy_condition(
            "PolicyMatched",
            policy_matched,
            Some("global_exclude_list_active"),
            Some("Policy is evaluated under the global namespace exclude-list model".into()),
            observed_generation,
            now_ms,
        ),
        policy_condition(
            "PermissionGranted",
            permission_granted,
            if permission_granted {
                Some("rolebinding_reconciled")
            } else {
                Some("rolebinding_reconcile_failed")
            },
            if permission_granted {
                Some("Managed RoleBindings were reconciled successfully".into())
            } else {
                Some("Managed RoleBindings are not fully reconciled".into())
            },
            observed_generation,
            now_ms,
        ),
        policy_condition(
            "ActionAllowed",
            action_allowed,
            if action_allowed {
                Some("action_allowed")
            } else {
                Some("action_not_allowed")
            },
            if action_allowed {
                Some("At least one declared action is supported".into())
            } else {
                Some("No supported actions are declared".into())
            },
            observed_generation,
            now_ms,
        ),
        policy_condition(
            "ResourceAllowed",
            resource_allowed,
            if resource_allowed {
                Some("resource_allowed")
            } else {
                Some("resource_not_allowed")
            },
            if resource_allowed {
                Some("Declared resources are compatible with declared actions".into())
            } else {
                Some("Declared resources do not permit the declared actions".into())
            },
            observed_generation,
            now_ms,
        ),
        policy_condition(
            "LimitsSatisfied",
            limits_satisfied,
            if limits_satisfied {
                Some("limits_satisfied")
            } else {
                Some("limits_exceeded")
            },
            if limits_satisfied {
                Some("Declared limits are valid".into())
            } else {
                Some("Declared limits are invalid".into())
            },
            observed_generation,
            now_ms,
        ),
        policy_condition(
            "ReconciliationError",
            reconciliation_error.is_some(),
            if reconciliation_error.is_some() {
                Some("reconcile_error")
            } else {
                Some("reconcile_ok")
            },
            reconciliation_error.map(|msg| format!("Reconciliation error: {}", msg)),
            observed_generation,
            now_ms,
        ),
        policy_condition(
            "Stale",
            stale,
            if stale {
                Some("status_stale")
            } else {
                Some("status_fresh")
            },
            if stale {
                Some("Policy status is stale".into())
            } else {
                Some("Policy status is fresh".into())
            },
            observed_generation,
            now_ms,
        ),
    ];

    conditions.push(policy_condition(
        "Ready",
        ready,
        if ready {
            Some("ready")
        } else {
            Some("not_ready")
        },
        if ready {
            Some("Policy is Ready for global agent action checks".into())
        } else {
            Some("Policy is not Ready for global agent action checks".into())
        },
        observed_generation,
        now_ms,
    ));

    conditions
}

fn policy_condition(
    type_: &str,
    is_true: bool,
    reason: Option<&str>,
    message: Option<String>,
    observed_generation: Option<i64>,
    now_ms: u128,
) -> SentinellaHubActionPolicyCondition {
    SentinellaHubActionPolicyCondition {
        type_: type_.to_string(),
        status: if is_true { "True" } else { "False" }.to_string(),
        reason: reason.map(ToString::to_string),
        message,
        observed_generation,
        last_transition_time_ms: Some(now_ms),
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SentinellaHubActionPolicyLimits;
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};

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

    fn policy_named(name: &str, selector: Value) -> SentinellaHubActionPolicy {
        serde_json::from_value(json!({
            "apiVersion": "sentinella.io/v1alpha1",
            "kind": "SentinellaHubActionPolicy",
            "metadata": { "name": name },
            "spec": {
                "namespaceSelector": selector,
                "allowedActions": ["rollout_restart", "scale", "preview_workload_resources", "apply_workload_resources", "self_update", "update_agent", "diagnose_postgresql"],
                "allowedResources": ["Deployment", "StatefulSet", "DaemonSet"],
                "approvalRequired": true
            }
        }))
        .unwrap()
    }

    fn policy(selector: Value) -> SentinellaHubActionPolicy {
        policy_named("workload-tuning", selector)
    }

    fn namespace_with_name(name: &str, labels: Option<Value>) -> Namespace {
        let mut ns = namespace(labels);
        ns.metadata.name = Some(name.into());
        ns
    }

    fn binding_outcome(
        permission_granted: bool,
        reconciliation_error: Option<&str>,
    ) -> BindingReconcileOutcome {
        BindingReconcileOutcome {
            permission_granted,
            reconciliation_error: reconciliation_error.map(str::to_string),
        }
    }

    fn ready_conditions(
        stale: bool,
        ready: bool,
        reconciliation_error: Option<&str>,
    ) -> Vec<SentinellaHubActionPolicyCondition> {
        let policy = policy(json!({
            "matchLabels": {"environment": "prod"}
        }));

        build_policy_conditions(
            &policy,
            true,
            true,
            reconciliation_error.is_none(),
            true,
            true,
            true,
            reconciliation_error,
            stale,
            ready,
            1234,
        )
    }

    fn condition_status(conditions: &[SentinellaHubActionPolicyCondition], name: &str) -> String {
        conditions
            .iter()
            .find(|condition| condition.type_ == name)
            .map(|condition| condition.status.clone())
            .unwrap_or_default()
    }

    #[test]
    fn build_policy_status_marks_ready_when_all_gates_pass() {
        let namespaces = vec![namespace(Some(json!({
            ACTION_MODE_NAMESPACE_LABEL: ACTION_MODE_NAMESPACE_LABEL_ENABLED,
            "environment": "prod"
        })))];
        let excluded_namespaces = HashSet::new();
        let mut policy = policy(json!({
            "matchLabels": {"environment": "prod"}
        }));
        policy.status = Some(SentinellaHubActionPolicyStatus {
            effective_namespaces: Vec::new(),
            conditions: Vec::new(),
            observed_generation: Some(1),
            last_reconciled_at_ms: Some(900),
            stale: false,
        });

        let status = build_policy_status(
            &policy,
            &namespaces,
            &excluded_namespaces,
            &binding_outcome(true, None),
            1000,
            150,
        );

        assert_eq!(status.effective_namespaces, vec!["app-prod"]);
        assert_eq!(condition_status(&status.conditions, "Ready"), "True");
        assert_eq!(
            condition_status(&status.conditions, "ActionAllowed"),
            "True"
        );
        assert_eq!(
            condition_status(&status.conditions, "ResourceAllowed"),
            "True"
        );
        assert_eq!(
            condition_status(&status.conditions, "LimitsSatisfied"),
            "True"
        );
        assert_eq!(condition_status(&status.conditions, "Stale"), "False");
        assert_eq!(
            condition_status(&status.conditions, "PermissionGranted"),
            "True"
        );
        assert_eq!(
            condition_status(&status.conditions, "ReconciliationError"),
            "False"
        );
        assert!(!status.stale);
    }

    #[test]
    fn build_policy_status_marks_not_ready_on_partial_rbac_failure() {
        let namespaces = vec![namespace(Some(json!({
            ACTION_MODE_NAMESPACE_LABEL: ACTION_MODE_NAMESPACE_LABEL_ENABLED,
            "environment": "prod"
        })))];
        let excluded_namespaces = HashSet::new();
        let mut policy = policy(json!({
            "matchLabels": {"environment": "prod"}
        }));
        policy.status = Some(SentinellaHubActionPolicyStatus {
            effective_namespaces: Vec::new(),
            conditions: Vec::new(),
            observed_generation: Some(1),
            last_reconciled_at_ms: Some(900),
            stale: false,
        });

        let status = build_policy_status(
            &policy,
            &namespaces,
            &excluded_namespaces,
            &binding_outcome(false, Some("cannot reconcile app-prod")),
            1000,
            150,
        );

        assert_eq!(condition_status(&status.conditions, "Ready"), "False");
        assert_eq!(
            condition_status(&status.conditions, "PermissionGranted"),
            "False"
        );
        assert_eq!(
            condition_status(&status.conditions, "ReconciliationError"),
            "True"
        );
        assert_eq!(condition_status(&status.conditions, "Stale"), "False");
    }

    #[test]
    fn build_policy_status_marks_stale_when_timestamp_missing() {
        let namespaces = vec![namespace(Some(json!({
            ACTION_MODE_NAMESPACE_LABEL: ACTION_MODE_NAMESPACE_LABEL_ENABLED,
            "environment": "prod"
        })))];
        let excluded_namespaces = HashSet::new();
        let mut policy = policy(json!({
            "matchLabels": {"environment": "prod"}
        }));
        policy.status = Some(SentinellaHubActionPolicyStatus {
            effective_namespaces: Vec::new(),
            conditions: Vec::new(),
            observed_generation: Some(1),
            last_reconciled_at_ms: None,
            stale: false,
        });

        let status = build_policy_status(
            &policy,
            &namespaces,
            &excluded_namespaces,
            &binding_outcome(true, None),
            1000,
            150,
        );

        assert!(status.stale);
        assert_eq!(condition_status(&status.conditions, "Ready"), "False");
        assert_eq!(condition_status(&status.conditions, "Stale"), "True");
    }

    #[test]
    fn build_policy_status_marks_not_ready_when_no_eligible_namespaces() {
        let namespaces = vec![namespace_with_name(
            "app-qa",
            Some(json!({
                "environment": "qa"
            })),
        )];
        let excluded_namespaces = HashSet::from(["app-qa".to_string()]);
        let mut policy = policy(json!({
            "matchLabels": {"environment": "prod"}
        }));
        policy.status = Some(SentinellaHubActionPolicyStatus {
            effective_namespaces: Vec::new(),
            conditions: Vec::new(),
            observed_generation: Some(1),
            last_reconciled_at_ms: Some(900),
            stale: false,
        });

        let status = build_policy_status(
            &policy,
            &namespaces,
            &excluded_namespaces,
            &binding_outcome(true, None),
            1000,
            150,
        );

        assert_eq!(
            condition_status(&status.conditions, "NamespacesEligible"),
            "False"
        );
        assert_eq!(condition_status(&status.conditions, "Ready"), "False");
    }

    #[test]
    fn build_policy_status_marks_not_ready_when_action_is_not_allowed() {
        let namespaces = vec![namespace(Some(json!({
            ACTION_MODE_NAMESPACE_LABEL: ACTION_MODE_NAMESPACE_LABEL_ENABLED,
            "environment": "prod"
        })))];
        let excluded_namespaces = HashSet::new();
        let mut policy = policy(json!({
            "matchLabels": {"environment": "prod"}
        }));
        policy.spec.allowed_actions = vec!["bogus_action".into()];
        policy.status = Some(SentinellaHubActionPolicyStatus {
            effective_namespaces: Vec::new(),
            conditions: Vec::new(),
            observed_generation: Some(1),
            last_reconciled_at_ms: Some(900),
            stale: false,
        });

        let status = build_policy_status(
            &policy,
            &namespaces,
            &excluded_namespaces,
            &binding_outcome(true, None),
            1000,
            150,
        );

        assert_eq!(
            condition_status(&status.conditions, "ActionAllowed"),
            "False"
        );
        assert_eq!(condition_status(&status.conditions, "Ready"), "False");
    }

    #[test]
    fn build_policy_status_marks_not_ready_when_resource_is_not_allowed() {
        let namespaces = vec![namespace(Some(json!({
            ACTION_MODE_NAMESPACE_LABEL: ACTION_MODE_NAMESPACE_LABEL_ENABLED,
            "environment": "prod"
        })))];
        let excluded_namespaces = HashSet::new();
        let mut policy = policy(json!({
            "matchLabels": {"environment": "prod"}
        }));
        policy.spec.allowed_resources = vec!["ConfigMap".into()];
        policy.status = Some(SentinellaHubActionPolicyStatus {
            effective_namespaces: Vec::new(),
            conditions: Vec::new(),
            observed_generation: Some(1),
            last_reconciled_at_ms: Some(900),
            stale: false,
        });

        let status = build_policy_status(
            &policy,
            &namespaces,
            &excluded_namespaces,
            &binding_outcome(true, None),
            1000,
            150,
        );

        assert_eq!(
            condition_status(&status.conditions, "ResourceAllowed"),
            "False"
        );
        assert_eq!(condition_status(&status.conditions, "Ready"), "False");
    }

    #[test]
    fn build_policy_status_marks_not_ready_when_limits_are_invalid() {
        let namespaces = vec![namespace(Some(json!({
            ACTION_MODE_NAMESPACE_LABEL: ACTION_MODE_NAMESPACE_LABEL_ENABLED,
            "environment": "prod"
        })))];
        let excluded_namespaces = HashSet::new();
        let mut policy = policy(json!({
            "matchLabels": {"environment": "prod"}
        }));
        policy.spec.limits = Some(SentinellaHubActionPolicyLimits {
            max_cpu_limit: Some("bogus".into()),
            max_memory_limit: Some("1Gi".into()),
        });
        policy.status = Some(SentinellaHubActionPolicyStatus {
            effective_namespaces: Vec::new(),
            conditions: Vec::new(),
            observed_generation: Some(1),
            last_reconciled_at_ms: Some(900),
            stale: false,
        });

        let status = build_policy_status(
            &policy,
            &namespaces,
            &excluded_namespaces,
            &binding_outcome(true, None),
            1000,
            150,
        );

        assert_eq!(
            condition_status(&status.conditions, "LimitsSatisfied"),
            "False"
        );
        assert_eq!(condition_status(&status.conditions, "Ready"), "False");
    }

    #[tokio::test]
    async fn reconcile_action_policies_with_patches_each_policy_even_when_binding_fails() {
        let namespaces = vec![namespace_with_name(
            "app-prod",
            Some(json!({
                ACTION_MODE_NAMESPACE_LABEL: ACTION_MODE_NAMESPACE_LABEL_ENABLED,
                "environment": "prod"
            })),
        )];
        let mut policy_a = policy_named(
            "workload-a",
            json!({"matchLabels": {"environment": "prod"}}),
        );
        policy_a.status = Some(SentinellaHubActionPolicyStatus {
            effective_namespaces: Vec::new(),
            conditions: Vec::new(),
            observed_generation: Some(1),
            last_reconciled_at_ms: Some(900),
            stale: false,
        });
        let mut policy_b = policy_named(
            "workload-b",
            json!({"matchLabels": {"environment": "prod"}}),
        );
        policy_b.status = Some(SentinellaHubActionPolicyStatus {
            effective_namespaces: Vec::new(),
            conditions: Vec::new(),
            observed_generation: Some(1),
            last_reconciled_at_ms: Some(900),
            stale: false,
        });

        let patch_calls = Arc::new(Mutex::new(Vec::new()));
        let patch_calls_cloned = Arc::clone(&patch_calls);
        let namespaces_for_test = namespaces.clone();
        let policies_for_test = vec![policy_a.clone(), policy_b.clone()];
        let excluded_namespaces = HashSet::new();

        reconcile_action_policies_with(
            1000,
            150,
            excluded_namespaces,
            move || {
                let namespaces = namespaces_for_test.clone();
                async move { Ok(namespaces) }
            },
            move || {
                let policies = policies_for_test.clone();
                async move { Ok(policies) }
            },
            move |_, _, _| async { Ok(binding_outcome(false, Some("cannot reconcile app-prod"))) },
            move |name, status| {
                let patch_calls = Arc::clone(&patch_calls_cloned);
                async move {
                    patch_calls
                        .lock()
                        .expect("patch_calls lock")
                        .push((name, status));
                    Ok(())
                }
            },
        )
        .await
        .expect("reconcile should complete");

        let patch_calls = patch_calls.lock().expect("patch_calls lock");
        assert_eq!(patch_calls.len(), 2);
        assert_eq!(patch_calls[0].0, "workload-a");
        assert_eq!(patch_calls[1].0, "workload-b");
        assert_eq!(
            condition_status(&patch_calls[0].1.conditions, "Ready"),
            "False"
        );
        assert_eq!(
            condition_status(&patch_calls[1].1.conditions, "Ready"),
            "False"
        );
    }

    #[tokio::test]
    async fn reconcile_action_role_bindings_reports_partial_failure() {
        let namespaces = vec![
            namespace(Some(json!({
                ACTION_MODE_NAMESPACE_LABEL: ACTION_MODE_NAMESPACE_LABEL_ENABLED,
                "environment": "prod"
            }))),
            {
                let mut namespace = namespace(Some(json!({
                    "environment": "qa"
                })));
                namespace.metadata.name = Some("app-qa".into());
                namespace
            },
        ];
        let policies = vec![policy(json!({
            "matchLabels": {"environment": "prod"}
        }))];
        let excluded_namespaces = HashSet::from(["app-qa".to_string()]);

        let ensure_calls = Arc::new(Mutex::new(Vec::new()));
        let remove_calls = Arc::new(Mutex::new(Vec::new()));

        let ensure_calls_cloned = Arc::clone(&ensure_calls);
        let ensure = move |namespace: String| {
            let ensure_calls = Arc::clone(&ensure_calls_cloned);
            async move {
                ensure_calls
                    .lock()
                    .expect("ensure_calls lock")
                    .push(namespace.clone());
                if namespace == "app-prod" {
                    Err(anyhow::anyhow!("cannot reconcile app-prod"))
                } else {
                    Ok(())
                }
            }
        };

        let remove_calls_cloned = Arc::clone(&remove_calls);
        let remove = move |namespace: String| {
            let remove_calls = Arc::clone(&remove_calls_cloned);
            async move {
                remove_calls
                    .lock()
                    .expect("remove_calls lock")
                    .push(namespace);
                Ok(())
            }
        };

        let outcome = reconcile_action_role_bindings_with(
            &namespaces,
            &policies,
            &excluded_namespaces,
            ensure,
            remove,
        )
        .await
        .expect("binding reconciliation should complete");

        assert!(!outcome.permission_granted);
        assert_eq!(
            outcome.reconciliation_error.as_deref(),
            Some("cannot reconcile app-prod")
        );
        assert_eq!(
            ensure_calls.lock().expect("ensure_calls lock").as_slice(),
            &["app-prod".to_string()]
        );
        assert_eq!(
            remove_calls.lock().expect("remove_calls lock").as_slice(),
            &["app-qa".to_string()]
        );
    }

    #[test]
    fn namespace_is_excluded_matches_additive_denylist() {
        let excluded_namespaces =
            HashSet::from(["app-prod".to_string(), "kube-system".to_string()]);

        assert!(namespace_is_excluded("app-prod", &excluded_namespaces));
        assert!(namespace_is_excluded("kube-system", &excluded_namespaces));
        assert!(!namespace_is_excluded("app-qa", &excluded_namespaces));
    }

    #[test]
    fn effective_namespaces_excludes_blocked_namespaces() {
        let namespaces = vec![
            namespace_with_name("app-prod", Some(json!({"environment": "prod"}))),
            namespace_with_name("app-qa", Some(json!({"environment": "qa"}))),
        ];
        let excluded_namespaces = HashSet::from(["app-qa".to_string()]);

        assert_eq!(
            effective_namespaces(&namespaces, &excluded_namespaces),
            vec!["app-prod".to_string()]
        );
    }

    #[test]
    fn policy_status_is_fresh_when_timestamp_is_recent() {
        assert!(!policy_status_is_stale(Some(900), 1000, 150));
    }

    #[test]
    fn policy_status_is_stale_when_timestamp_exceeds_window() {
        assert!(policy_status_is_stale(Some(800), 1000, 150));
    }

    #[test]
    fn policy_status_is_stale_when_timestamp_missing() {
        assert!(policy_status_is_stale(None, 1000, 150));
    }

    #[test]
    fn build_policy_conditions_marks_ready_when_all_gates_pass() {
        let conditions = ready_conditions(false, true, None);

        assert_eq!(condition_status(&conditions, "Ready"), "True");
        assert_eq!(condition_status(&conditions, "Stale"), "False");
        assert_eq!(condition_status(&conditions, "PermissionGranted"), "True");
        assert_eq!(
            condition_status(&conditions, "ReconciliationError"),
            "False"
        );
    }

    #[test]
    fn build_policy_conditions_marks_not_ready_on_rbac_error() {
        let conditions = ready_conditions(false, false, Some("rbac denied"));

        assert_eq!(condition_status(&conditions, "Ready"), "False");
        assert_eq!(condition_status(&conditions, "PermissionGranted"), "False");
        assert_eq!(condition_status(&conditions, "ReconciliationError"), "True");
    }

    #[test]
    fn build_policy_conditions_marks_not_ready_when_stale() {
        let conditions = ready_conditions(true, false, None);

        assert_eq!(condition_status(&conditions, "Ready"), "False");
        assert_eq!(condition_status(&conditions, "Stale"), "True");
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
