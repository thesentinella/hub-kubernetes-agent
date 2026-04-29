//! Command executor.
//!
//! In this release `actions_enabled = false` is the default and the executor
//! refuses to execute any command. The dispatch table below already lists the
//! v0.2 command contract: known kinds return `not_implemented` (distinct from
//! `unknown`) so the Hub can tell apart "agent too old" from "Hub sent
//! something garbled".
//!
//! When v0.2 lands, each `not_implemented` arm becomes a real handler. The
//! Hub-side contract (kind names, spec shapes, result fields) is already
//! frozen in `model.rs` — Hub developers can start generating these commands
//! against this v0.1 build today and verify the agent receives, parses, and
//! acks them correctly. They will just come back as `not_implemented`.

use crate::config::Config;
use crate::model::{Command, CommandResult, WorkloadResourcesSpec};
use kube::Client;
use tracing::{info, warn};

pub struct Executor {
    cfg: Config,
    #[allow(dead_code)] // used once handlers land in v0.2
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
        _spec: WorkloadResourcesSpec,
    ) -> CommandResult {
        info!(
            command_id,
            "preview_workload_resources: not implemented yet (v0.2)"
        );
        let mut r = CommandResult::simple(
            command_id.to_string(),
            "not_implemented",
            Some("preview_workload_resources will land in v0.2".into()),
        );
        r.dry_run = Some(true);
        r
    }

    async fn apply_workload_resources(
        &self,
        command_id: &str,
        _spec: WorkloadResourcesSpec,
    ) -> CommandResult {
        info!(
            command_id,
            "apply_workload_resources: not implemented yet (v0.2)"
        );
        let mut r = CommandResult::simple(
            command_id.to_string(),
            "not_implemented",
            Some("apply_workload_resources will land in v0.2".into()),
        );
        r.dry_run = Some(false);
        r
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
