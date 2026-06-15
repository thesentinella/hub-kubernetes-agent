# 0001 — Workload Monitoring Plugin Contract

Status: proposed
Owners: Sentinella Hub Agent
Related issues: SEN-317 (parent), SEN-318 (agent), SEN-319 (hub-engine), SEN-320 (AI), SEN-321 (UI), SEN-322 (demo)

## Context

A prospective customer wants Sentinella Hub to monitor specific application workloads, not only the cluster as a whole. The target customer stack is Angular frontend, Spring Boot backend, and Oracle Database running on Kubernetes (initial demo target: k3s in the team's current environment; broader target list: OpenShift, Tanzu, MicroK8s, Docker-based local/dev Kubernetes). This is the monitored workload stack, not Sentinella Hub's internal stack.

The existing agent already collects cluster-wide inventory. We need a plugin-style capability that focuses on a user-selected allowlist of namespaces and enriches the observed workloads with signals useful for application troubleshooting and sizing.

This document locks the data contract for that plugin so the agent, the Hub backend, the Hub UI, and the AI analysis layer can be implemented in parallel without surprise.

## Scope

In scope for v1:

- A namespace-scoped plugin block in the inventory snapshot.
- ConfigMap/env configuration on the agent side.
- Tech detection rules for Spring Boot, Oracle, and Angular workloads.
- A filtered projection of the existing snapshot fields into the plugin block.

Out of scope for v1 (tracked as follow-up issues in the Sentinella Hub project):

- Hub-pushed plugin configuration (a new command kind or long-poll config channel).
- Hub-side snapshot ingest that recognizes the `plugins` envelope.
- Hub UI for namespace selection and plugin toggling.
- AI troubleshooting and scaling recommendations that consume the plugin signals.
- Sample/demo workloads (Angular + Spring Boot + Oracle) for validation.
- A plugin command kind for runtime reconfiguration.

## Data Contract

The plugin block lives at the top of the snapshot, parallel to the existing cluster-wide fields:

```json
{
  "plugins": {
    "workload_monitoring": {
      "enabled": true,
      "namespaces": ["customer-app", "customer-db"],
      "technology_targets": ["angular", "spring_boot", "oracle_database"],
      "schema_version": 1,
      "generated_at_ms": 1748523600000,
      "signals": {
        "workloads": [ /* WorkloadRef, filtered */ ],
        "pods":      [ /* PodInfo, filtered */ ],
        "services":  [ /* ServiceInfo, filtered */ ],
        "ingresses": [ /* IngressInfo, filtered */ ],
        "events":    [ /* EventInfo, filtered */ ],
        "dependencies": { /* DependencyInventory or null when tetragon off */ }
      }
    }
  }
}
```

- The plugin signals reuse the existing `WorkloadRef`, `PodInfo`, `ServiceInfo`, `IngressInfo`, `EventInfo`, and `DependencyInventory` types. No duplicate DTOs.
- `signals` is keyed by the kind of data; the Hub can ignore keys it does not need.
- `schema_version` starts at `1` and bumps only on breaking changes. Additive changes do not bump.
- `generated_at_ms` lets the Hub reason about plugin freshness independently of the top-level `timestamp_ms`.

When the plugin is disabled or the namespace allowlist is empty, the `plugins` field is **omitted entirely** from the JSON. Hub code can use the presence of `plugins.workload_monitoring` as the signal that the feature is active.

`InventorySnapshot` gains one new top-level field:

- `plugins`: `Option<Plugins>` with `#[serde(skip_serializing_if = "Option::is_none")]`.

The existing fields stay byte-for-byte identical. Existing Hub ingest code keeps working unchanged.

## Configuration

Current (v1):

| Variable | Source | Default | Notes |
|---|---|---|---|
| `WORKLOAD_MONITORING_ENABLED` | ConfigMap | `false` | Master plugin switch. When `false`, the plugin block is omitted from the snapshot. |
| `WORKLOAD_MONITORING_NAMESPACES` | ConfigMap | empty | Comma-separated allowlist of namespaces to monitor. Empty disables the plugin. |
| `WORKLOAD_MONITORING_TARGETS` | ConfigMap | `angular,spring_boot,oracle_database` | Tech detection targets enabled inside the plugin. |

Future (out of scope for v1, tracked as follow-up issues in the Sentinella Hub project):

- Hub-pushed configuration: Hub stores the namespace allowlist per cluster and pushes it to the agent via a new command kind (e.g. `set_plugin_config`) or a long-poll config channel.
- Per-workload overrides: Hub UI lets users pin specific workloads to the plugin or opt them out.

The agent-side flags remain the source of truth for v1. The Hub-pushed path must not silently override the agent flags; the precedence rule will be defined in the follow-up ADR.

## Security and RBAC

- No new RBAC. The existing reader `ClusterRole` already covers everything the plugin needs (pods, services, ingresses, events, workloads, dependencies).
- No privileged access. No `exec` into pods, no `/proc` reads, no env var scraping.
- No command/args leak: the plugin never includes raw `command`/`args` arrays in the plugin payload. Only the detected `Technology` fields (vendor, product, version, language, subtype, source) are emitted.
- Fail-soft: missing metrics-server, Tetragon, or any optional API does not break the plugin block. The block is always present when the feature is enabled, even if some signal arrays are empty.
- The plugin is opt-in and disabled by default.

## Technology Detection Rules

Detection runs in this order, first match wins:

1. **Labels / annotations** (`Technology.source = "labels"`):
   - `app.kubernetes.io/component=angular` (or `=spring-boot`, `=oracle`).
   - Annotation `angular.io/version` (Angular).
   - Annotation `app.spring.io/version` (Spring Boot).
2. **Image name patterns** (`Technology.source = "image"`):
   - Spring Boot: image name matches `springboot`, `spring-boot`, prefix `*-springboot` or `*-spring-boot`.
   - Oracle: image name matches `oracle/database`, prefix `gvenzl/oracle-`, or any image from `container-registry.oracle.com/database/`.
   - Angular: image name matches prefix `angular-` or `*-ng-`.
3. **Container command/args patterns** (`Technology.source = "process"`):
   - Spring Boot: `command`/`args` contains `spring` in a `.jar` reference or `-Dspring.profiles.active=`.
   - Oracle: `command`/`args` contains `oracle` or env-style `ORACLE_SID=`.
4. **Explicit plugin config** (`Technology.source = "config"`): reserved for the future Hub-pushed config path.

### `Technology` extension (additive, non-breaking)

Two new fields are added to `Technology`:

- `subtype: Option<String>` — used to tag the application stack when the runtime product is generic. Example: `product=nginx, subtype=angular` means "this nginx is serving an Angular app." Null for everything else.
- `source` extends from `"image"|"process"` to `"image"|"process"|"labels"|"config"`. Existing values are unchanged; new values are additive.

`Technology` keeps its existing fields (`vendor`, `product`, `version`, `language`) and their semantics.

The extension is wire-visible and must be coordinated with the Hub team before `SEN-318` is implemented. Since the new field is `Option` and the new enum values are additive, existing Hub parsers keep working.

## Demo Target and Initial Validation

- First supported target: k3s in the team's current environment.
- Demo manifests / sample workloads are tracked in `SEN-322`; this ADR only fixes the contract.
- For the demo, dependency collection (Tetragon) is opt-in. The plugin block is useful even without Tetragon — only `signals.dependencies` becomes `null`.

## Known Limitations

- No multi-process container detection. The plugin reads the entrypoint `command`/`args` only; sidecar processes are not detected.
- No env var scraping (security constraint). Workloads that hide their stack behind env-driven config will fall through to image detection.
- Angular detection depends on labels, annotations, or naming convention. There is no static-asset fingerprinting.
- Labels/annotations are not validated. Missing or stale labels degrade to image-only detection.
- Plugin output may be empty if no workloads match the allowlist. The Hub must handle empty `signals` gracefully.
- `Technology.source = "config"` is reserved and unused in v1; the slot is allocated to keep the enum stable for the follow-up work.

## References

- Linear: `SEN-317` (parent), `SEN-318` (agent impl), `SEN-319` (hub-engine), `SEN-320` (AI), `SEN-321` (UI), `SEN-322` (demo)
- Follow-up issues filed in the Sentinella Hub project: Hub-pushed plugin config, snapshot ingest envelope, plugin config command kind
- Repos: `hub-kubernetes-agent` (this repo), `hub-engine`, `hub-ui`
