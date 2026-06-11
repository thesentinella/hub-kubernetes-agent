# Sentinella Hub Kubernetes Agent Spec

## Runtime Model

- The agent runs as a Kubernetes `DaemonSet` using the `sentinella-hub-k8s-agent` `ServiceAccount` in namespace `sentinella`.
- Every pod starts health/metrics, leader election, inventory collection, and command polling.
- Inventory collection is leader-only. Non-leader pods skip collection but still poll commands.
- Leader election uses a namespace-scoped `coordination.k8s.io/Lease` named by `LEASE_NAME`, default `sentinella-hub-k8s-agent-leader`.
- Lease holder identity is `NODE_NAME`, not pod name.
- Lease election is non-fencing; duplicate inventory snapshots are possible during clock skew or partitions and must be tolerated by the Hub.

## Kubernetes Permissions

- The deployed reader `ClusterRole` is read-only and grants `get`, `list`, `watch`.
- Core resources covered by the reader role: nodes, namespaces, pods, services, configmaps, persistentvolumeclaims, persistentvolumes, events.
- Secret collection requires separate read RBAC on `secrets` (`get`, `list`, `watch`) and is gated by `COLLECT_SECRETS=true`.
- Apps resources covered by the reader role: deployments, statefulsets, daemonsets, replicasets.
- Batch resources covered by the reader role: jobs, cronjobs.
- Networking resources covered by the reader role: ingresses, networkpolicies.
- Storage resources covered by the reader role: storageclasses, persistentvolumes, persistentvolumeclaims.
- Snapshot resources covered by the reader role: volumesnapshotclasses, volumesnapshots.
- Lease permissions are namespace-scoped in a `Role`: `get`, `create`, `update`, `patch` on `coordination.k8s.io/leases`.
- Action RBAC, when enabled, must be separate from the reader role and narrowly scoped to the needed mutating verbs/resources.

## Configuration

| Variable | Required | Default | Notes |
|---|---:|---|---|
| `HUB_URL` | yes | none | Trailing slashes are stripped. |
| `CLUSTER_ID` | yes | none | Sent in every Hub URL and snapshot body. |
| `HUB_API_KEY` | no | none | Used as bearer auth when present; expected to start with `shub_`. |
| `AGENT_VERSION_OVERRIDE` | no | none | Overrides reported `agent.version` when non-empty (max 128 chars). |
| `COLLECT_INTERVAL_SECS` | no | `60` | Leader inventory interval. |
| `POLL_WAIT_SECS` | no | `30` | Server-side command long-poll wait. |
| `HTTP_TIMEOUT_SECS` | no | `20` | Base HTTP client timeout. |
| `AGENT_HTTP_DEBUG` | no | `false` | Enables structured Hub HTTP request/response debug logs. |
| `AGENT_HTTP_DEBUG_BODIES` | no | `false` | Enables bounded (`200` chars) HTTP response body previews and POST request body previews in debug/error logs. |
| `LEASE_TTL_SECS` | no | `30` | Lease validity window; renew interval is `ttl / 3`. |
| `LEASE_NAME` | no | `sentinella-hub-k8s-agent-leader` | Lease object name. |
| `ACTIONS_ENABLED` | no | `false` | Only `true` or `1` enables action dispatch. |
| `COLLECT_SECRETS` | no | `false` | When `true`, collect Secret metadata and key names only; requires separate `secrets` read RBAC. |
| `COLLECT_DEPENDENCIES_TETRAGON` | no | `false` | When `true`, collect dependency edges from Tetragon gRPC. |
| `TETRAGON_GRPC_ADDRESS` | no | when `COLLECT_DEPENDENCIES_TETRAGON=true`, `tetragon-grpc.tetragon.svc.cluster.local:54321` | Tetragon gRPC server address used for dependency collection. |
| `AGENT_LOG` | no | `info` | Primary log filter variable for JSON tracing output. |
| `RUST_LOG` | no | none | Optional legacy alias if `AGENT_LOG` is not set. |
| `POD_NAME` | no | `unknown` | Usually set by downward API. |
| `POD_NAMESPACE` | no | `default` | Usually set by downward API. |
| `NODE_NAME` | no | `unknown-node` | Usually set by downward API; used for lease holder identity. |

## Agent Endpoints

- `:9090/livez` returns liveness status for kubelet probes.
- `:9090/readyz` returns readiness status for kubelet probes.
- `:9090/metrics` exposes Prometheus metrics.
- Metrics include `agent_snapshots_total{outcome="ok|error|skipped_not_leader"}`.
- Metrics include `agent_commands_received_total`.
- Metrics include `agent_commands_executed_total{status="ok|error|skipped|not_implemented|unknown"}`.
- Metrics include `agent_is_leader`, set to `1` when the pod currently believes it is leader and `0` otherwise.

## Hub API Contract

Path parameter note:

- `{cluster_id}` is the configured `CLUSTER_ID` value used by this agent at runtime.

### Endpoint Comparison (Current vs Target)

| Purpose | Current agent endpoint (implemented) | Target backend endpoint (migration) | Observed status on `api.hub.sentinel.la` | Backend action needed |
|---|---|---|---|---|
| Inventory ingest | `POST /v1/clusters/{cluster_id}/inventory` | `POST /api/v1/agent/ingest` | Current path returns `404`; target path exists (`422` on invalid payload) | Keep target ingest endpoint and confirm payload/schema compatibility |
| Cluster status | `GET /v1/clusters/{cluster_id}` | `GET /api/v1/clusters/{cluster_id}` | Expected JSON status for startup duplicate-check | Ensure endpoint returns `last_seen_at` and optional `k8s_uid` |
| Command poll | `GET /v1/clusters/{cluster_id}/commands/poll?wait={POLL_WAIT_SECS}s` | `GET /api/v1/clusters/{cluster_id}/commands/poll?wait={POLL_WAIT_SECS}s` | Current path returns `404`; target path responds `401` with the tested key | Ensure route is active and API key scope is valid for poll |
| Command ack | `POST /v1/clusters/{cluster_id}/commands/{command_id}/ack` | `POST /api/v1/clusters/{cluster_id}/commands/{command_id}/ack` | Current path returns `404`; target path returns `404` | Implement/enable ack route under target contract |

### Current Agent Behavior (Implemented in this version)

- The Hub client supports route compatibility across legacy (`/v1/...`) and API (`/api/v1/...`) families.
- For inventory, poll, and ack calls, a `404` on the primary route triggers one fallback attempt to the alternate route family.

### Inventory

- Endpoint: `POST /v1/clusters/{cluster_id}/inventory`.
- Body: `InventorySnapshot` JSON.
- Success: any `2xx` response.
- Success responses may include `{ "already_existed": true|false }`; when `true`, agent logs a duplicate registration warning.
- Retry behavior: network errors, `5xx`, `408`, and `429` are retried with backoffs `0s`, `2s`, `5s`.
- Non-retry behavior: other `4xx` responses fail the send immediately.
- Auth: bearer token from `HUB_API_KEY` when configured.

### Startup Duplicate Check

- Endpoint: `GET /v1/clusters/{cluster_id}`.
- Called once during startup before the first inventory loop.
- If `last_seen_at` indicates activity in the last 5 minutes, agent logs a warning about possible duplicate installation.
- Fail-soft: endpoint errors or parse issues do not block agent startup.

### Command Polling

- Endpoint: `GET /v1/clusters/{cluster_id}/commands/poll?wait={POLL_WAIT_SECS}s`.
- The request is long-polling; the Hub may hold it until commands are available or the wait expires.
- The agent sets per-call timeout to `POLL_WAIT_SECS + 10s`.
- `200` with `{"commands":[...]}` returns work.
- `204` returns no work.
- A request timeout is treated as no work.
- `200` with an empty or omitted `commands` list returns no work.
- `200` with an empty body is treated as no work.

### Command Ack

- Endpoint: `POST /v1/clusters/{cluster_id}/commands/{command_id}/ack`.
- Body: `CommandResult` JSON.
- Non-`2xx` ack responses are logged but do not make `ack_command` return an error.
- Auth: bearer token from `HUB_API_KEY` when configured.

### Target Backend Contract (Migration)

- Preferred API base for backend routes: `https://api.hub.sentinel.la`.
- Target paths use `/api/v1/...`.
- API responses for agent routes should always be JSON (no HTML bodies).

#### Inventory Ingest

- Target endpoint: `POST /api/v1/agent/ingest`.
- Body should carry the same inventory snapshot semantics currently sent by the agent.
- Success should be a `2xx` JSON response.

#### Command Polling

- Target endpoint: `GET /api/v1/clusters/{cluster_id}/commands/poll?wait={POLL_WAIT_SECS}s`.
- Success response should be JSON, preferably `{"commands":[...]}` or `{"commands":[]}`.
- If `204` is used for no-work, the agent implementation must explicitly support it.

#### Command Ack

- Target endpoint: `POST /api/v1/clusters/{cluster_id}/commands/{command_id}/ack`.
- Body should be `CommandResult` JSON.
- Non-`2xx` behavior should be deterministic and documented for retries/idempotency.

## Inventory Snapshot Schema

`InventorySnapshot` fields:

- `schema_version`: integer, currently `1`.
- `agent`: `AgentInfo`.
- `cluster_id`: string.
- `timestamp_ms`: Unix epoch milliseconds.
- `k8s_uid`: optional string from `kube-system` namespace UID.
- `cluster`: `ClusterInfo`.
- `namespaces`: array of `NamespaceInfo`.
- `workloads`: `Workloads`.
- `pods`: array of `PodInfo`.
- `network`: `NetworkInventory`.
- `dependencies`: `DependencyInventory`.
- `configuration`: `ConfigurationInventory`.
- `storage`: `StorageInventory`.
- `events`: array of `EventInfo`.

`AgentInfo` fields:

- `name`: static agent name.
- `version`: value from `AGENT_VERSION_OVERRIDE` when set to a non-empty string; otherwise package version from `Cargo.toml`.
- `pod_name`: pod name.
- `pod_namespace`: pod namespace.
- `node_name`: node name.
- `actions_enabled`: boolean indicating whether this agent instance is currently allowed to execute actions.
- `collect_dependencies_tetragon`: boolean indicating whether this agent instance currently has Tetragon dependency collection enabled.

`ClusterInfo` fields:

- `kubernetes_version`: optional string from apiserver version.
- `platform`: optional string; detected values include `eks`, `gke`, `aks`, `openshift`, and `vanilla`.
- `node_count`: number.
- `nodes`: array of `NodeInfo`.

`NodeInfo` fields:

- `name`, `kubelet_version`, `os_image`, `container_runtime`, `architecture`.
- `capacity_cpu`, `capacity_memory`, `allocatable_cpu`, `allocatable_memory`.
- `ready`: boolean.
- `roles`: array from `node-role.kubernetes.io/*` labels.

`NamespaceInfo` fields:

- `name`: string.
- `phase`: optional string.
- `labels`: array of `{ "key": string, "value": string }`.

`Workloads` fields:

- `deployments`: array of `WorkloadRef`.
- `statefulsets`: array of `WorkloadRef`.
- `daemonsets`: array of `WorkloadRef`.

`NetworkInventory` fields:

- `services`: array of `ServiceInfo`.
- `ingresses`: array of `IngressInfo`.

`ServiceInfo` fields:

- `namespace`: string.
- `name`: string.
- `type`: string.
- `cluster_ip`: optional string.
- `external_ips`: array of strings.
- `selector`: array of `{ "key": string, "value": string }`.
- `ports`: array of `ServicePortInfo`.
- `load_balancer_ingress`: array of strings (hostname/ip hints).

`ServicePortInfo` fields:

- `name`: optional string.
- `protocol`: optional string.
- `port`: integer.
- `target_port`: optional string (numeric or named target port).
- `node_port`: optional integer.

`IngressInfo` fields:

- `namespace`: string.
- `name`: string.
- `class_name`: optional string.
- `hosts`: array of strings.
- `rules`: array of `IngressRuleInfo`.
- `tls`: array of `IngressTlsInfo`.
- `load_balancer_ingress`: array of strings (hostname/ip hints).

`IngressRuleInfo` fields:

- `host`: optional string.
- `path`: optional string.
- `path_type`: optional string.
- `backend_service`: optional string.
- `backend_port`: optional string.

`IngressTlsInfo` fields:

- `hosts`: array of strings.
- `secret_name`: optional string.

`WorkloadRef` fields:

- `namespace`: string.
- `name`: string.
- `replicas_desired`: optional integer.
- `replicas_ready`: optional integer.

`PodInfo` fields:

- `namespace`, `name`.
- `age_seconds`: optional integer; pod age in seconds at collection time (`0` if clock skew makes creation time appear in the future).
- `node`: optional string.
- `phase`: optional string.
- `owner_kind`: optional string.
- `owner_name`: optional string.
- `containers`: array of `ContainerInfo`.

Compatibility note:

- Older agent versions may omit `age_seconds`; Hub should treat missing values as unknown.

`ContainerInfo` fields:

- `name`: string.
- `image`: string.
- `image_pull_policy`: optional string.
- `technology`: `Technology`.
- `resources`: `ResourceSpec`.

`ResourceSpec` fields:

- `requests_cpu`, `requests_memory`, `limits_cpu`, `limits_memory`: optional Kubernetes quantity strings.

`Technology` fields:

- `vendor`: optional string.
- `product`: optional string.
- `version`: optional string.
- `language`: optional string.
- `source`: currently `image`.

`StorageInventory` fields:

- `storage_classes`: array of `StorageClassInfo`.
- `persistent_volumes`: array of `PersistentVolumeInfo`.
- `persistent_volume_claims`: array of `PersistentVolumeClaimInfo`.
- `volume_snapshot_classes`: array of `VolumeSnapshotClassInfo`.
- `volume_snapshots`: array of `VolumeSnapshotInfo`.

`StorageClassInfo` fields:

- `name`: string.
- `provisioner`: string.
- `parameters`: array of `{ "key": string, "value": string }`, filtered by a hardcoded safe allowlist in the agent.

`PersistentVolumeInfo` fields:

- `name`: string.
- `storage_class`: optional string.

`PersistentVolumeClaimInfo` fields:

- `namespace`: string.
- `name`: string.
- `storage_class`: optional string.
- `volume_name`: optional string.

`VolumeSnapshotClassInfo` fields:

- `name`: string.
- `driver`: string.

`VolumeSnapshotInfo` fields:

- `namespace`: string.
- `name`: string.
- `snapshot_class`: optional string.
- `bound_content_name`: optional string.

`EventInfo` fields:

- `namespace`: string.
- `name`: string.
- `type`: optional string (`Warning` or `Normal` in current collection behavior).
- `reason`: optional string.
- `message`: optional string, truncated to 500 chars.
- `count`: optional integer.
- `first_timestamp`: optional RFC3339 string.
- `last_timestamp`: optional RFC3339 string.
- `reporting_controller`: optional string.
- `reporting_instance`: optional string.
- `involved_object`: `InvolvedObjectInfo`.

`InvolvedObjectInfo` fields:

- `kind`: optional string.
- `name`: optional string.
- `namespace`: optional string.
- `uid`: optional string.

## Inventory Collection Behavior

- The collector lists nodes, namespaces, deployments, statefulsets, daemonsets, pods, services, ingresses, storage resources, events, snapshot resources, and apiserver version concurrently.
- Individual Kubernetes list failures are fail-soft: the failed resource list becomes empty and a warning is logged.
- Event payload is bounded: keep up to 500 events per snapshot; each event message is truncated to 500 chars.
- Event ordering for snapshots is deterministic: `Warning` first, then newest-first timestamp, then namespace/name tie-breaker.
- Storage backend mapping (`backend_slug`) is intentionally server-side; the agent sends raw StorageClass signals (`provisioner` plus safe parameter subset) and the Hub maps slugs/icons.
- Platform detection checks apiserver version for `eks`, `gke`, or `aks`, then OpenShift node labels with prefix `node.openshift.io/`, then falls back to `vanilla`.
- Technology detection is image-based and table-driven in `src/tech.rs`.
- Process-level/runtime technology inspection is out of scope for this release and tracked as a separate follow-up.
- Unknown images are still reported with `vendor: null`, `product: <image-name>`, `version: <tag>`, `source: "image"`.
- Dependency collection from Tetragon gRPC is opt-in (`COLLECT_DEPENDENCIES_TETRAGON=true`) and fail-soft.
- When dependency collection is enabled, the agent readiness probe blocks the pod from becoming Ready until it has connected to Tetragon.
- Dependency output is bounded by internal caps (max edges and max fanout per source). Truncation sets `dependencies.truncated=true` and increments `dependencies.dropped_edges`.
- Unknown endpoint mappings are included as `kind: "unknown"` edges and still include `ip` when known.
- Dependency source is `tetragon_grpc`; the agent consumes Tetragon gRPC directly and manages its own node-local tracing policy.

## Command Schema

`CommandBatch` fields:

- `commands`: array of `Command`, defaulting to empty when omitted.

`Command` fields:

- `id`: command identifier string.
- `type`: command kind string, deserialized as `kind` in agent code.
- `spec`: command-specific JSON object, defaulting to `{}` when omitted.

Known command kinds:

- `preview_workload_resources`.
- `apply_workload_resources`.
- `self_update`.
- `update_agent`.

`WorkloadResourcesSpec` fields for known resource commands:

- `workload_kind`: `Deployment`, `StatefulSet`, or `DaemonSet`.
- `namespace`: workload namespace.
- `name`: workload name.
- `container`: target container name in the workload pod template.
- `requests`: optional resource map.
- `limits`: optional resource map.
- At least one of `requests` or `limits` must be present. An omitted side is left untouched; an empty map clears that side.

`ResourceMap` fields:

- `cpu`: optional Kubernetes quantity string.
- `memory`: optional Kubernetes quantity string.

`SelfUpdateSpec` fields:

- `target_version`: optional string.
- `reason`: optional string.
- `strategy`: optional string, default behavior is `restart_pod`.

`UpdateAgentSpec` fields:

- `image`: required string. Must start with `us-east1-docker.pkg.dev/sentinella-hub/kubernetes-agent/` and include either a tag (`:<tag>`) or digest (`@sha256:<64 hex>`).

`CommandResult` fields:

- `command_id`: string.
- `status`: one of `ok`, `error`, `skipped`, `not_implemented`, `unknown`.
- `message`: optional string.
- `finished_at_ms`: Unix epoch milliseconds.
- `dry_run`: optional boolean.
- `applied_patch`: optional JSON value.
- `observed_before`: optional JSON value.
- `observed_after`: optional JSON value.
- `warnings`: array of strings, omitted when empty.
- `restart_requested`: optional boolean; when true on a successful command, the runtime exits after ack attempt to trigger pod restart.

## Action Execution Contract

- `ACTIONS_ENABLED=false` is read-only mode and is the default.
- In read-only mode the executor returns `status: "skipped"` before parsing `spec`.
- Unknown command kinds return `status: "unknown"` when actions are enabled.
- Recognized command kinds are `preview_workload_resources`, `apply_workload_resources`, `self_update`, and `update_agent`.
- Resource commands target workload controllers, not Pods.
- Resource patch implementation must use strategic-merge semantics for `spec.template.spec.containers[name=<container>].resources`; JSON merge would clobber the whole `containers` array.
- `preview_workload_resources` performs a Kubernetes strategic-merge dry-run patch with `dryRun=All` for `Deployment`, `StatefulSet`, and `DaemonSet` when actions are enabled.
- Preview pre-flight checks are best-effort and non-fatal; check errors become warnings and do not fail the preview by themselves.
- Implemented pre-flight warning code prefixes are: `preflight.hpa.targeted`, `preflight.vpa.auto_mode`, `preflight.limitrange.present`, `preflight.resourcequota.present`, `preflight.pdb.selector_overlap`, and `preflight.check.unavailable`.
- If the agent lacks read permissions (or the VPA CRD is absent), checks return `preflight.check.unavailable` warnings and preview execution continues.
- Successful previews return `status: "ok"`, `dry_run: true`, `applied_patch`, `observed_before`, `observed_after`, and `warnings`.
- Failed previews return `status: "error"` and `dry_run: true`.
- `apply_workload_resources` performs a Kubernetes strategic-merge patch for `Deployment`, `StatefulSet`, and `DaemonSet` when actions are enabled.
- Successful apply responses return `status: "ok"`, `dry_run: false`, `applied_patch`, `observed_before`, `observed_after`, and `warnings`.
- Failed apply responses return `status: "error"` and `dry_run: false`.
- `self_update` returns `status: "ok"` and requests an immediate process exit so Kubernetes can recreate the pod.
- For `self_update`, the agent attempts ack first, then exits even if ack fails.
- `self_update` does not mutate image tags or deployment manifests; rollout/version control remains external to the agent.
- `update_agent` validates the requested image reference and returns `status: "error"` with an explanatory message when validation fails.
- `update_agent` targets only namespace `sentinella`, DaemonSet `sentinella-hub-k8s-agent`, container `agent`.
- `update_agent` performs a strategic-merge dry-run patch before live apply; dry-run failure returns `status: "error"`.
- `update_agent` uses strategic-merge patch semantics to set only the named container image.
- `update_agent` returns `status: "ok"`, `dry_run: false`, `applied_patch`, `observed_before`, `observed_after`, and warning strings when patching succeeds.
- `update_agent` allows tagged images including `:latest` and allows sha256 digest references in the allowed registry prefix.

## Deployment Manifest

- The deploy manifest is root `agent.yaml`.
- The `agent` container image is `us-east1-docker.pkg.dev/sentinella-hub/kubernetes-agent/sentinella-hub-k8s-agent:<tag>`.
- `agent.yaml` stores runtime config in ConfigMap `sentinella-hub-k8s-agent-config` and auth in Secret `sentinella-hub-k8s-agent-auth` key `api-key`.
- The DaemonSet injects `HUB_API_KEY` from Secret key `api-key`, optionally.
- The pod runs as non-root UID/GID `65532`, with `readOnlyRootFilesystem: true`, no privilege escalation, all capabilities dropped, and `RuntimeDefault` seccomp.
- The DaemonSet tolerates all `NoSchedule` and `NoExecute` taints, so it schedules on control-plane and tainted nodes by default.
`ConfigurationInventory` fields:

- `configmaps`: array of `ConfigMapInfo`.
- `secrets`: array of `SecretInfo`.
- `agent_runtime_env`: array of `KV` entries representing the running agent's applied non-secret config values.
- `agent_configured_env`: array of `KV` entries representing allowlisted non-secret values from `sentinella-hub-k8s-agent-config`.

`agent_runtime_env` and `agent_configured_env` are intentionally limited to the agent config allowlist:

- `HUB_URL`
- `CLUSTER_ID`
- `COLLECT_INTERVAL_SECS`
- `POLL_WAIT_SECS`
- `HTTP_TIMEOUT_SECS`
- `LEASE_TTL_SECS`
- `ACTIONS_ENABLED`
- `COLLECT_SECRETS`
- `COLLECT_DEPENDENCIES_TETRAGON`
- `TETRAGON_GRPC_ADDRESS`
- `FULL_DEBUG`
- `AGENT_HTTP_DEBUG`
- `AGENT_HTTP_DEBUG_BODIES`
- `AGENT_LOG`
- `AGENT_VERSION_OVERRIDE`
- `LEASE_NAME`

Excluded keys include `HUB_API_KEY`, `POD_NAME`, `POD_NAMESPACE`, and `NODE_NAME`. This is a special-case agent config drift view, not a generic ConfigMap value export.

`ConfigMapInfo` fields:

- `namespace`: string.
- `name`: string.
- `immutable`: optional boolean.
- `labels`: array of key/value pairs.
- `annotations`: array of key/value pairs (v1 includes all annotations).
- `data_keys`: array of strings (keys from `data`).
- `binary_data_keys`: array of strings (keys from `binaryData`).

`SecretInfo` fields:

- `namespace`: string.
- `name`: string.
- `type`: optional string.
- `immutable`: optional boolean.
- `labels`: array of key/value pairs.
- `annotations`: array of key/value pairs (v1 includes all annotations).
- `data_keys`: array of strings (keys from `data`).

Secret/config values are intentionally excluded from the snapshot payload.
## Dependency Edge Payload (EBP)

EBP is reported inside `InventorySnapshot.dependencies` and is opt-in via
`COLLECT_DEPENDENCIES_TETRAGON=true`.

- Source is `tetragon_grpc` from `TETRAGON_GRPC_ADDRESS`.
- Collection is fail-soft: if Tetragon gRPC is unavailable, snapshots
  still succeed and `dependencies.edges` is empty.

### DependencyInventory schema

| Field | Type | Description |
|---|---|---|
| `edges` | `DependencyEdge[]` | Aggregated dependency edges for the current collection window. |
| `source` | `string` | Fixed value: `tetragon_grpc`. |
| `window_seconds` | `u64` | Aggregation window length in seconds. Phase-1 value is `60`. |
| `truncated` | `bool` | `true` when internal edge/fanout caps dropped data in this snapshot. |
| `dropped_edges` | `u64` | Number of edges dropped due to cap enforcement. |

### DependencyEdge schema

| Field | Type | Description |
|---|---|---|
| `from` | `DependencyEndpoint` | Source endpoint identity. |
| `to` | `DependencyEndpoint` | Destination endpoint identity. |
| `protocol` | `string` | Upper-cased protocol (`TCP`, `UDP`, `SCTP`, `UNKNOWN`). |
| `destination_port` | `u16` | Destination port. Defaults to `0` when missing in source event. |
| `direction` | `string` | Phase-1 fixed value: `egress`. |
| `bytes` | `u64` | Aggregated bytes over matching events. |
| `packets` | `u64` | Aggregated packets over matching events. |
| `connections` | `u64` | Aggregated connections over matching events. Defaults to `1` per event when absent. |
| `first_seen_unix_ms` | `u128` | Earliest timestamp in aggregate (Unix epoch milliseconds). |
| `last_seen_unix_ms` | `u128` | Latest timestamp in aggregate (Unix epoch milliseconds). |

### DependencyEndpoint schema

| Field | Type | Description |
|---|---|---|
| `kind` | `string` | `pod`, `service`, or `unknown`. |
| `namespace` | `string \| null` | Namespace for pod/service endpoints; `null` for unknown endpoints. |
| `name` | `string \| null` | Pod name or service name; `null` for unknown endpoints. |
| `workload_kind` | `string \| null` | Owner kind for pod endpoints (`Deployment`, `StatefulSet`, `DaemonSet`, etc.). |
| `workload_name` | `string \| null` | Owner name for pod endpoints. |
| `ip` | `string \| null` | Endpoint IP when known. Included for unknown endpoints when source IP is present. |

### Behavioral contract

- **Edge identity** is workload-aware and deterministic. The deduplication key is:
  `from(identity) + to(identity) + protocol + destination_port + direction`.
- **Identity normalization for keying** uses lowercase for
  `kind/namespace/name/workload_kind/workload_name`; IP is kept as-is.
- **Aggregation** for matching keys:
  - `bytes`, `packets`, `connections` are summed with saturating arithmetic.
  - `first_seen_unix_ms` keeps the minimum timestamp.
  - `last_seen_unix_ms` keeps the maximum timestamp.
- **Sorting** is deterministic by normalized `from`, then normalized `to`, then
  upper-cased `protocol`, then `destination_port`, then lower-cased `direction`.
- **Direction** is currently always `egress` in phase-1; field stays explicit for
  forward compatibility.

### Capacity and truncation rules

| Limit | Value | Behavior |
|---|---:|---|
| `MAX_TETRAGON_LINES` | `20_000` | Max normalized Tetragon event records buffered per collection cycle. |
| `MAX_DEP_EDGES_PER_SNAPSHOT` | `2_000` | Max unique edge keys in `edges`. New unique keys are dropped when full. |
| `MAX_DEP_FANOUT_PER_SOURCE` | `200` | Max distinct targets per source endpoint. New source->target pairs are dropped after this cap. |

When any cap drops data:

- `dependencies.truncated = true`
- `dependencies.dropped_edges > 0`

### Endpoint resolution rules

Resolution is best-effort and evaluated in this order:

1. Pod IP index (`pod.status.podIP`) -> `kind="pod"` + workload owner metadata.
2. Service cluster IP index (`service.spec.clusterIP`) -> `kind="service"`.
3. No match -> `kind="unknown"` with raw `ip` when present.

Unknown edges are intentionally preserved so the backend can surface external or
unresolved traffic.

### Tetragon event contract

The agent normalizes observed Tetragon gRPC `ProcessKprobe` messages into an internal event envelope
compatible with the dependency parser and attempts
the following pointer fallbacks for each field:

| Output field | JSON pointers (first match wins) | Default |
|---|---|---|
| `src_ip` | `/src_ip`, `/flow/src_ip`, `/flow/ip/source` | Required; line skipped if absent |
| `dst_ip` | `/dst_ip`, `/flow/dst_ip`, `/flow/ip/destination` | Required; line skipped if absent |
| `protocol` | `/protocol`, `/flow/protocol` | `UNKNOWN` |
| `destination_port` | `/destination_port`, `/dst_port`, `/flow/dst_port`, `/flow/l4/dst_port` | `0` |
| `bytes` | `/bytes`, `/flow/bytes`, `/summary/bytes` | `0` |
| `packets` | `/packets`, `/flow/packets`, `/summary/packets` | `0` |
| `connections` | `/connections`, `/flow/connections`, `/summary/connections` | `1` |
| `timestamp_unix_ms` | `/timestamp_unix_ms`, `/time_unix_ms`, `/flow/timestamp_unix_ms` | Current time (`now`) |

Additional normalized endpoint hint fields may be present:

| Output field | JSON pointer | Description |
|---|---|---|
Invalid JSON lines are skipped (warn-level log), preserving fail-soft behavior.

### JSON examples

#### Example A: populated payload

```json
{
  "dependencies": {
    "edges": [
      {
        "from": {
          "kind": "pod",
          "namespace": "production",
          "name": "api-server-7d9b8c6f4-x2kjp",
          "workload_kind": "Deployment",
          "workload_name": "api-server",
          "ip": "10.244.1.15"
        },
        "to": {
          "kind": "service",
          "namespace": "production",
          "name": "postgres-primary",
          "workload_kind": null,
          "workload_name": null,
          "ip": "10.96.42.10"
        },
        "protocol": "TCP",
        "destination_port": 5432,
        "direction": "egress",
        "bytes": 1048576,
        "packets": 8923,
        "connections": 34,
        "first_seen_unix_ms": 1748523600000,
        "last_seen_unix_ms": 1748524260000
      },
      {
        "from": {
          "kind": "pod",
          "namespace": "production",
          "name": "api-server-7d9b8c6f4-x2kjp",
          "workload_kind": "Deployment",
          "workload_name": "api-server",
          "ip": "10.244.1.15"
        },
        "to": {
          "kind": "unknown",
          "namespace": null,
          "name": null,
          "workload_kind": null,
          "workload_name": null,
          "ip": "203.0.113.50"
        },
        "protocol": "TCP",
        "destination_port": 443,
        "direction": "egress",
        "bytes": 524288,
        "packets": 4210,
        "connections": 12,
        "first_seen_unix_ms": 1748523660000,
        "last_seen_unix_ms": 1748524260000
      }
    ],
    "source": "tetragon_grpc",
    "window_seconds": 60,
    "truncated": false,
    "dropped_edges": 0
  }
}
```

#### Example B: disabled or source unavailable

```json
{
  "dependencies": {
    "edges": [],
    "source": "tetragon_grpc",
    "window_seconds": 60,
    "truncated": false,
    "dropped_edges": 0
  }
}
```

#### Example C: truncated payload

```json
{
  "dependencies": {
    "edges": [
      {
        "from": { "kind": "pod", "namespace": "ns-a", "name": "pod-a", "workload_kind": "Deployment", "workload_name": "api", "ip": "10.244.1.10" },
        "to": { "kind": "unknown", "namespace": null, "name": null, "workload_kind": null, "workload_name": null, "ip": "198.51.100.30" },
        "protocol": "TCP",
        "destination_port": 443,
        "direction": "egress",
        "bytes": 1024,
        "packets": 12,
        "connections": 1,
        "first_seen_unix_ms": 1748523601000,
        "last_seen_unix_ms": 1748523605000
      }
    ],
    "source": "tetragon_grpc",
    "window_seconds": 60,
    "truncated": true,
    "dropped_edges": 487
  }
}
```
