# Sentinella Hub Kubernetes Agent

Inventory collector and (future) action executor for Kubernetes and OpenShift clusters, part of the **Sentinella Hub** ecosystem. Designed to live alongside future sibling agents (VMware Agent, OpenStack Agent, etc.) under the same Hub.

## What it does in this release

- Connects to the Kubernetes API using its `ServiceAccount` (cluster-wide read-only RBAC).
- Runs as a **DaemonSet** with **leader election** via a `coordination.k8s.io/Lease`. Only the leader collects and ships inventory; non-leaders idle on that loop. All pods poll for commands.
- Every minute the leader collects and sends to the Hub an inventory **snapshot**:
  - **Cluster**: Kubernetes version, detected platform (vanilla / openshift / eks / gke / aks), OpenShift version when present, nodes with capacity/allocatable, kubelet version, container runtime, OS image, roles.
  - **Namespaces** with labels and phase.
  - **Workloads**: deployments, statefulsets, daemonsets (name, namespace, desired/ready replicas).
  - **Pods**: age in seconds (`age_seconds`), actual pod `usage_cpu` / `usage_memory` when metrics-server is available, and each container with image, **detected technology** (vendor/product/version inferred from the image, or from `command`/`args` when `TECH_DETECT_PROCESS=true`), `requests` and `limits` (CPU and memory).
  - **Configuration**: ConfigMaps metadata by default, and optional Secrets metadata when `COLLECT_SECRETS=true` (name/namespace/type/immutability/labels/annotations) plus key names only (`data_keys`, `binary_data_keys` where applicable), never raw values.
  - **Network**: Services (type, selector, ports, exposure metadata) and Ingresses (class, hosts/paths/backends, TLS summary, load balancer status hints).
  - **Security**: NetworkPolicy summaries, security-relevant ClusterRoleBinding summaries, and namespace Pod Security Admission label posture.
  - **Operational Maturity**: Descheduler and VPA detection, plus all CronJob summaries for operational-tool visibility.
  - **Dependencies (optional)**: bounded pod/service dependency edges derived from Tetragon gRPC (`source=tetragon_grpc`), including unresolved `unknown` edges when endpoint mapping is unavailable.
  - **Storage**: StorageClasses (name/provisioner/safe parameter subset), PersistentVolumes, PersistentVolumeClaims, VolumeSnapshotClasses, and VolumeSnapshots.
  - **Events**: Kubernetes `Warning` and `Normal` events with bounded payload (max 500 events per snapshot, event message truncated to 500 chars).
- Maintains an open long-poll against the Hub for command delivery. **Action execution is disabled by default** (`ACTIONS_ENABLED=false`); the agent replies with `skipped` and an explanatory message to any command received. When actions are explicitly enabled, the agent can preview workload resource patches with a Kubernetes server-side dry-run.
- Snapshot agent metadata includes whether actions are enabled (`agent.actions_enabled`) and whether Tetragon dependency collection is enabled (`agent.collect_dependencies_tetragon`) so the Hub can accurately reflect agent execution capability.

## Architecture

### DaemonSet with leader election

The agent runs on every node, but only one pod at a time — the leader — collects cluster inventory. This gives you:

- **Resilience**: if the leader's node goes down, another pod takes over within `LEASE_TTL_SECS`.
- **Forward compatibility with node-local work**: when future versions need per-node data (kubelet stats, host filesystem reads, in-cluster sidecar coordination), every pod is already in place — no need to deploy a second workload.
- **Forward compatibility with node-targeted commands**: when actions are enabled, a command like "patch this pod's resources" can be routed to the agent on the node where the target pod lives.

Leader election uses the standard Kubernetes `Lease` resource — no external dependency. The lease lives in the agent's own namespace. Holder identity is the node name, so `kubectl describe lease -n sentinella` immediately tells you which node is leading.

**Note on fencing**: Lease-based election is non-fencing. Under heavy clock skew or network partitions, two pods could briefly both believe they hold the lease. For inventory collection this only causes a duplicate snapshot — harmless; the Hub should be idempotent on `(cluster_id, timestamp_ms)`. When action execution is enabled, the executor still tolerates this: every command has an `id`, the Hub deduplicates acks, and operations should be idempotent.

### Hub → Agent channel: long-polling over HTTPS

Considered alternatives:

- **Persistent WebSocket**: cleaner for bidirectional traffic, but corporate load balancers sometimes terminate them. Reconnection adds complexity.
- **gRPC streaming**: same story, worse compatibility with proxies.
- **Short polling (every N seconds)**: high latency for actions, unnecessary pressure on the Hub.
- **Long-polling**: this binary opens `GET /v1/clusters/{cluster_id}/commands/poll?wait=30s`; the Hub holds the connection until a command arrives or the wait expires. ✅

Chosen because:

- Only outbound HTTPS is required — works behind any corporate proxy or firewall (relevant for banking and telco clients).
- The agent never exposes inbound ports.
- Reconnection is trivial (it's just HTTP).

If heavy bidirectional streaming is needed later (live logs, interactive exec), that case is solved with a separate channel without touching this transport.

### Image-based technology detection

`src/tech.rs` holds a rules table (image name → vendor/product) plus light version normalization (`v1.30.2-alpine` → `1.30.2`). Initial coverage:

- **Web/proxy**: nginx, httpd, haproxy, envoy, traefik, caddy
- **Databases**: postgres, mysql, mariadb, mongo, redis, elasticsearch, kibana, influxdb, cockroach
- **Messaging**: rabbitmq, kafka, nats
- **Runtimes**: openjdk/temurin/corretto/semeru, node, python, golang, rust, dotnet, ruby, php
- **Observability**: prometheus, grafana, logstash, fluentd, fluent-bit
- **K8s/OpenShift control plane**: kube-apiserver, controller-manager, scheduler, kube-proxy, coredns, etcd, ose-*
- **Service mesh**: istio-proxy, linkerd-proxy

Unknown images are not discarded — they return `vendor=null`, `product=<image-name>`, `version=<tag>` so the Hub can group and surface frequently seen images for promotion to vendor/product later.

Technology detection in this release is intentionally image-derived only. Process-level/runtime inspection is tracked separately.

Unit tests are included in `tech.rs`. Extending coverage is one entry in the `RULES` table.

### Dependency collection (Tetragon)

Dependency collection is opt-in and disabled by default (`COLLECT_DEPENDENCIES_TETRAGON=false`).

- Source: Tetragon gRPC exposed on an internal Kubernetes service. When dependency collection is enabled, the default address is `tetragon-grpc.tetragon.svc.cluster.local:54321` via `TETRAGON_GRPC_ADDRESS`.
- Output: metadata-only dependency edges (no packet payload capture, no process args/env collection).
- Behavior: bounded, deterministic ordering, and fail-soft when the source is unavailable.
- Readiness: by default the pod stays not-ready until Tetragon connects; set `TETRAGON_REQUIRED_FOR_READINESS=false` to relax readiness for dev or nodes that cannot run Tetragon.
- The direct Rust Tetragon gRPC client auto-loads a node-local `tcp_sendmsg`/`tcp_close` tracing policy so users do not have to apply a policy manually.
- Unknown destinations/sources are included as `kind="unknown"` edges.
- Portability note: this remains optional and disabled by default so clusters without Tetragon are unaffected.
- Deployment note: this feature depends on Tetragon being installed; it does not assume Cilium/Hubble.

Current payload contract:

- The agent publishes dependency data inside `InventorySnapshot.dependencies`.
- `dependencies.source` is always `tetragon_grpc` when this feature is enabled.
- Each edge contains:
  - `from`, `to`: endpoint identity with `kind`, `namespace`, `name`, `workload_kind`, `workload_name`, `ip`
  - `protocol`
  - `destination_port`
  - `direction` (`egress` in the current implementation)
  - `bytes`, `packets`, `connections`
  - `first_seen_unix_ms`, `last_seen_unix_ms`
- Endpoint resolution order is:
  - pod IP
  - service ClusterIP
  - `unknown` with raw IP preserved

Current Tetragon normalization:

- The direct Rust gRPC client consumes `ProcessKprobe` events from Tetragon.
- The current tracing policy watches `tcp_sendmsg` and `tcp_close`.
- Those events are normalized into an internal JSON shape with:
  - `src_ip`
  - `dst_ip`
  - `protocol`
  - `destination_port`
  - `bytes`
  - `packets`
  - `connections`
  - `timestamp_unix_ms`

Example snapshot payload:

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
      }
    ],
    "source": "tetragon_grpc",
    "window_seconds": 60,
    "truncated": false,
    "dropped_edges": 0
  }
}
```

**Storage note:** TimescaleDB is the recommended first choice for server-side topology history and rollups. Move to ClickHouse later if event volume, retention, or analytics complexity grows beyond what a Postgres-based time-series stack handles comfortably.

### Actions — designed for gradual rollout

`src/executor.rs` keeps the agent in read-only mode while `ACTIONS_ENABLED=false`: mutating commands are skipped before parsing the command spec, but `get_resource_yaml` remains available as a read-only fetch. When actions are enabled, `preview_workload_resources` performs a server-side dry-run, `apply_workload_resources` performs a live strategic-merge patch, `drain_node` cordons a node and evicts eligible pods after action-policy approval, `self_update` triggers an immediate agent restart, and `update_agent` updates the fixed agent DaemonSet image.

The binary now supports `--mode agent|operator`. The default is `agent`. The DaemonSet runs `--mode agent`; the separate operator Deployment runs `--mode operator` and is fully independent of Hub connectivity.

For Action Mode, the target namespace must also carry the label `sentinella.io/action-mode=enabled`; the executor rejects workload patch commands when the label is missing or set to another value.

The Phase 3 operator path now runs in a separate Deployment using `--mode operator`. That workload reconciles namespace-scoped `RoleBinding`s and updates policy status for namespaces that are both labeled `sentinella.io/action-mode=enabled` and selected by a matching cluster-scoped `SentinellaHubActionPolicy`.

`HUB_URL`, `CLUSTER_ID`, and `HUB_API_KEY` are required only in `--mode agent`. `--mode operator` is fully Hub-independent.

The operator manifest must include the `sentinellahubactionpolicies/status` subresource permission; without it, status freshness cannot be written and the operator will fail closed.

`SentinellaHubActionPolicy` uses `namespaceSelector` for eligibility and now enforces `allowedActions`, `allowedResources`, and `limits` as part of the action policy gate. Policies are also treated as stale when their freshness timestamp is missing or too old.

#### Commands: setting requests and limits

Two command kinds form the resource-patch flow:

- **`preview_workload_resources`** — implemented server-side dry-run. Computes the patch, runs it against the apiserver with `?dryRun=All`, and returns the would-be state. Cluster state is unchanged.
- **`get_resource_yaml`** — reads an allowlisted Kubernetes object and returns manifest-like YAML with server-managed metadata stripped. Secrets are rejected.
- **`apply_workload_resources`** — live apply. Uses the same strategic-merge patch shape and pre-flight warning checks as preview, but persists the change.

The two-command pattern is intentional. Each artifact (preview, approval, apply) is a separate Hub record with its own `command_id`, timestamp, and audit trail — easier for dashboards, easier for compliance reviews. Cluster state can change between preview and apply (HPA scaled, new pods); the apply re-validates against fresh state rather than relying on a stale preview.

#### Command: self update

- **`self_update`** — accepts a restart signal from Hub and immediately restarts the agent process after ack attempt.
- The agent attempts to ack the result first; restart proceeds even if ack fails.
- The agent does not mutate deployment manifests or image tags; Kubernetes restarts the pod via DaemonSet reconciliation.

#### Command: update agent image

- **`update_agent`** — updates the image of the fixed target `DaemonSet/sentinella-hub-k8s-agent` container `agent` in namespace `sentinella`.
- Accepted image refs must start with `us-east1-docker.pkg.dev/sentinella-hub/kubernetes-agent/` and must include either `:<tag>` or `@sha256:<64-hex-digest>`.
- `:latest` is allowed.
- The agent runs a Kubernetes dry-run patch before live apply and returns pre-flight warnings alongside the result.
- The command fails with `status: "error"` when validation fails or when the DaemonSet/container lookup/patch fails.
- Reader RBAC in `agent.yaml` remains read-only; mutating patch permissions must be granted via a separate action RBAC role/binding when actions are enabled.

**Target is a workload controller**, not a Pod. A `Pod`-targeted patch is futile — its ReplicaSet/StatefulSet recreates it with the original spec in seconds. Supported kinds: `Deployment`, `StatefulSet`, `DaemonSet`. In-place pod resize via the `pods/resize` subresource (Kubernetes 1.33+, beta) is on the v0.3 roadmap.

**Spec shape** (`WorkloadResourcesSpec`):

```json
{
  "workload_kind": "Deployment",
  "namespace": "production",
  "name": "api-server",
  "container": "api",
  "requests": { "cpu": "500m", "memory": "512Mi" },
  "limits":   { "cpu": "1000m", "memory": "1Gi" }
}
```

At least one of `requests` or `limits` must be present. Either side may be omitted to leave it untouched; an empty map clears that side.

**Result shape** (`CommandResult`) for a successful preview includes:

- `applied_patch` — the strategic-merge patch the agent computed.
- `observed_before` — the targeted container's resources block before the operation.
- `observed_after` — what the apiserver returned for the dry-run patch.
- `warnings` — non-fatal safety findings collected during best-effort pre-flight checks. These warnings do not block preview success.

The agent uses **strategic-merge patch**, not JSON-merge — JSON-merge would clobber the entire `containers` array. Strategic-merge addresses just `spec.template.spec.containers[name=X].resources`.

**Pre-flight safety checks** (best-effort — accumulated into `warnings`, non-fatal):
- HPA targeting this workload (CPU/memory autoscaling targets interact with requests).
- VPA in `Auto` or `Recreate` mode (we would fight the VPA).
- Namespace `LimitRange` admits the new values.
- Namespace `ResourceQuota` has headroom for the delta.
- `PodDisruptionBudget` would block the rolling restart.

Warning strings use stable code prefixes for Hub-side grouping:

- `preflight.hpa.targeted`
- `preflight.vpa.auto_mode`
- `preflight.limitrange.present`
- `preflight.resourcequota.present`
- `preflight.pdb.selector_overlap`
- `preflight.check.unavailable`

**RBAC required to enable preview actions**:

```yaml
- apiGroups: ["apps"]
  resources: ["deployments", "statefulsets", "daemonsets"]
  verbs: ["get", "list", "watch", "patch"]
- apiGroups: ["apps"]
  resources: ["deployments/scale", "statefulsets/scale", "daemonsets/scale"]
  verbs: ["get", "patch"]
```

This must be a separate ClusterRole/Binding applied only when `ACTIONS_ENABLED=true`. The read-only ClusterRole stays untouched. **No `*` on `*/*`**. The root `agent.yaml` does not grant this workload patch RBAC by default. Eligible namespaces must also be labeled `sentinella.io/action-mode=enabled`; the agent checks that label before patching.

`drain_node` is a separate apply-only command. It requires `ACTIONS_ENABLED=true`, a Ready `SentinellaHubActionPolicy` that allows `drain_node`, and its own cluster-wide node plus pod-eviction RBAC. The spec supports `timeoutSeconds` (default `300`, max `3600`), optional `gracePeriodSeconds` for pod eviction, and `force` to allow unmanaged pods while still using the eviction API.

Pre-flight warning checks are also best-effort. With the bundled reader RBAC, HPAs, VPAs, LimitRanges, ResourceQuotas, and PDBs are evaluated; preview still succeeds but may include `preflight.check.unavailable` warnings when a checked resource type is absent or unreadable.

Recommendation when enabling: only grant `patch` after the Hub has dashboard approval flow in place. The preview-then-apply pattern is the technical mechanism; the Hub-side approval workflow is what makes it safe for regulated clients.

Storage backend mapping (`backend_slug`, icon selection) is intentionally owned by the Hub. The agent sends raw StorageClass signals (`provisioner` plus a hardcoded safe subset of parameters), and the server maps those to technology slugs.

## Agent endpoints

- `:9090/livez`, `:9090/readyz` — kubelet probes.
- `:9090/metrics` — Prometheus:
  - `agent_snapshots_total{outcome="ok|error|skipped_not_leader"}`
  - `agent_commands_received_total`
  - `agent_commands_executed_total{status="ok|error|skipped|not_implemented|unknown"}`
  - `agent_is_leader` (gauge: 1 if this pod holds the lease, 0 otherwise)

## Hub contract

`{cluster_id}` in paths below is the configured `CLUSTER_ID` value used by the agent at runtime.

### Current agent behavior (implemented in this version)

The agent supports route compatibility by trying both legacy (`/v1/...`) and API (`/api/v1/...`) route families. If a route returns `404`, it automatically retries once against the alternate family for the same operation.

### POST `/v1/clusters/{cluster_id}/inventory`

Body: `InventorySnapshot` (see `src/model.rs`). Respond `2xx` to accept. `4xx` is not retried (except `408`/`429`); `5xx` and network errors are (3 attempts: 0s/2s/5s).

`InventorySnapshot.k8s_uid` is populated from the `kube-system` namespace UID when available and is used by the Hub for duplicate physical cluster detection.

On successful inventory ingest, when the Hub responds with `{"already_existed":true}`, the agent logs a warning indicating potential duplicate re-registration.

`InventorySnapshot.configuration` includes `configmaps` and `secrets` entries. Secret entries are populated only when `COLLECT_SECRETS=true`. Secret and generic ConfigMap payloads include metadata and key names only; values are intentionally excluded.

`InventorySnapshot.configuration.agent_runtime_env` and `InventorySnapshot.configuration.agent_configured_env` are special-case allowlisted views for the agent's own non-secret configuration only. They are used to compare the running agent's applied settings against values present in `sentinella-hub-k8s-agent-config`. They do not expose arbitrary ConfigMap values.

`InventorySnapshot.security` includes summarized `NetworkPolicy` coverage, non-`system:*` `ClusterRoleBinding` summaries, and Pod Security Admission posture derived from namespace labels. The agent does not export full RBAC rule bodies, excludes well-known `system:*` ClusterRole bindings from the summary, and still reports namespaces with missing PSA labels.

`InventorySnapshot.operational_maturity` includes descheduler detection (via well-known deployment names), VPA object count and update modes, and all CronJob summaries. All are fail-soft; snapshot still succeeds when those APIs are unavailable.

`InventorySnapshot.plugins` is an optional envelope of plugin blocks. The field is omitted from the JSON when no plugin is enabled. The supported plugin blocks are `workload_monitoring` and `postgresql_monitoring` (see "Plugins" below); the workload data contract is defined in `docs/adr/0001-workload-monitoring-plugin.md`.

### Plugins

Workload monitoring is opt-in; when disabled, `plugins` is omitted.

When enabled, workload monitoring can also scrape app metrics from annotated Services in the allowlisted namespaces. Discovery uses `prometheus.io/*` or `sentinella.io/app-metrics*`, and metric names are allowlisted.

| Variable | Default | Notes |
|---|---|---|
| `WORKLOAD_MONITORING_ENABLED` | `false` | Master switch. When `false`, the plugin block is omitted. |
| `WORKLOAD_MONITORING_NAMESPACES` | `[]` | YAML list allowlist. Empty disables the plugin regardless of `ENABLED`. |
| `WORKLOAD_MONITORING_TARGETS` | `angular,spring_boot,oracle_database` | Tech detection targets enabled for the plugin. |
| `APP_METRICS_ENABLED` | `false` | Master switch for app metrics. |
| `APP_METRICS_DISCOVERY_ENABLED` | `true` | Service annotation discovery. |
| `APP_METRICS_NAMESPACES` | `[]` | YAML allowlist for discovery. |
| `APP_METRICS_ALLOWLIST` | demo + Spring/JVM/Hikari/Tomcat allowlist | Metric name allowlist. |
| `APP_METRICS_TIMEOUT_SECS` | `3` | Per-target scrape timeout. |
| `APP_METRICS_MAX_SAMPLES` | `500` | Max samples per target. |
| `POSTGRESQL_MONITORING_ENABLED` | `false` | Master switch. When `false`, the plugin block is omitted. |
| `POSTGRESQL_MONITORING_NAMESPACES` | `[]` | YAML list allowlist. Empty disables the plugin regardless of `ENABLED`. |

When enabled with a non-empty allowlist, the snapshot gains:

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
        "app_metrics": { "targets": [] }
      },
      "logs": { "pods": [] }
    }
  }
}
```

`plugins.workload_monitoring.logs` is current pod log tail data for the allowlist.

`plugins.workload_monitoring.signals.app_metrics` holds scrape results for allowlisted Services: namespace/name, path/port, status, and bounded samples.

When `COLLECT_DEPENDENCIES_TETRAGON=true`, the agent discovers Tetragon endpoints from `EndpointSlice` and falls back to `TETRAGON_GRPC_ADDRESS`.

Technology detection comes from Pod metadata and container image/process signals. Put app-stack metadata on `spec.template.metadata.labels` or `spec.template.metadata.annotations`.

### PostgreSQL monitoring

The PostgreSQL plugin is opt-in, namespace-scoped, and discovery-first. It derives health from Kubernetes evidence and only runs `SELECT 1` when a Service clearly matches PostgreSQL heuristics.

| Variable | Default | Notes |
|---|---|---|
| `POSTGRESQL_MONITORING_ENABLED` | `false` | Master switch. When `false`, the plugin block is omitted. |
| `POSTGRESQL_MONITORING_NAMESPACES` | `[]` | YAML list allowlist. Empty disables the plugin regardless of `ENABLED`. |
| `POSTGRESQL_MONITORING_SECRET_NAME` | empty | Optional Secret name to read in the discovered service namespace. |
| `POSTGRESQL_MONITORING_HOST` | empty | Env fallback host override. Defaults to the discovered Service DNS name. |
| `POSTGRESQL_MONITORING_PORT` | empty | Env fallback port override. Defaults to the discovered PostgreSQL port. |
| `POSTGRESQL_MONITORING_USER` | empty | Env fallback user override. Defaults to `postgres`. |
| `POSTGRESQL_MONITORING_DATABASE` | empty | Env fallback database override. Defaults to `postgres`. |
| `POSTGRESQL_MONITORING_SSLMODE` | `disable` | Env fallback TLS mode. `disable` skips TLS; any other value enables TLS. |
| `READONLY_COMMANDS_ENABLED` | `false` | Enables read-only Hub commands such as `diagnose_postgresql` without enabling mutating actions. |

When a Secret is configured, the probe reads `host`, `port`, `user`, `password`, `database`, `sslmode`, and `sslrootcert`. Env vars fill gaps.

`diagnose_postgresql` reuses the same path and stays read-only.

When enabled with a non-empty allowlist, the snapshot gains:

```json
{
  "plugins": {
    "postgresql_monitoring": {
      "enabled": true,
      "namespaces": ["customer-db"],
      "schema_version": 1,
      "generated_at_ms": 1748523600000,
      "status": "healthy",
      "summary": "PostgreSQL workload found in 1 namespace(s) with 1 running pod(s)",
      "detail": [
        { "title": "namespaces", "value": "customer-db" },
        { "title": "matched workloads", "value": "1" }
      ],
      "evidence": [
        { "title": "workload", "value": "customer-db/postgresql" },
        { "title": "pod", "value": "customer-db/postgresql-0 phase=Running image=postgres:16" }
      ],
      "missing_data": []
    }
  }
}
```

`status` is synthesized from the available evidence: `healthy`, `warning`, `critical`, or `unknown`. `detail`, `evidence`, and `missing_data` are arrays of `{title, value}` objects. When the live probe runs, `evidence` includes a `probe` entry and `detail` includes the probe outcome.

`critical` means no PostgreSQL-like workload was found in the configured namespaces. `warning` means a workload was found but one or more supporting signals are incomplete. `healthy` means the discovery signals are consistent. `unknown` is reserved for ambiguous evidence.

Detection rules (in order, first match wins):

- **Labels / annotations** — `app.kubernetes.io/component` or `app.kubernetes.io/runtime` with `angular`, `spring-boot`, `oracle`, or `postgresql`; `angular.io/version`; `app.spring.io/version`.
- **Image name patterns** — `springboot`/`spring-boot` in the image name; `container-registry.oracle.com/database/...`, `gvenzl/oracle-*`, `oracle/database`; `angular-*` / `*-ng-*`.
- **Container command/args** — `java -jar *spring*.jar`, `-Dspring.profiles.active=...` (Spring Boot subtype); `oracle`, `dbca`, `sqlplus` (Oracle subtype).

Each detection sets `Technology.subtype` (`angular` / `spring_boot` / `oracle_database`) and `Technology.source` (`labels` / `image` / `process`).

**Limitations:**

- No new RBAC; uses the existing reader `ClusterRole`.
- No exec, no env var scraping, no command/args leak in the payload.
- Multi-process containers are not detected; only the entrypoint.
- Plugin block is fail-soft; missing metrics-server, Tetragon, or any optional API does not break the snapshot.
- The Hub-pushed configuration path is **out of scope** for v1 and tracked as a follow-up issue.

### GET `/v1/clusters/{cluster_id}/commands/poll?wait=30s`

Long-poll. The Hub holds the connection until it has a `CommandBatch` or until `wait` expires. Valid responses:

- `200` with body `{"commands":[...]}` — work to do.
- `204`, or `200` with `{"commands":[]}` — normal timeout; the agent reopens.
- `200` with an empty body is treated as no work.

### GET `/v1/clusters/{cluster_id}`

Startup preflight check (best-effort, fail-soft). The agent reads `last_seen_at` and logs a warning when the same `CLUSTER_ID` appears active in the last 5 minutes.

### POST `/v1/clusters/{cluster_id}/commands/{command_id}/ack`

Body: `CommandResult` with `status` (`ok` | `error` | `skipped` | `not_implemented` | `unknown`) and an optional message. Successful resource previews also carry `dry_run`, `applied_patch`, `observed_before`, `observed_after`, and `warnings` for full audit.

### Target backend contract (migration)

Target API route family is `/api/v1/...` on `https://api.hub.sentinel.la`.

| Purpose | Current implemented endpoint | Target endpoint |
|---|---|---|
| Inventory ingest | `POST /v1/clusters/{cluster_id}/inventory` | `POST /api/v1/agent/ingest` |
| Command poll | `GET /v1/clusters/{cluster_id}/commands/poll?wait=30s` | `GET /api/v1/clusters/{cluster_id}/commands/poll?wait=30s` |
| Command ack | `POST /v1/clusters/{cluster_id}/commands/{command_id}/ack` | `POST /api/v1/clusters/{cluster_id}/commands/{command_id}/ack` |
| Cluster status | `GET /v1/clusters/{cluster_id}` | `GET /api/v1/clusters/{cluster_id}` |

### Troubleshooting route/response mismatches

- `command poll failed: poll status 404 Not Found` usually means route mismatch (`/v1/...` vs `/api/v1/...`) or missing backend endpoint.
- `error decoding response body: expected value at line 1 column 1` usually means the poll endpoint returned non-JSON or empty body where JSON was expected.
- For temporary wire diagnostics, set `AGENT_LOG=debug` and `AGENT_HTTP_DEBUG=true`. Set `AGENT_HTTP_DEBUG_BODIES=true` only when needed; logs include bounded (`200` chars) response body previews and POST request body previews. `FULL_DEBUG=true` is a last resort only and prints full payloads and bodies.

### Troubleshooting runtime capabilities

Every snapshot carries two top-level fields that report the availability state of optional Kubernetes APIs the agent depends on. The same `state ∈ {ok, missing, forbidden, unavailable, error}` enum is used in both.

#### `metrics` — pod-metrics API (`metrics.k8s.io`)

| `metrics.state` | Reason / action |
|---|---|
| `ok` | metrics-server is installed and reachable. `usage_cpu` / `usage_memory` are populated. |
| `missing` | `metrics-server not installed`. Install metrics-server (or a compatible substitute) in the cluster. |
| `forbidden` | `ServiceAccount missing metrics.k8s.io RBAC`. Grant the agent ServiceAccount `get/list/watch` on `pods` and on the `metrics.k8s.io` API group. |
| `unavailable` | `metrics-server registered but not ready` (or `metrics-server timeout`). Check the metrics-server pods. |
| `error` | Transient failure. The agent retries every cycle. Inspect the agent logs for the full kube error. |

The agent tries `metrics.k8s.io/v1` first and falls back to `v1beta1` only on a clean v1 404. The `source` field reports which path succeeded.

#### `snapshot_api` — CSI snapshot API (`snapshot.storage.k8s.io`)

| `snapshot_api.state` | Reason / action |
|---|---|
| `ok` | CSI snapshot CRDs are installed. `volumesnapshotclasses_count` / `volumesnapshots_count` are populated. |
| `missing` | `CSI snapshot CRDs not installed`. This is normal on clusters that don't use volume snapshots; the agent logs once and stays quiet. |
| `forbidden` | `ServiceAccount missing snapshot.storage.k8s.io RBAC`. Grant the agent ServiceAccount `get/list/watch` on `volumesnapshotclasses` and `volumesnapshots`. |
| `unavailable` | `CSI snapshot API registered but not ready` (or `CSI snapshot API timeout`). Check the snapshot-controller pods. |
| `error` | Transient failure. The agent retries every cycle. Inspect the agent logs for the full kube error. |

The agent probes `VolumeSnapshotClass` as the canonical signal. If that returns 404, the `VolumeSnapshot` probe is skipped (the API group is absent); the `volumesnapshots_count` is 0 in that case.

For both fields, the agent logs the first hit and state transitions immediately; the same state is suppressed for 5 minutes before being re-emitted as a reminder.

## Configuration (env vars from ConfigMap/Secret)

Recommended `HUB_URL` is `https://api.hub.sentinel.la`.

| Variable | Source | Default |
|---|---|---|
| `HUB_URL` | ConfigMap | required |
| `CLUSTER_ID` | ConfigMap | required |
| `HUB_API_KEY` | Secret | optional |
| `AGENT_VERSION_OVERRIDE` | ConfigMap | optional |
| `COLLECT_INTERVAL_SECS` | ConfigMap | `60` |
| `POLL_WAIT_SECS` | ConfigMap | `30` |
| `HTTP_TIMEOUT_SECS` | ConfigMap | `20` |
| `LEASE_TTL_SECS` | ConfigMap | `30` |
| `LEASE_NAME` | ConfigMap | `sentinella-hub-k8s-agent-leader` |
| `ACTIONS_ENABLED` | ConfigMap | `false` |
| `COLLECT_SECRETS` | ConfigMap | `false` |
| `COLLECT_DEPENDENCIES_TETRAGON` | ConfigMap | `false` |
| `TETRAGON_REQUIRED_FOR_READINESS` | ConfigMap | `true` |
| `TETRAGON_GRPC_ADDRESS` | ConfigMap | when `COLLECT_DEPENDENCIES_TETRAGON=true`, default `tetragon-grpc.tetragon.svc.cluster.local:54321` |
| `WORKLOAD_MONITORING_ENABLED` | ConfigMap | `false` |
| `WORKLOAD_MONITORING_NAMESPACES` | ConfigMap | empty YAML list (`[]`) |
| `WORKLOAD_MONITORING_TARGETS` | ConfigMap | `angular,spring_boot,oracle_database` |
| `APP_METRICS_ENABLED` | ConfigMap | `false` |
| `APP_METRICS_DISCOVERY_ENABLED` | ConfigMap | `true` |
| `APP_METRICS_NAMESPACES` | ConfigMap | empty YAML list (`[]`) |
| `APP_METRICS_ALLOWLIST` | ConfigMap | `process_.*` |
| `APP_METRICS_TIMEOUT_SECS` | ConfigMap | `3` |
| `APP_METRICS_MAX_SAMPLES` | ConfigMap | `500` |
| `POSTGRESQL_MONITORING_ENABLED` | ConfigMap | `false` |
| `POSTGRESQL_MONITORING_NAMESPACES` | ConfigMap | empty YAML list (`[]`) |
| `FULL_DEBUG` | ConfigMap | `false` |
| `AGENT_HTTP_DEBUG` | ConfigMap | `false` |
| `AGENT_HTTP_DEBUG_BODIES` | ConfigMap | `false` |
| `AGENT_LOG` | ConfigMap | `info` |
| `RUST_LOG` | ConfigMap | optional legacy alias |
| `POD_NAME`, `POD_NAMESPACE`, `NODE_NAME` | downward API | auto |

When `AGENT_VERSION_OVERRIDE` is set to a non-empty value, snapshots report that value as `agent.version`. When unset or empty, the agent reports the compile-time package version.

When `COLLECT_SECRETS=true`, the agent attempts to collect Secret metadata and key names. This requires separate read RBAC for `secrets` (`get/list/watch`).

When `COLLECT_DEPENDENCIES_TETRAGON=true`, Sentinella connects to Tetragon over internal Kubernetes gRPC using `TETRAGON_GRPC_ADDRESS`. The default address is `tetragon-grpc.tetragon.svc.cluster.local:54321`.

Dependency collection remains fail-soft at the snapshot layer. By default, the agent readiness probe blocks the pod from becoming Ready while dependency collection is enabled and Tetragon is not connected. Set `TETRAGON_REQUIRED_FOR_READINESS=false` to keep the pod Ready even when Tetragon is unavailable.

## Build

This repository produces a multi-arch Rust agent image from the `agent-runtime` target in `Dockerfile`. The published image is a manifest list for `linux/amd64` and `linux/arm64`; the runtime layer is `gcr.io/distroless/cc-debian12:nonroot`. aarch64 support is built with QEMU emulation in CI.

```bash
podman build --target agent-runtime -t ghcr.io/sentinella/sentinella-hub-k8s-agent:0.1.0 .
podman push ghcr.io/sentinella/sentinella-hub-k8s-agent:0.1.0
```

## Deploy

1. Quick install (one-liner):
   ```bash
   curl -sfL https://raw.githubusercontent.com/thesentinella/hub-kubernetes-agent/main/install.sh \
     | CLUSTER_ID="my-cluster" HUB_API_KEY="shub_..." bash
   ```

   The installer creates the auth Secret from `HUB_API_KEY`, validates the workload with server-side dry-run, applies `agent.yaml` plus `sentinella-dev-operator-policy.yaml`, and auto-detects the platform.
   - Force a platform when needed:
     ```bash
     curl -sfL https://raw.githubusercontent.com/thesentinella/hub-kubernetes-agent/main/install.sh \
       | CLUSTER_ID="my-cluster" HUB_API_KEY="shub_..." bash -s -- --platform openshift
     ```
   - Or set `INSTALL_PLATFORM=kubernetes|openshift`.
   - Optional integrity check: set `VERIFY_MANIFEST_CHECKSUM=true` (or `1`) to verify the downloaded `agent.yaml` before apply.

2. (Optional) Install Tetragon — required only when `COLLECT_DEPENDENCIES_TETRAGON=true`:
   ```bash
   helm repo add cilium https://helm.cilium.io >/dev/null 2>&1 || true; helm repo update; kubectl create namespace tetragon --dry-run=client -o yaml | kubectl apply -f -; helm upgrade --install tetragon cilium/tetragon -n tetragon --reset-values --set tetragonOperator.enabled=false --set crds.installMethod=helm --set tetragon.grpc.enabled=true --set-string tetragon.grpc.address="0.0.0.0:54321"; printf '%s\n' 'apiVersion: v1' 'kind: Service' 'metadata:' '  name: tetragon-grpc' '  namespace: tetragon' 'spec:' '  type: ClusterIP' '  internalTrafficPolicy: Local' '  selector:' '    app.kubernetes.io/name: tetragon' '    app.kubernetes.io/instance: tetragon' '  ports:' '    - name: grpc' '      protocol: TCP' '      port: 54321' '      targetPort: 54321' | kubectl apply -f -
   ```
   This creates the internal service `tetragon-grpc.tetragon.svc.cluster.local:54321` with `internalTrafficPolicy: Local` so each agent pod talks to a Tetragon pod on the same node.

3. Manual install alternative:
   - Create the auth Secret (once per namespace/cluster):
     ```bash
     printf '%s' '<API_KEY>' | kubectl create secret generic sentinella-hub-k8s-agent-auth \
       --namespace sentinella \
       --from-file=api-key=/dev/stdin \
       --dry-run=client -o yaml | kubectl apply -f -
     ```
    - Edit `agent.yaml`:
      - `CLUSTER_ID` unique per cluster.
      - `image:` for the `agent` container pointing to your Rust agent image.
      - For dependency collection: set `COLLECT_DEPENDENCIES_TETRAGON=true`. The default `TETRAGON_GRPC_ADDRESS` in that mode is `tetragon-grpc.tetragon.svc.cluster.local:54321`. Set `TETRAGON_REQUIRED_FOR_READINESS=false` for dev clusters or nodes that cannot run Tetragon.
      - Toleration block — current value runs on every node including control plane; trim if you want a smaller footprint.
    - Apply `sentinella-dev-operator-policy.yaml` alongside `agent.yaml` so the operator policy ships with the install bundle.
    - Apply:
      ```bash
      kubectl apply -f agent.yaml
      kubectl apply -f sentinella-dev-operator-policy.yaml
      ```

4. Verify:
   ```bash
   kubectl -n sentinella get ds,po
   kubectl -n sentinella get lease
   kubectl -n sentinella logs ds/sentinella-hub-k8s-agent --tail=50
   kubectl -n sentinella port-forward ds/sentinella-hub-k8s-agent 9090:9090
   curl localhost:9090/metrics
   ```

OpenShift installs use the same agent image and code path. The installer auto-detects OpenShift and applies the right toleration/SCC annotations. With dependency collection disabled, the agent behaves like a normal inventory collector. With dependency collection enabled, readiness blocks until Tetragon is connected unless `TETRAGON_REQUIRED_FOR_READINESS=false`. The manifest checksum check is opt-in and off by default. `FULL_DEBUG` is a last resort only.

The `Lease` object will appear once the first pod is up; its `holderIdentity` is the leader node's name.

## Project layout

```
src/
  main.rs        # entrypoint, loops, shutdown
  config.rs      # env -> Config
  model.rs       # wire format DTOs
  collector.rs   # reads Kubernetes API and builds the snapshot
  hub.rs         # HTTP client to the Hub (snapshot + long-poll + ack)
  executor.rs    # command dispatch (disabled by flag)
  leader.rs      # leader election via Kubernetes Lease
  tech.rs        # image-based technology detection + tests
  health.rs      # /livez /readyz /metrics
agent.yaml      # ServiceAccount, RBAC (ClusterRole + Role), ConfigMap, DaemonSet
```

## Contributing

### Branching

Work on a feature branch and open a PR against `main`. All merges use **squash merge** — the PR title becomes the commit on `main`.

### PR title conventions

Releases are automated via [release-please](https://github.com/googleapis/release-please). The PR title determines whether a release is created and what version bump it triggers:

Note: the prefix must be lowercase (`feat:`, `fix:`, `perf:`, etc.). Capitalized variants like `Feat:` are ignored.

| Prefix | Effect | Example |
|--------|--------|---------|
| `feat:` | minor bump (`v0.1.0` → `v0.2.0`) | `feat: collect NetworkPolicies` |
| `fix:` | patch bump (`v0.1.0` → `v0.1.1`) | `fix: correct timeout on hub reconnect` |
| `perf:` | patch bump | `perf: skip unchanged nodes in snapshot` |
| `feat!:` / `fix!:` | major bump (`v0.1.0` → `v1.0.0`) | `feat!: redesign inventory payload format` |
| `chore:` | no release | `chore: update Cargo dependencies` |
| `ci:` | no release | `ci: add Docker layer cache` |
| `docs:` | no release | `docs: document action contract` |
| `refactor:` | no release | `refactor: split collector into modules` |

Rules:
- English, imperative mood ("add" not "added").
- No trailing period.
- Breaking changes append `!` after the type: `feat!:`.

### Release process

Releases are fully automated:

1. Merge a `feat:` or `fix:` PR → release-please opens (or updates) a release PR that bumps `Cargo.toml` and updates `CHANGELOG.md`.
2. Merge the release PR → release-please creates the git tag (e.g. `v0.2.0`).
3. The `release` workflow triggers: builds the Docker image, pushes `:v0.2.0` + `:latest` to Artifact Registry, updates `agent.yaml` with the new image tag, and notifies Slack.

No manual tagging required.

## Suggested roadmap

1. Expand pre-flight safety checks from presence/target signals to full value validation (LimitRange bounds and ResourceQuota deltas).
2. Add command-level policy controls for apply (for example allow/deny lists by namespace/workload kind).
3. Add additional commands: `scale_workload`, `restart_workload` (rollout restart annotation), `cordon_node`.
4. Evaluate in-place pod resize via the `pods/resize` subresource (Kubernetes 1.33+) for containers with a compatible `resizePolicy`.
5. Add incremental collection (watches instead of full lists every minute) once snapshots get costly on large clusters.
6. Add node-local data collection (kubelet stats, host filesystem) using the existing per-node pod presence.
