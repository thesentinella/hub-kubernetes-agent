# Sentinella Hub Kubernetes Agent

Inventory collector and (future) action executor for Kubernetes and OpenShift clusters, part of the **Sentinella Hub** ecosystem. Designed to live alongside future sibling agents (VMware Agent, OpenStack Agent, etc.) under the same Hub.

## What it does in this release

- Connects to the Kubernetes API using its `ServiceAccount` (cluster-wide read-only RBAC).
- Runs as a **DaemonSet** with **leader election** via a `coordination.k8s.io/Lease`. Only the leader collects and ships inventory; non-leaders idle on that loop. All pods poll for commands.
- Every minute the leader collects and sends to the Hub an inventory **snapshot**:
  - **Cluster**: Kubernetes version, detected platform (vanilla / openshift / eks / gke / aks), nodes with capacity/allocatable, kubelet version, container runtime, OS image, roles.
  - **Namespaces** with labels and phase.
  - **Workloads**: deployments, statefulsets, daemonsets (name, namespace, desired/ready replicas).
  - **Pods**: age in seconds (`age_seconds`), each container with image, **detected technology** (vendor/product/version inferred from the image), `requests` and `limits` (CPU and memory).
  - **Storage**: StorageClasses (name/provisioner/safe parameter subset), PersistentVolumes, PersistentVolumeClaims, VolumeSnapshotClasses, and VolumeSnapshots.
- Maintains an open long-poll against the Hub for command delivery. **Action execution is disabled by default** (`ACTIONS_ENABLED=false`); the agent replies with `skipped` and an explanatory message to any command received. When actions are explicitly enabled, the agent can preview workload resource patches with a Kubernetes server-side dry-run.

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

Unit tests are included in `tech.rs`. Extending coverage is one entry in the `RULES` table.

### Actions — designed for gradual rollout

`src/executor.rs` rejects every command while `ACTIONS_ENABLED=false`, before parsing the command spec. When actions are enabled, `preview_workload_resources` performs a server-side dry-run and `apply_workload_resources` performs a live strategic-merge patch.

#### Commands: setting requests and limits

Two command kinds form the resource-patch flow:

- **`preview_workload_resources`** — implemented server-side dry-run. Computes the patch, runs it against the apiserver with `?dryRun=All`, and returns the would-be state. Cluster state is unchanged.
- **`apply_workload_resources`** — live apply. Uses the same strategic-merge patch shape and pre-flight warning checks as preview, but persists the change.

The two-command pattern is intentional. Each artifact (preview, approval, apply) is a separate Hub record with its own `command_id`, timestamp, and audit trail — easier for dashboards, easier for compliance reviews. Cluster state can change between preview and apply (HPA scaled, new pods); the apply re-validates against fresh state rather than relying on a stale preview.

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
  verbs: ["patch"]
```

This must be a separate ClusterRole/Binding applied only when `ACTIONS_ENABLED=true`. The read-only ClusterRole stays untouched. **No `*` on `*/*`**. The root `agent.yaml` does not grant this workload patch RBAC by default.

Pre-flight warning checks are also best-effort. Without additional read permissions for HPAs, VPAs, LimitRanges, ResourceQuotas, and PDBs, preview still succeeds but includes `preflight.check.unavailable` warnings for the checks the agent cannot evaluate.

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

### GET `/v1/clusters/{cluster_id}/commands/poll?wait=30s`

Long-poll. The Hub holds the connection until it has a `CommandBatch` or until `wait` expires. Valid responses:

- `200` with body `{"commands":[...]}` — work to do.
- `204`, or `200` with `{"commands":[]}` — normal timeout; the agent reopens.
- `200` with an empty body is treated as no work.

### POST `/v1/clusters/{cluster_id}/commands/{command_id}/ack`

Body: `CommandResult` with `status` (`ok` | `error` | `skipped` | `not_implemented` | `unknown`) and an optional message. Successful resource previews also carry `dry_run`, `applied_patch`, `observed_before`, `observed_after`, and `warnings` for full audit.

### Target backend contract (migration)

Target API route family is `/api/v1/...` on `https://api.hub.sentinel.la`.

| Purpose | Current implemented endpoint | Target endpoint |
|---|---|---|
| Inventory ingest | `POST /v1/clusters/{cluster_id}/inventory` | `POST /api/v1/agent/ingest` |
| Command poll | `GET /v1/clusters/{cluster_id}/commands/poll?wait=30s` | `GET /api/v1/clusters/{cluster_id}/commands/poll?wait=30s` |
| Command ack | `POST /v1/clusters/{cluster_id}/commands/{command_id}/ack` | `POST /api/v1/clusters/{cluster_id}/commands/{command_id}/ack` |

### Troubleshooting route/response mismatches

- `command poll failed: poll status 404 Not Found` usually means route mismatch (`/v1/...` vs `/api/v1/...`) or missing backend endpoint.
- `error decoding response body: expected value at line 1 column 1` usually means the poll endpoint returned non-JSON or empty body where JSON was expected.
- For temporary wire diagnostics, set `AGENT_LOG=debug` and `AGENT_HTTP_DEBUG=true`. Set `AGENT_HTTP_DEBUG_BODIES=true` only when needed; logs include bounded (`200` chars) response body previews and POST request body previews.

## Configuration (env vars from ConfigMap/Secret)

Recommended `HUB_URL` is `https://api.hub.sentinel.la`.

| Variable | Source | Default |
|---|---|---|
| `HUB_URL` | ConfigMap | required |
| `CLUSTER_ID` | ConfigMap | required |
| `HUB_API_KEY` | Secret | optional |
| `COLLECT_INTERVAL_SECS` | ConfigMap | `60` |
| `POLL_WAIT_SECS` | ConfigMap | `30` |
| `HTTP_TIMEOUT_SECS` | ConfigMap | `20` |
| `LEASE_TTL_SECS` | ConfigMap | `30` |
| `LEASE_NAME` | ConfigMap | `sentinella-hub-k8s-agent-leader` |
| `ACTIONS_ENABLED` | ConfigMap | `false` |
| `AGENT_HTTP_DEBUG` | ConfigMap | `false` |
| `AGENT_HTTP_DEBUG_BODIES` | ConfigMap | `false` |
| `AGENT_LOG` | ConfigMap | `info` |
| `RUST_LOG` | ConfigMap | optional legacy alias |
| `POD_NAME`, `POD_NAMESPACE`, `NODE_NAME` | downward API | auto |

## Build

```bash
docker build -t ghcr.io/sentinella/sentinella-hub-k8s-agent:0.1.0 .
docker push  ghcr.io/sentinella/sentinella-hub-k8s-agent:0.1.0
```

## Deploy

1. Create the auth Secret (once per namespace/cluster):
   ```bash
   kubectl create secret generic sentinella-hub-k8s-agent-auth \
     --namespace sentinella \
     --from-literal=api-key=<API_KEY>
   ```
2. Edit `agent.yaml`:
   - `CLUSTER_ID` unique per cluster.
   - `image:` pointing to your registry.
   - Toleration block — current value runs on every node including control plane; trim if you want a smaller footprint.
3. `kubectl apply -f agent.yaml`
4. Verify:
   ```bash
   kubectl -n sentinella get ds,po
   kubectl -n sentinella get lease
   kubectl -n sentinella logs ds/sentinella-hub-k8s-agent --tail=50
   kubectl -n sentinella port-forward ds/sentinella-hub-k8s-agent 9090:9090
   curl localhost:9090/metrics
   ```

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
3. Add additional commands: `scale_workload`, `restart_workload` (rollout restart annotation), `cordon_node`, `drain_node`.
4. Evaluate in-place pod resize via the `pods/resize` subresource (Kubernetes 1.33+) for containers with a compatible `resizePolicy`.
5. Add incremental collection (watches instead of full lists every minute) once snapshots get costly on large clusters.
6. Add node-local data collection (kubelet stats, host filesystem) using the existing per-node pod presence.
