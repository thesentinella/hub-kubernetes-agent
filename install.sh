#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="sentinella"
MANIFEST_URL="https://raw.githubusercontent.com/thesentinella/hub-kubernetes-agent/main/agent.yaml"
MANIFEST_SHA256="82ea2b350181d2d25639a4d2ac1eb65c8502814d9016381b8d8a596704e32331"
POLICY_URL="https://raw.githubusercontent.com/thesentinella/hub-kubernetes-agent/main/sentinella-dev-operator-policy.yaml"
POLICY_SHA256="db6945c787735cac6d2d1809cb01bf599a43ca8b9f75fe76d2f48847e05f7235"
HUB_URL="https://api.hub.sentinel.la"
INSTALL_PLATFORM="${INSTALL_PLATFORM:-}"
PLATFORM_OVERRIDE=""
SHA256_CMD=""
VERIFY_MANIFEST_CHECKSUM="${VERIFY_MANIFEST_CHECKSUM:-false}"
COLLECT_DEPENDENCIES_TETRAGON="${COLLECT_DEPENDENCIES_TETRAGON:-false}"

validate_cluster_id() {
  printf '%s' "$1" | grep -Eq '^[A-Za-z0-9][A-Za-z0-9._-]*$'
}

usage() {
  cat <<'EOF'
Usage: install.sh [--platform kubernetes|openshift]

Environment:
  INSTALL_PLATFORM   Override platform detection (kubernetes|openshift)
  VERIFY_MANIFEST_CHECKSUM  Set to true/1 to verify downloaded agent.yaml and policy manifest
  COLLECT_DEPENDENCIES_TETRAGON  Set to true/1 when the Tetragon gRPC service is installed
  CLUSTER_ID         Required cluster identifier
  HUB_API_KEY        Required Hub API key
EOF
}

detect_openshift() {
  kubectl get nodes -o json 2>/dev/null | grep -q 'node.openshift.io/'
}

resolve_platform() {
  if [ -n "$PLATFORM_OVERRIDE" ]; then
    printf '%s\n' "$PLATFORM_OVERRIDE"
    return 0
  fi

  if [ -n "$INSTALL_PLATFORM" ]; then
    printf '%s\n' "$INSTALL_PLATFORM"
    return 0
  fi

  if detect_openshift; then
    printf '%s\n' "openshift"
  else
    printf '%s\n' "kubernetes"
  fi
}

resolve_sha256_cmd() {
  if command -v shasum >/dev/null 2>&1; then
    SHA256_CMD="shasum -a 256"
    return 0
  fi

  if command -v sha256sum >/dev/null 2>&1; then
    SHA256_CMD="sha256sum"
    return 0
  fi

  return 1
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --platform)
      if [ "$#" -lt 2 ]; then
        echo "Error: --platform requires a value." >&2
        usage >&2
        exit 1
      fi
      PLATFORM_OVERRIDE="$2"
      shift 2
      ;;
    --platform=*)
      PLATFORM_OVERRIDE="${1#*=}"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown argument '$1'." >&2
      usage >&2
      exit 1
      ;;
  esac
done

for cmd in kubectl curl; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "Error: '$cmd' not found in PATH"; exit 1; }
done

: "${CLUSTER_ID:?Error: CLUSTER_ID is required (e.g. production-k8s)}"
: "${HUB_API_KEY:?Error: HUB_API_KEY is required (e.g. shub_...)}"

if ! validate_cluster_id "$CLUSTER_ID"; then
  echo "Error: CLUSTER_ID must match [A-Za-z0-9][A-Za-z0-9._-]*" >&2
  exit 1
fi

PLATFORM="$(resolve_platform)"
case "$PLATFORM" in
  kubernetes|openshift) ;;
  *)
    echo "Error: unsupported platform '$PLATFORM' (expected kubernetes or openshift)." >&2
    exit 1
    ;;
esac

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT
BASE_MANIFEST="$TMPDIR/agent.yaml"
RENDERED_MANIFEST="$TMPDIR/agent.rendered.yaml"
NAMESPACE_MANIFEST="$TMPDIR/agent.namespace.yaml"
WORKLOAD_MANIFEST="$TMPDIR/agent.workload.yaml"
POLICY_MANIFEST="$TMPDIR/sentinella-dev-operator-policy.yaml"

split_namespace_manifest() {
  awk -v ns="$NAMESPACE_MANIFEST" -v rest="$WORKLOAD_MANIFEST" '
    BEGIN {
      seen_first_separator = 0
      in_rest = 0
    }

    /^[[:space:]]*---[[:space:]]*$/ {
      if (!seen_first_separator) {
        seen_first_separator = 1
        print > ns
      } else {
        in_rest = 1
        print > rest
      }
      next
    }

    {
      if (!seen_first_separator) {
        print > ns
      } else if (in_rest) {
        print > rest
      } else {
        print > ns
      }
    }

    END {
      close(ns)
      close(rest)
    }
  ' "$RENDERED_MANIFEST"
}

# Validate API key before touching the cluster
echo "Validating API key..."
HTTP_STATUS=$(curl -so /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer ${HUB_API_KEY}" \
  "${HUB_URL}/api/v1/agent/whoami")

if [ "$HTTP_STATUS" != "200" ]; then
  echo "Error: API key is invalid or rejected by the hub (HTTP ${HTTP_STATUS})."
  echo "Check the key in your project settings and try again."
  exit 1
fi

WHOAMI=$(curl -sf \
  -H "Authorization: Bearer ${HUB_API_KEY}" \
  "${HUB_URL}/api/v1/agent/whoami")

echo "API key valid."
echo "  Project : $(echo "$WHOAMI" | grep -o '"project_id":"[^"]*"' | cut -d'"' -f4)"
echo "  Tenant  : $(echo "$WHOAMI" | grep -o '"tenant_id":"[^"]*"' | cut -d'"' -f4)"
echo ""

# Warn if a cluster with this ID is already registered
CLUSTER_CHECK_STATUS=$(curl -so /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer ${HUB_API_KEY}" \
  "${HUB_URL}/v1/clusters/${CLUSTER_ID}")

if [ "$CLUSTER_CHECK_STATUS" = "200" ]; then
  echo "WARNING: A cluster with ID '${CLUSTER_ID}' is already registered in Sentinella Hub."
  echo "  Running the installer again will continue reporting to the existing entry."
  echo "  If you intended to create a fresh cluster, delete the existing entry first"
  echo "  at https://hub.sentinel.la and use a different CLUSTER_ID."
  echo ""
  if [ -t 0 ]; then
    # Interactive: ask for confirmation
    read -r -p "Continue anyway? [y/N] " CONFIRM
    case "$CONFIRM" in
      [yY][eE][sS]|[yY]) ;;
      *) echo "Aborted."; exit 1 ;;
    esac
  else
    # Non-interactive (piped): proceed but keep the warning visible
    echo "Running non-interactively — continuing with existing cluster entry."
  fi
  echo ""
fi

echo "Installing Sentinella Hub Agent..."
echo "  Cluster ID : $CLUSTER_ID"
echo "  Namespace  : $NAMESPACE"
echo "  Platform   : $PLATFORM"
echo ""

case "$VERIFY_MANIFEST_CHECKSUM" in
  false|0|"")
    echo "WARNING: manifest checksum verification is disabled (set VERIFY_MANIFEST_CHECKSUM=true to enable)."
    echo ""
    ;;
esac

# Apply manifests (namespace + RBAC + ConfigMap + DaemonSet + action policy)
curl -sfL "$MANIFEST_URL" > "$BASE_MANIFEST"
curl -sfL "$POLICY_URL" > "$POLICY_MANIFEST"

case "$VERIFY_MANIFEST_CHECKSUM" in
  true|1)
    if ! resolve_sha256_cmd; then
      echo "Error: no SHA-256 tool found (need shasum or sha256sum)." >&2
      exit 1
    fi
    if ! echo "${MANIFEST_SHA256}  ${BASE_MANIFEST}" | $SHA256_CMD -c - >/dev/null 2>&1; then
      echo "Error: downloaded manifest checksum mismatch (expected ${MANIFEST_SHA256})." >&2
      exit 1
    fi
    if ! echo "${POLICY_SHA256}  ${POLICY_MANIFEST}" | $SHA256_CMD -c - >/dev/null 2>&1; then
      echo "Error: downloaded policy checksum mismatch (expected ${POLICY_SHA256})." >&2
      exit 1
    fi
    ;;
  false|0|"")
    ;;
  *)
    echo "Error: VERIFY_MANIFEST_CHECKSUM must be true, 1, false, 0, or empty." >&2
    exit 1
    ;;
esac

sed "s|REPLACE_ME|${CLUSTER_ID}|g" "$BASE_MANIFEST" > "$RENDERED_MANIFEST"

if [ "$PLATFORM" = "openshift" ]; then
  # OpenShift rejects fixed UID/GID settings under default SCCs.
  sed '/runAsUser: 65532/d;/runAsGroup: 65532/d' "$RENDERED_MANIFEST" > "$TMPDIR/agent.openshift.yaml"
  mv "$TMPDIR/agent.openshift.yaml" "$RENDERED_MANIFEST"

fi

: > "$NAMESPACE_MANIFEST"
: > "$WORKLOAD_MANIFEST"
split_namespace_manifest

if ! grep -q '^kind: Namespace$' "$NAMESPACE_MANIFEST"; then
  echo "Error: namespace manifest does not contain a Namespace resource." >&2
  exit 1
fi

if [ ! -s "$WORKLOAD_MANIFEST" ]; then
  echo "Error: workload manifest is empty after split." >&2
  exit 1
fi

kubectl apply -f "$NAMESPACE_MANIFEST"

echo "Validating manifest with server-side dry-run..."
if ! kubectl apply --dry-run=server -f "$WORKLOAD_MANIFEST" >/dev/null; then
  if [ "$PLATFORM" = "openshift" ]; then
    echo "Error: OpenShift rejected the agent manifest. Check SCC constraints or other workload security restrictions." >&2
  else
    echo "Error: the cluster rejected the agent manifest." >&2
  fi
  exit 1
fi

kubectl apply -f "$WORKLOAD_MANIFEST"

kubectl apply -f "$POLICY_MANIFEST"

# Create secret separately (not in agent.yaml)
printf '%s' "$HUB_API_KEY" | kubectl create secret generic sentinella-hub-k8s-agent-auth \
  --namespace "$NAMESPACE" \
  --from-file=api-key=/dev/stdin \
  --dry-run=client -o yaml | kubectl apply -f -

echo ""
echo "Waiting for rollout..."
kubectl rollout status daemonset/sentinella-hub-k8s-agent \
  --namespace "$NAMESPACE" \
  --timeout=120s

echo ""
echo "Done. The cluster will appear in the Hub within ~60 seconds."
