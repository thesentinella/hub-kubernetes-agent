#!/usr/bin/env bash
set -euo pipefail

: "${CLUSTER_ID:?Error: CLUSTER_ID is required (e.g. production-k8s)}"
: "${HUB_API_KEY:?Error: HUB_API_KEY is required (e.g. shub_...)}"

NAMESPACE="sentinella"
MANIFEST_URL="https://raw.githubusercontent.com/thesentinella/hub-kubernetes-agent/main/agent.yaml"
HUB_URL="https://api.hub.sentinel.la"

for cmd in kubectl curl; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "Error: '$cmd' not found in PATH"; exit 1; }
done

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
echo ""

# Apply manifest (namespace + RBAC + ConfigMap + DaemonSet)
curl -sfL "$MANIFEST_URL" \
  | sed "s|REPLACE_ME|${CLUSTER_ID}|g" \
  | kubectl apply -f -

# Create secret separately (not in agent.yaml)
kubectl create secret generic sentinella-hub-k8s-agent-auth \
  --namespace "$NAMESPACE" \
  --from-literal=api-key="$HUB_API_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -

echo ""
echo "Waiting for rollout..."
kubectl rollout status daemonset/sentinella-hub-k8s-agent \
  --namespace "$NAMESPACE" \
  --timeout=120s

echo ""
echo "Done. The cluster will appear in the Hub within ~60 seconds."
