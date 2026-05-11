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
