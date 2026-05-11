#!/usr/bin/env bash
set -euo pipefail

: "${CLUSTER_ID:?Error: CLUSTER_ID is required (e.g. production-k8s)}"
: "${HUB_API_KEY:?Error: HUB_API_KEY is required (e.g. shub_...)}"

NAMESPACE="sentinella"
MANIFEST_URL="https://raw.githubusercontent.com/thesentinella/hub-kubernetes-agent/main/agent.yaml"

for cmd in kubectl curl base64; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "Error: '$cmd' not found in PATH"; exit 1; }
done

echo "Installing Sentinella Hub Agent..."
echo "  Cluster ID : $CLUSTER_ID"
echo "  Namespace  : $NAMESPACE"
echo ""

# Portable base64 (Linux + macOS)
HUB_API_KEY_B64=$(printf '%s' "$HUB_API_KEY" | base64 | tr -d '\n')

curl -sfL "$MANIFEST_URL" \
  | sed \
      -e "s|REPLACE_ME|${CLUSTER_ID}|g" \
      -e "s|BASE64_TOKEN_HERE|${HUB_API_KEY_B64}|g" \
  | kubectl apply -f -

echo ""
echo "Waiting for rollout..."
kubectl rollout status daemonset/sentinella-hub-k8s-agent \
  --namespace "$NAMESPACE" \
  --timeout=120s

echo ""
echo "Done. The cluster will appear in the Hub within ~60 seconds."
