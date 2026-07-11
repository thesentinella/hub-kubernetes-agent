#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

sha256_file() {
  local file="$1"

  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
    return
  fi

  echo "Error: neither sha256sum nor shasum is available." >&2
  exit 1
}

MANIFEST_HASH="$(sha256_file agent.yaml)"
POLICY_HASH="$(sha256_file sentinella-default-action-policy.yaml)"

MANIFEST_HASH="$MANIFEST_HASH" \
POLICY_HASH="$POLICY_HASH" \
python3 <<'PY'
import os
import re
from pathlib import Path

path = Path("install.sh")
content = path.read_text()

content, manifest_count = re.subn(
    r'^MANIFEST_SHA256="[^"]*"$',
    f'MANIFEST_SHA256="{os.environ["MANIFEST_HASH"]}"',
    content,
    flags=re.MULTILINE,
)

content, policy_count = re.subn(
    r'^POLICY_SHA256="[^"]*"$',
    f'POLICY_SHA256="{os.environ["POLICY_HASH"]}"',
    content,
    flags=re.MULTILINE,
)

if manifest_count != 1:
    raise SystemExit(
        f"Expected exactly one MANIFEST_SHA256 entry, found {manifest_count}"
    )

if policy_count != 1:
    raise SystemExit(
        f"Expected exactly one POLICY_SHA256 entry, found {policy_count}"
    )

path.write_text(content)
PY

echo "Installer checksums synchronized."
echo "MANIFEST_SHA256=$MANIFEST_HASH"
echo "POLICY_SHA256=$POLICY_HASH"
