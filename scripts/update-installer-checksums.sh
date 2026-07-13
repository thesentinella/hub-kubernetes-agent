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
LIFECYCLE_HASH="$(sha256_file sentinella-action-operator-lifecycle.yaml)"

MANIFEST_HASH="$MANIFEST_HASH" \
LIFECYCLE_HASH="$LIFECYCLE_HASH" \
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

if manifest_count != 1:
    raise SystemExit(
        f"Expected exactly one MANIFEST_SHA256 entry, found {manifest_count}"
    )

content, lifecycle_count = re.subn(
    r'^ACTION_OPERATOR_LIFECYCLE_SHA256="[^"]*"$',
    f'ACTION_OPERATOR_LIFECYCLE_SHA256="{os.environ["LIFECYCLE_HASH"]}"',
    content,
    flags=re.MULTILINE,
)

if lifecycle_count != 1:
    raise SystemExit(
        f"Expected exactly one ACTION_OPERATOR_LIFECYCLE_SHA256 entry, found {lifecycle_count}"
    )

path.write_text(content)
PY

echo "Installer checksums synchronized."
echo "MANIFEST_SHA256=$MANIFEST_HASH"
echo "ACTION_OPERATOR_LIFECYCLE_SHA256=$LIFECYCLE_HASH"
