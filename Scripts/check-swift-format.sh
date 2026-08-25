#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if [[ -n ${SWIFT_FORMAT:-} ]]; then
  formatter=$SWIFT_FORMAT
else
  formatter=$(xcrun --find swift-format 2>/dev/null || true)
fi

if ! command -v "$formatter" >/dev/null 2>&1; then
  printf 'swift-format is required (set SWIFT_FORMAT to its absolute path if needed)\n' >&2
  exit 69
fi

cd "$root"
"$formatter" lint -r Sources Tests Examples/LinearLite/Sources Examples/LinearLite/Tests \
  Examples/LinearLite/iOS
