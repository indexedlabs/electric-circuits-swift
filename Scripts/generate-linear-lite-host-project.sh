#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
example_root="$root/Examples/LinearLite"
checked_project="$example_root/iOS/LinearLiteHost.xcodeproj/project.pbxproj"

if [[ -n ${XCODEGEN:-} ]]; then
  xcodegen=$XCODEGEN
elif [[ -n ${XCODEGEN_BIN_DIR:-} && -x "$XCODEGEN_BIN_DIR/xcodegen" ]]; then
  xcodegen="$XCODEGEN_BIN_DIR/xcodegen"
else
  xcodegen=$(command -v xcodegen || true)
fi

if [[ -z ${xcodegen:-} ]]; then
  printf 'xcodegen is required (set XCODEGEN or XCODEGEN_BIN_DIR)\n' >&2
  exit 69
fi

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/electric-circuits-xcodegen-project.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT
ditto "$example_root" "$work_dir/LinearLite"

(
  cd "$work_dir/LinearLite/iOS"
  "$xcodegen" generate --spec project.yml --project LinearLiteHost.xcodeproj
)

generated_project="$work_dir/LinearLite/iOS/LinearLiteHost.xcodeproj/project.pbxproj"
if ! cmp -s "$checked_project" "$generated_project"; then
  printf 'LinearLiteHost.xcodeproj is not reproducible from iOS/project.yml\n' >&2
  diff -u "$checked_project" "$generated_project" || true
  exit 1
fi

printf 'LinearLiteHost.xcodeproj matches XcodeGen output\n'
