#!/bin/sh
set -eu

# These are external deadlock guards only. The contracts finish on named request, provider,
# terminal-state, and release receipts; they do not use elapsed time as a correctness signal.
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)

perl -e 'alarm shift @ARGV; exec @ARGV or die "exec failed: $!\n"' 120 \
  swift test --package-path "$root" --filter NetworkRecoveryQualificationTests
perl -e 'alarm shift @ARGV; exec @ARGV or die "exec failed: $!\n"' 120 \
  swift test --package-path "$root/Examples/LinearLite" \
  --filter closedNativeStreamKeepsOldVisibleUntilFreshScopeSnapshotIsSelectedAndPurged
