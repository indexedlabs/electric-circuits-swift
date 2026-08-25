#!/bin/sh
set -eu

# The alarm is a harness-only hang guard. The test's pass criteria are named gate receipts and
# exact counters, never elapsed time.
cd "$(dirname "$0")/.."
exec perl -e 'alarm shift @ARGV; exec @ARGV or die "exec failed: $!\n"' 120 \
  swift test --filter capacityQualificationRecordsOneTenOneHundredAndOneThousand
