#!/bin/sh
set -eu

# The deadline is a harness-only hang guard. Its separate process group ensures a timed-out
# `swift test` also reaps the `swiftpm-testing` child that runs the Swift Testing suite. The test
# pass criteria remain named gate receipts and exact counters, never elapsed time.
deadline_seconds=${SUBSCRIPTION_CAPACITY_DEADLINE_SECONDS:-120}
case "$deadline_seconds" in
  ''|*[!0-9]*) echo "subscription capacity deadline must be a positive integer" >&2; exit 64 ;;
esac
if [ "$deadline_seconds" -eq 0 ]; then
  echo "subscription capacity deadline must be positive" >&2
  exit 64
fi

cd "$(dirname "$0")/.."
printf 'subscription capacity: starting isolated suite (deadline=%ss); receipts identify the last completed scale\n' "$deadline_seconds"

exec perl Scripts/subscription-capacity-supervisor.pl "$deadline_seconds" \
  swift test --no-parallel --filter SubscriptionCapacityTests "$@"
