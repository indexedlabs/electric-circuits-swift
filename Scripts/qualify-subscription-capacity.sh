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

exec perl -MPOSIX=setsid -e '
  my $deadline = shift @ARGV;
  my $pid = fork();
  die "fork failed: $!\n" unless defined $pid;
  if ($pid == 0) {
    setsid() or die "setsid failed: $!\n";
    exec @ARGV or die "exec failed: $!\n";
  }
  $SIG{ALRM} = sub {
    warn "subscription capacity: deadline (${deadline}s) exceeded; terminating process group $pid\n";
    kill "TERM", -$pid;
    sleep 2;
    kill "KILL", -$pid;
    waitpid $pid, 0;
    exit 124;
  };
  alarm $deadline;
  waitpid $pid, 0;
  my $status = $?;
  alarm 0;
  if ($status & 127) { exit 128 + ($status & 127); }
  exit $status >> 8;
' "$deadline_seconds" swift test --no-parallel --filter SubscriptionCapacityTests "$@"
