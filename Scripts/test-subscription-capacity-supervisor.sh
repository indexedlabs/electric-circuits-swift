#!/bin/sh
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
supervisor="$root/Scripts/subscription-capacity-supervisor.pl"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/electric-circuits-swift-capacity-supervisor.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

wait_for_file() {
  file=$1
  attempts=0
  while [ ! -s "$file" ] && [ "$attempts" -lt 100 ]; do
    attempts=$((attempts + 1))
    sleep 0.01
  done
  [ -s "$file" ]
}

assert_reaped() {
  child_pid=$1
  if kill -0 "$child_pid" 2>/dev/null; then
    echo "supervisor test: direct child $child_pid survived" >&2
    return 1
  fi
  if kill -0 -"$child_pid" 2>/dev/null; then
    echo "supervisor test: process group $child_pid survived" >&2
    return 1
  fi
}

run_external_signal_case() {
  signal=$1
  expected_status=$2
  pid_file="$work_dir/$signal.pid"
  marker_file="$work_dir/$signal.marker"

  CAPACITY_TEST_PID_FILE="$pid_file" CAPACITY_TEST_MARKER_FILE="$marker_file" \
    perl "$supervisor" 30 sh -c '
      trap "echo received > \"$CAPACITY_TEST_MARKER_FILE\"; exit 0" TERM INT HUP
      echo "$$" > "$CAPACITY_TEST_PID_FILE"
      while :; do sleep 1; done
    ' &
  supervisor_pid=$!
  wait_for_file "$pid_file"
  child_pid=$(cat "$pid_file")
  kill -"$signal" "$supervisor_pid"
  if wait "$supervisor_pid"; then status=0; else status=$?; fi

  [ "$status" -eq "$expected_status" ]
  [ -f "$marker_file" ]
  assert_reaped "$child_pid"
  printf 'subscription supervisor: %s forwarding passed\n' "$signal"
}

run_alarm_case() {
  pid_file="$work_dir/alarm.pid"
  CAPACITY_TEST_PID_FILE="$pid_file" perl "$supervisor" 1 sh -c '
    trap "" TERM
    echo "$$" > "$CAPACITY_TEST_PID_FILE"
    while :; do sleep 1; done
  ' &
  supervisor_pid=$!
  wait_for_file "$pid_file"
  child_pid=$(cat "$pid_file")
  if wait "$supervisor_pid"; then status=0; else status=$?; fi

  [ "$status" -eq 124 ]
  assert_reaped "$child_pid"
  printf 'subscription supervisor: alarm forwarding passed\n'
}

run_external_escalation_case() {
  pid_file="$work_dir/escalation.pid"
  descendant_pid_file="$work_dir/escalation-descendant.pid"
  marker_file="$work_dir/escalation.marker"
  CAPACITY_TEST_PID_FILE="$pid_file" CAPACITY_TEST_DESCENDANT_PID_FILE="$descendant_pid_file" \
    CAPACITY_TEST_MARKER_FILE="$marker_file" perl "$supervisor" 30 sh -c '
    trap "echo received > \"$CAPACITY_TEST_MARKER_FILE\"; exit 0" TERM
    sh -c "trap \"\" TERM; echo \"\$\$\" > \"$CAPACITY_TEST_DESCENDANT_PID_FILE\"; while :; do sleep 1; done" &
    descendant=$!
    echo "$$" > "$CAPACITY_TEST_PID_FILE"
    wait "$descendant"
  ' &
  supervisor_pid=$!
  wait_for_file "$pid_file"
  wait_for_file "$descendant_pid_file"
  child_pid=$(cat "$pid_file")
  descendant_pid=$(cat "$descendant_pid_file")
  kill -TERM "$supervisor_pid"
  if wait "$supervisor_pid"; then status=0; else status=$?; fi

  [ "$status" -eq 143 ]
  [ -f "$marker_file" ]
  assert_reaped "$child_pid"
  assert_reaped "$descendant_pid"
  printf 'subscription supervisor: TERM descendant escalation passed\n'
}

run_pre_ready_signal_case() {
  pid_file="$work_dir/pre-ready.pid"
  gate_file="$work_dir/pre-ready.gate"
  CAPACITY_SUPERVISOR_TEST_PRE_READY_PID_FILE="$pid_file" \
    CAPACITY_SUPERVISOR_TEST_PRE_READY_GATE="$gate_file" \
    perl "$supervisor" 30 sh -c 'while :; do sleep 1; done' &
  supervisor_pid=$!
  wait_for_file "$pid_file"
  child_pid=$(cat "$pid_file")

  # TERM is pending in the blocked supervisor while the child cannot form its session yet.
  kill -TERM "$supervisor_pid"
  : > "$gate_file"
  if wait "$supervisor_pid"; then status=0; else status=$?; fi

  [ "$status" -eq 143 ]
  assert_reaped "$child_pid"
  printf 'subscription supervisor: pre-ready TERM forwarding passed\n'
}

run_pre_ready_alarm_case() {
  pid_file="$work_dir/pre-ready-alarm.pid"
  gate_file="$work_dir/pre-ready-alarm.gate"
  CAPACITY_SUPERVISOR_TEST_PRE_READY_PID_FILE="$pid_file" \
    CAPACITY_SUPERVISOR_TEST_PRE_READY_GATE="$gate_file" \
    perl "$supervisor" 1 sh -c 'while :; do sleep 1; done' &
  supervisor_pid=$!
  wait_for_file "$pid_file"
  child_pid=$(cat "$pid_file")
  if wait "$supervisor_pid"; then status=0; else status=$?; fi

  [ "$status" -eq 124 ]
  assert_reaped "$child_pid"
  printf 'subscription supervisor: pre-ready alarm forwarding passed\n'
}

run_external_signal_case TERM 143
run_external_signal_case INT 130
run_external_signal_case HUP 129
run_external_escalation_case
run_pre_ready_signal_case
run_pre_ready_alarm_case
run_alarm_case
