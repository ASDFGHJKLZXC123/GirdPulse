#!/usr/bin/env bash
# Stop only known GridPulse app processes recorded by scripts/run.sh.
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_dir="$repo_root/.run"
status=0

command_matches() {
  local name="$1"
  local command="$2"
  case "$name" in
    sim)
      [[ "$command" == *"pnpm"* && "$command" == *"simulator"* && "$command" == *"start"* ]]
      ;;
    anomaly-job)
      [[ "$command" == *"$repo_root/streams/anomaly-job"* ]]
      ;;
    rollup-job)
      [[ "$command" == *"$repo_root/streams/rollup-job"* ]]
      ;;
    projector)
      [[ "$command" == *"$repo_root/projector/src/index.ts"* ]]
      ;;
    api)
      [[ "$command" == *"$repo_root/api/src/index.ts"* ]]
      ;;
    *)
      return 1
      ;;
  esac
}

stop_recorded_process() {
  local pid_file="$1"
  local name pid command attempt

  name="$(basename "$pid_file" .pid)"
  case "$name" in
    sim | anomaly-job | rollup-job | projector | api) ;;
    *)
      echo "stop-apps: refusing unknown PID file $pid_file" >&2
      return 1
      ;;
  esac

  pid="$(cat "$pid_file")"
  if ! [[ "$pid" =~ ^[1-9][0-9]*$ ]]; then
    echo "stop-apps: refusing invalid PID in $pid_file" >&2
    return 1
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$pid_file"
    echo "stop-apps: removed stale PID file for $name"
    return 0
  fi

  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  if ! command_matches "$name" "$command"; then
    echo "stop-apps: refusing PID $pid; command does not match $name: $command" >&2
    return 1
  fi

  echo "stopping $name (pid $pid)"
  kill "$pid" 2>/dev/null || true
  for attempt in $(seq 1 80); do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$pid_file"
      return 0
    fi
    command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    if ! command_matches "$name" "$command"; then
      echo "stop-apps: PID $pid exited and was reused; not signaling the new process" >&2
      rm -f "$pid_file"
      return 0
    fi
    sleep 0.25
  done

  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  if ! command_matches "$name" "$command"; then
    echo "stop-apps: PID $pid changed identity; refusing SIGKILL" >&2
    rm -f "$pid_file"
    return 0
  fi

  echo "stop-apps: $name did not stop in 20 seconds; sending SIGKILL" >&2
  kill -KILL "$pid" 2>/dev/null || true
  for attempt in $(seq 1 40); do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$pid_file"
      return 0
    fi
    sleep 0.25
  done

  echo "stop-apps: verified $name PID $pid is still alive; keeping $pid_file" >&2
  return 1
}

if [ ! -d "$run_dir" ]; then
  exit 0
fi

for pid_file in "$run_dir"/*.pid; do
  [ -e "$pid_file" ] || continue
  if ! stop_recorded_process "$pid_file"; then
    status=1
  fi
done

exit "$status"
