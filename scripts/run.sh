#!/usr/bin/env bash
# scripts/run.sh <name> <readiness-probe> -- <command...>
#
# Starts <command...> in the background, writes its PID to .run/<name>.pid,
# redirects stdout/stderr to .logs/<name>.log, then polls <readiness-probe>
# (a shell command) until it exits 0 or a 60s timeout elapses. Refuses to
# start if a live PID file already exists for <name>.
set -euo pipefail

if [ "$#" -lt 4 ]; then
  echo "usage: run.sh <name> <readiness-probe> -- <command...>" >&2
  exit 1
fi

name="$1"
probe="$2"
shift 2

if [ "${1:-}" != "--" ]; then
  echo "usage: run.sh <name> <readiness-probe> -- <command...>" >&2
  exit 1
fi
shift

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_dir="$repo_root/.run"
log_dir="$repo_root/.logs"
pid_file="$run_dir/$name.pid"
log_file="$log_dir/$name.log"

mkdir -p "$run_dir" "$log_dir"

if [ -f "$pid_file" ]; then
  existing_pid="$(cat "$pid_file")"
  if ! [[ "$existing_pid" =~ ^[1-9][0-9]*$ ]]; then
    echo "run.sh: removing invalid PID file for '$name' ($pid_file)" >&2
    rm -f "$pid_file"
  elif kill -0 "$existing_pid" 2>/dev/null; then
    echo "run.sh: '$name' already running (pid $existing_pid, $pid_file)" >&2
    exit 1
  else
    rm -f "$pid_file"
  fi
fi

"$@" >"$log_file" 2>&1 &
pid=$!
echo "$pid" >"$pid_file"

terminate_child() {
  local attempt
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    for attempt in $(seq 1 20); do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 0.25
    done
  fi
  if kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
  rm -f "$pid_file"
}

timeout=60
elapsed=0
until eval "$probe" >/dev/null 2>&1; do
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "run.sh: '$name' (pid $pid) exited before becoming ready; see $log_file" >&2
    rm -f "$pid_file"
    exit 1
  fi
  if [ "$elapsed" -ge "$timeout" ]; then
    echo "run.sh: '$name' (pid $pid) did not become ready within ${timeout}s" >&2
    terminate_child
    exit 1
  fi
  sleep 1
  elapsed=$((elapsed + 1))
done

echo "run.sh: '$name' ready (pid $pid), logging to $log_file"
exit 0
