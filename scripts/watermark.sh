#!/usr/bin/env bash
# Pause the three event producers and capture a stable terminal Kafka offset per partition.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_dir="$repo_root/.run"
output="${1:-$run_dir/watermark.json}"

if [ "$#" -gt 1 ]; then
  echo "usage: watermark.sh [output-file]" >&2
  exit 2
fi
if [[ "$output" != /* ]]; then
  output="$repo_root/$output"
fi

stop_producer() {
  local name="$1"
  local expected_fragment="$2"
  local pid_file="$run_dir/$name.pid"
  local pid command attempt

  if [ ! -f "$pid_file" ]; then
    echo "watermark: $name is not running (no PID file)"
    return
  fi

  pid="$(cat "$pid_file")"
  if ! [[ "$pid" =~ ^[1-9][0-9]*$ ]]; then
    echo "watermark: refusing invalid PID in $pid_file" >&2
    exit 1
  fi

  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$pid_file"
    echo "watermark: removed stale PID file for $name"
    return
  fi

  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  if [[ "$command" != *"$expected_fragment"* ]]; then
    echo "watermark: refusing to stop PID $pid; command does not match $name: $command" >&2
    exit 1
  fi

  echo "watermark: stopping $name (pid $pid)"
  kill "$pid"
  for attempt in $(seq 1 80); do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$pid_file"
      return
    fi
    sleep 0.25
  done

  echo "watermark: $name did not stop after 20 seconds; no offsets were captured" >&2
  exit 1
}

mkdir -p "$run_dir"
stop_producer sim "simulator"
stop_producer anomaly-job "streams/anomaly-job"
stop_producer rollup-job "streams/rollup-job"

pnpm --dir "$repo_root/projector" exec tsx src/tools/watermark.ts \
  --output "$output" \
  --stable-ms 1000
