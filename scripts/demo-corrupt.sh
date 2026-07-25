#!/usr/bin/env bash
# Rebuild the projection with the deliberate coordinate-swap bug enabled.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_dir="$repo_root/.run"
log_dir="$repo_root/.logs"
pid_file="$run_dir/projector.pid"
log_file="$log_dir/projector.log"
watermark=""

usage() {
  echo "usage: demo-corrupt.sh [--to-watermark <file>]" >&2
  exit 2
}

absolute_existing_file() {
  local input="$1"
  local directory base
  if [ ! -f "$input" ]; then
    echo "demo-corrupt: watermark file does not exist: $input" >&2
    exit 1
  fi
  directory="$(cd "$(dirname "$input")" && pwd)"
  base="$(basename "$input")"
  printf '%s/%s\n' "$directory" "$base"
}

stop_projector() {
  local pid command attempt
  if [ ! -f "$pid_file" ]; then
    return
  fi
  pid="$(cat "$pid_file")"
  if ! [[ "$pid" =~ ^[1-9][0-9]*$ ]]; then
    echo "demo-corrupt: refusing invalid PID in $pid_file" >&2
    exit 1
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$pid_file"
    return
  fi
  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  if [[ "$command" != *"$repo_root/projector/src/index.ts"* ]]; then
    echo "demo-corrupt: refusing to stop PID $pid; command is not the projector: $command" >&2
    exit 1
  fi

  echo "demo-corrupt: stopping projector (pid $pid)"
  kill "$pid"
  for attempt in $(seq 1 80); do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$pid_file"
      return
    fi
    sleep 0.25
  done
  echo "demo-corrupt: projector did not stop after 20 seconds; projection was not changed" >&2
  exit 1
}

wait_for_watermark() {
  local timeout="${REPLAY_TIMEOUT_SECONDS:-600}"
  local pid elapsed=0
  if ! [[ "$timeout" =~ ^[1-9][0-9]*$ ]]; then
    echo "demo-corrupt: REPLAY_TIMEOUT_SECONDS must be a positive integer" >&2
    exit 1
  fi
  if [ ! -f "$pid_file" ]; then
    echo "demo-corrupt: projector PID file disappeared before watermark wait" >&2
    exit 1
  fi
  pid="$(cat "$pid_file")"
  if ! [[ "$pid" =~ ^[1-9][0-9]*$ ]]; then
    echo "demo-corrupt: refusing invalid PID in $pid_file" >&2
    exit 1
  fi
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$elapsed" -ge "$timeout" ]; then
      echo "demo-corrupt: timed out after ${timeout}s waiting for the fixed watermark" >&2
      stop_projector
      exit 1
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  rm -f "$pid_file"

  if grep -q "projector failed:" "$log_file"; then
    echo "demo-corrupt: projector failed before completing; see $log_file" >&2
    exit 1
  fi
  if ! grep -q "projector watermark reached" "$log_file"; then
    echo "demo-corrupt: projector exited without reaching the requested watermark" >&2
    exit 1
  fi
  echo "demo-corrupt: fixed watermark reached in ${elapsed}s"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --to-watermark)
      [ "$#" -ge 2 ] || usage
      [ -z "$watermark" ] || usage
      watermark="$(absolute_existing_file "$2")"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

mkdir -p "$run_dir" "$log_dir"
stop_projector
seed_args=(--from-beginning)
if [ -n "$watermark" ]; then
  seed_args+=(--watermark "$watermark")
fi
seed_args+=(--confirm-projection-rebuild)
pnpm --dir "$repo_root/projector" exec tsx src/tools/seed-since.ts "${seed_args[@]}"

: >"$log_file"
probe="grep -q 'projector migrations applied' '$log_file' && grep -q 'projector consumer joined' '$log_file'"
launcher=(env -u WATERMARK_FILE BUG_SWAP_COORDS=1 RETENTION_ENABLED=0)
if [ -n "$watermark" ]; then
  launcher+=("WATERMARK_FILE=$watermark")
fi
launcher+=(
  "$repo_root/scripts/run.sh"
  projector
  "$probe"
  --
  node
  --import
  "$repo_root/projector/node_modules/tsx/dist/loader.mjs"
  "$repo_root/projector/src/index.ts"
)
"${launcher[@]}"

if [ -n "$watermark" ]; then
  wait_for_watermark
  pnpm --dir "$repo_root/projector" exec tsx src/tools/checksum.ts
else
  echo "demo-corrupt: BUG_SWAP_COORDS=1 is active; inspect vehicle_positions, then run make replay"
fi
