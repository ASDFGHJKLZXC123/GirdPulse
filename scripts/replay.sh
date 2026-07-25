#!/usr/bin/env bash
# Rebuild the five disposable projection tables from a selected Kafka log position.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_dir="$repo_root/.run"
log_dir="$repo_root/.logs"
pid_file="$run_dir/projector.pid"
log_file="$log_dir/projector.log"
mode=""
since=""
watermark=""

usage() {
  echo "usage: replay.sh (--from-beginning | --since <ISO8601>) [--to-watermark <file>]" >&2
  exit 2
}

absolute_existing_file() {
  local input="$1"
  local directory base
  directory="$(dirname "$input")"
  base="$(basename "$input")"
  if [ ! -f "$input" ]; then
    echo "replay: watermark file does not exist: $input" >&2
    exit 1
  fi
  directory="$(cd "$directory" && pwd)"
  printf '%s/%s\n' "$directory" "$base"
}

stop_projector() {
  local pid command attempt
  if [ ! -f "$pid_file" ]; then
    return
  fi

  pid="$(cat "$pid_file")"
  if ! [[ "$pid" =~ ^[1-9][0-9]*$ ]]; then
    echo "replay: refusing invalid PID in $pid_file" >&2
    exit 1
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$pid_file"
    return
  fi

  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  if [[ "$command" != *"$repo_root/projector/src/index.ts"* ]]; then
    echo "replay: refusing to stop PID $pid; command is not the projector: $command" >&2
    exit 1
  fi

  echo "replay: stopping projector (pid $pid)"
  kill "$pid"
  for attempt in $(seq 1 80); do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$pid_file"
      return
    fi
    sleep 0.25
  done

  echo "replay: projector did not stop after 20 seconds; projection was not changed" >&2
  exit 1
}

wait_for_watermark() {
  local timeout="${REPLAY_TIMEOUT_SECONDS:-600}"
  local pid elapsed=0
  if ! [[ "$timeout" =~ ^[1-9][0-9]*$ ]]; then
    echo "replay: REPLAY_TIMEOUT_SECONDS must be a positive integer" >&2
    exit 1
  fi
  if [ ! -f "$pid_file" ]; then
    echo "replay: projector PID file disappeared before watermark wait" >&2
    exit 1
  fi
  pid="$(cat "$pid_file")"
  if ! [[ "$pid" =~ ^[1-9][0-9]*$ ]]; then
    echo "replay: refusing invalid PID in $pid_file" >&2
    exit 1
  fi

  while kill -0 "$pid" 2>/dev/null; do
    if [ "$elapsed" -ge "$timeout" ]; then
      echo "replay: timed out after ${timeout}s waiting for the fixed watermark" >&2
      stop_projector
      exit 1
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  rm -f "$pid_file"

  if grep -q "projector failed:" "$log_file"; then
    echo "replay: projector failed before completing; see $log_file" >&2
    exit 1
  fi
  if ! grep -q "projector watermark reached" "$log_file"; then
    echo "replay: projector exited without reaching the requested watermark; see $log_file" >&2
    exit 1
  fi
  echo "replay: fixed watermark reached in ${elapsed}s"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --from-beginning)
      [ -z "$mode" ] || usage
      mode="from-beginning"
      shift
      ;;
    --since)
      [ -z "$mode" ] || usage
      [ "$#" -ge 2 ] || usage
      mode="since"
      since="$2"
      shift 2
      ;;
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

[ -n "$mode" ] || usage
mkdir -p "$run_dir" "$log_dir"
stop_projector

if [ "$mode" = "from-beginning" ]; then
  seed_args=(--from-beginning)
else
  seed_args=(--since "$since")
fi
if [ -n "$watermark" ]; then
  seed_args+=(--watermark "$watermark")
fi
seed_args+=(--confirm-projection-rebuild)
pnpm --dir "$repo_root/projector" exec tsx src/tools/seed-since.ts "${seed_args[@]}"

: >"$log_file"
probe="grep -q 'projector migrations applied' '$log_file' && grep -q 'projector consumer joined' '$log_file'"
launcher=(env -u BUG_SWAP_COORDS -u WATERMARK_FILE RETENTION_ENABLED=0)
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
else
  echo "replay: projector is rebuilding with retention disabled; stop it with make stop-apps"
fi
