#!/usr/bin/env bash
# Prove that a SIGKILL between committed batches resumes from Postgres offsets
# and reaches the same fixed-watermark checksum as an uninterrupted replay.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_dir="$repo_root/.run"
log_dir="$repo_root/.logs"
pid_file="$run_dir/projector.pid"
log_file="$log_dir/projector.log"
watermark="${1:-$run_dir/watermark.json}"
control="${2:-$run_dir/control-checksum.json}"
timeout="${CRASH_TEST_TIMEOUT_SECONDS:-600}"

if [ ! -f "$watermark" ]; then
  echo "crash-recovery: watermark file does not exist: $watermark" >&2
  exit 1
fi
if [ ! -f "$control" ]; then
  echo "crash-recovery: control checksum does not exist: $control" >&2
  exit 1
fi
if ! [[ "$timeout" =~ ^[1-9][0-9]*$ ]]; then
  echo "crash-recovery: CRASH_TEST_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 1
fi

watermark="$(cd "$(dirname "$watermark")" && pwd)/$(basename "$watermark")"
control="$(cd "$(dirname "$control")" && pwd)/$(basename "$control")"

projector_command_is_safe() {
  local pid="$1"
  local command
  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  [[ "$command" == *"$repo_root/projector/src/index.ts"* ]]
}

cleanup_projector() {
  local pid attempt
  if [ ! -f "$pid_file" ]; then
    return
  fi
  pid="$(cat "$pid_file")"
  if ! [[ "$pid" =~ ^[1-9][0-9]*$ ]]; then
    echo "crash-recovery: refusing invalid cleanup PID in $pid_file" >&2
    return
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$pid_file"
    return
  fi
  if ! projector_command_is_safe "$pid"; then
    echo "crash-recovery: refusing to clean up unverified PID $pid" >&2
    return
  fi

  kill "$pid" 2>/dev/null || true
  for attempt in $(seq 1 80); do
    if ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 0.25
  done
  if kill -0 "$pid" 2>/dev/null && projector_command_is_safe "$pid"; then
    kill -KILL "$pid" 2>/dev/null || true
    for attempt in $(seq 1 40); do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 0.25
    done
  fi
  if kill -0 "$pid" 2>/dev/null; then
    echo "crash-recovery: cleanup could not stop verified projector PID $pid" >&2
    return
  fi
  rm -f "$pid_file"
}

trap cleanup_projector EXIT

echo "crash-recovery: starting a clean replay bounded by the fixed watermark"
pnpm --dir "$repo_root/projector" exec tsx src/tools/seed-since.ts \
  --from-beginning \
  --watermark "$watermark" \
  --confirm-projection-rebuild

: >"$log_file"
probe="grep -q 'projector migrations applied' '$log_file' && grep -q 'projector consumer joined' '$log_file'"
env -u BUG_SWAP_COORDS \
  WATERMARK_FILE="$watermark" \
  RETENTION_ENABLED=0 \
  "$repo_root/scripts/run.sh" \
  projector \
  "$probe" \
  -- \
  node \
  --import \
  "$repo_root/projector/node_modules/tsx/dist/loader.mjs" \
  "$repo_root/projector/src/index.ts"

elapsed=0
while ! grep -q "projector progress messages=10000" "$log_file"; do
  if [ ! -f "$pid_file" ]; then
    echo "crash-recovery: projector PID disappeared before the interruption point" >&2
    exit 1
  fi
  pid="$(cat "$pid_file")"
  if ! [[ "$pid" =~ ^[1-9][0-9]*$ ]] || ! kill -0 "$pid" 2>/dev/null; then
    echo "crash-recovery: projector exited before the interruption point" >&2
    exit 1
  fi
  if [ "$elapsed" -ge "$timeout" ]; then
    echo "crash-recovery: timed out waiting for 10,000 projected messages" >&2
    exit 1
  fi
  sleep 1
  elapsed=$((elapsed + 1))
done

pid="$(cat "$pid_file")"
if ! [[ "$pid" =~ ^[1-9][0-9]*$ ]] || ! projector_command_is_safe "$pid"; then
  echo "crash-recovery: refusing to kill an unverified PID: $pid" >&2
  exit 1
fi

echo "crash-recovery: sending SIGKILL to projector pid $pid after a committed progress marker"
kill -KILL "$pid"
for _ in $(seq 1 40); do
  if ! kill -0 "$pid" 2>/dev/null; then
    break
  fi
  sleep 0.25
done
if kill -0 "$pid" 2>/dev/null; then
  echo "crash-recovery: killed projector pid $pid did not exit" >&2
  exit 1
fi
rm -f "$pid_file"

pnpm --dir "$repo_root/projector" exec tsx src/tools/checksum.ts --expect-different "$control"

echo "crash-recovery: restarting without truncation, bounded by the same watermark"
: >"$log_file"
env -u BUG_SWAP_COORDS \
  WATERMARK_FILE="$watermark" \
  RETENTION_ENABLED=0 \
  make -C "$repo_root" projector

pid="$(cat "$pid_file")"
elapsed=0
while kill -0 "$pid" 2>/dev/null; do
  if [ "$elapsed" -ge "$timeout" ]; then
    echo "crash-recovery: timed out waiting for the resumed projector" >&2
    exit 1
  fi
  sleep 1
  elapsed=$((elapsed + 1))
done
rm -f "$pid_file"

if grep -q "projector failed:" "$log_file"; then
  echo "crash-recovery: resumed projector failed; see $log_file" >&2
  exit 1
fi
if ! grep -q "projector watermark reached" "$log_file"; then
  echo "crash-recovery: resumed projector did not reach the fixed watermark" >&2
  exit 1
fi

pnpm --dir "$repo_root/projector" exec tsx src/tools/checksum.ts --expect "$control"
trap - EXIT
echo "crash-recovery: PASS resumed projection exactly matches the uninterrupted control"
