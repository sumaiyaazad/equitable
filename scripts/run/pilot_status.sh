#!/usr/bin/env bash
# Is the pre-flight done, and did it run OK?
#
# Pilots launched directly (not through job.sh) leave no status/exit_code file, so
# "ran OK" is judged from the evidence: the process is gone, the expected cells
# exist, none is `blocked`, and no idiomatic arm errored.
#
# Only ACTIVE pilot ids are reported. Superseded runs are parked in
# runs/pilot/superseded/ so a stale failure from a since-fixed bug cannot be
# mistaken for a current one.
set -uo pipefail
ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

running=0
echo "===== processes ====="
if pgrep -af "run_diagnostic.py|run_d5_recovery.py|run_d7s_sweep.py|run_boundary.py|preflight.sh|repair.sh" \
     | grep -v pgrep; then
  echo "  -> STILL RUNNING; do not start the campaign yet"
  running=1
else
  echo "  nothing running"
fi
# Cells written before this stamp predate the current adapter code; a repair run
# supersedes them, so they are labelled rather than counted as live failures.
SINCE="${PILOT_STALE_BEFORE:-$(date -u -r src/adapters/_flink_worker.py +%Y%m%dT%H%M%SZ 2>/dev/null)}"

shopt -s nullglob
found=0
for file in runs/pilot/*_id; do
  id="$(cat "$file")"; found=1
  # core grid = 5 domains x 7 engines x 6 diagnostics
  case "$(basename "$file")" in
    preflight_id) expected=210 ;;
    *)            expected=42  ;;
  esac
  n="$(grep -l "\"invocation_id\": \"$id\"" runs/*/summary.json 2>/dev/null | wc -l)"
  echo
  echo "===== $id : $n run(s) on disk ====="
  [ "$n" -eq 0 ] && { echo "  not started yet"; continue; }
  flags=(); [ "$running" -eq 1 ] && flags+=(--running)
  .venv/bin/python scripts/run/pilot_status_check.py --invocation "$id" \
    --expected "$expected" --since "$SINCE" "${flags[@]}"
done
[ "$found" -eq 1 ] || echo "  no active pilot ids in runs/pilot/"
