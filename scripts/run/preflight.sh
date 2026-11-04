#!/usr/bin/env bash
# Full-coverage pre-flight before committing a host to the 4,795-execution campaign.
#
# Everything the campaign touches, at the smallest official scaling size (10k) and
# one repeat, so a domain-specific fixture break or an adapter regression surfaces
# in hours instead of days. Writes only to runs/ with a pilot invocation id, so
# nothing here can be promoted into results/.
#
#   bash scripts/run/preflight.sh            # full pre-flight
#   PREFLIGHT_ROWS=50000 bash ...            # heavier variant
set -uo pipefail
ROOT="$(git rev-parse --show-toplevel)"; cd "$ROOT"
PY=".venv/bin/python"
ROWS="${PREFLIGHT_ROWS:-10000}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ID="preflight_${STAMP}"
mkdir -p runs/pilot; printf '%s\n' "$ID" > runs/pilot/preflight_id
export EQUITABLE_INVOCATION_ID="$ID"
export KSQL_SERVICE_ID="verifiable_stream_${ID}"
export KSQL_RUN_SUFFIX="$ID"

fail=0
step() { echo; echo "############ $* ############"; }

step "1/4 core grid: 5 domains x 7 engines x 6 diagnostics @ ${ROWS} rows"
$PY -u scripts/run/run_diagnostic.py --engine all --domain all \
    --diagnostic core --co-oracle --max-rows "$ROWS" --sweep || fail=$((fail+1))

step "2/4 D5 recovery: all engines x all domains (unsupported -> capability cells)"
for engine in reference materialize risingwave ksqldb flink spark feldera; do
  $PY -u scripts/run/run_d5_recovery.py --engine "$engine" --domain all \
      --rows "$ROWS" --sweep || fail=$((fail+1))
done

step "3/4 D7S sweep: engine-independent, once per domain"
$PY -u scripts/run/run_d7s_sweep.py --domain all --max-rows "$ROWS" || fail=$((fail+1))

step "4/4 campaign check-gate controls: boundary, mutation, native"
$PY -u scripts/run/run_boundary.py --domain all --max-rows 5000 || fail=$((fail+1))
RUN_GRID=0 SCALE_ROWS="" D5_ENGINES="" RUN_NATIVE=1 RUN_MUTATION=1 \
  RUN_D7S_SWEEP=0 PYTHON="$PY" bash scripts/run/run_matrix.sh || fail=$((fail+1))

echo
echo "############ pre-flight finished: $fail section(s) reported failures ############"
echo "invocation: $ID"
echo "review with: bash scripts/run/pilot_status.sh"
exit "$fail"
