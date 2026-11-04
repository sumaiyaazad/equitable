#!/usr/bin/env bash
# ===========================================================================
# run_matrix.sh -- Step 7: the full diagnostic run matrix.
#
#   7 engines x 5 domains x {D1,D2,D3,D4,D6,D7,D7S} x REPEATS   (run_diagnostic.py)
# + D5 (crash/restart) for the containerized engines            (run_d5_recovery.py)
# + a workload-size scaling sweep on the row-count knob         (run_diagnostic.py)
# + native watermark and incremental Top-N trajectories          (run_native_semantics.py)
# + seven-condition mutation detection and D7S negative control  (in-process runners)
#
# Every cell is one append-only run under runs/<UTC>_<domain>_<engine>_<diag>/;
# nothing here aggregates -- promotion (scripts/results/promote_results.py) and the
# figure/table generators (scripts/figures/) read the runs afterwards.
#
# Knobs (env vars; defaults reproduce the paper matrix):
#   ENGINES    one engine name or all                 (default: all)
#   DOMAINS    one domain name or all                 (default: all)
#   DIAGS      one diagnostic name or all             (default: all)
#   REPEATS    repeats per cell                      (default: runs.repeats from
#                                                     config/default.yaml, i.e. 3)
#   MAX_ROWS   fixture rows per run (0 = full 1M)    (default: 50000)
#   RUN_GRID   run the MAX_ROWS correctness grid      (default: 1)
#   SCALE_ROWS scaling sweep row counts (space-sep,  (default: "10000 50000 200000 500000 1000000")
#              "" disables the scaling section)
#   SCALE_REPEATS repeats at each scaling size        (default: REPEATS)
#   SCALE_ENGINES engines for the scaling sweep      (default: all 7 SUTs)
#   D5_ENGINES engines represented in the recovery section (default: all 7;
#              unsupported engines emit explicit valid capability cells;
#              "" disables D5)
#   D5_ROWS    rows per D5 run                       (default: 20000)
#
# Prereqs:
#   - fixtures built (scripts/perturb/build_fixtures.py)
#   - containerized engines up; for the D5 section use the DURABLE overlay:
#       docker compose -f docker/streaming-engines.yml \
#                      -f docker/streaming-engines.durable.yml up -d
#     and confirm survival first: python scripts/run/verify_persistence.py --engine <e>
#
# Runtime: hours at the defaults (containers dominate). Run it detached:
#   scripts/run/job.sh start matrix bash scripts/run/run_matrix.sh
#   scripts/run/job.sh status matrix
#
# Exit code: 0 iff every section produced scientifically valid artifacts. Engine/query divergences remain FAIL verdicts in summaries but do not abort the benchmark run; blocked or crashed sections do.
# ===========================================================================
set -uo pipefail
ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

# Lane logs are a redirected file, not a tty, so Python block-buffers stdout and
# a cell's verdict only reaches the log once 8KB has accumulated. On a
# low-output lane that can take days, which makes a working lane
# indistinguishable from a wedged one. Unbuffered costs nothing here and keeps
# `job.sh tail`, the stall check in `status`, and a human reading the log all
# honest.
export PYTHONUNBUFFERED=1
# Match benchmark.sh: caller override, else the pinned venv, else python3.
if [ -n "${PYTHON:-}" ]; then
  PY="$PYTHON"
elif [ -x "$ROOT/.venv/bin/python" ]; then
  PY="$ROOT/.venv/bin/python"
else
  PY=python3
fi

ENGINES="${ENGINES:-all}"
DOMAINS="${DOMAINS:-all}"
DIAGS="${DIAGS:-all}"
for selector in "$ENGINES" "$DOMAINS" "$DIAGS"; do
  case "$selector" in
    *" "*) echo "ERROR: ENGINES, DOMAINS, and DIAGS each accept one name or all; got: $selector" >&2; exit 2 ;;
  esac
done
REPEATS="${REPEATS:-$($PY - <<'EOF'
import yaml, pathlib
cfg = yaml.safe_load(pathlib.Path("config/default.yaml").read_text()) or {}
print(int((cfg.get("runs") or {}).get("repeats", 3)))
EOF
)}"
MAX_ROWS="${MAX_ROWS:-50000}"
RUN_GRID="${RUN_GRID:-1}"
SCALE_ROWS="${SCALE_ROWS-10000 50000 200000 500000 1000000}"
SCALE_REPEATS="${SCALE_REPEATS:-$REPEATS}"
ALL_SCALE_ENGINES="reference materialize risingwave ksqldb flink spark feldera"
SCALE_ENGINES="${SCALE_ENGINES:-${ALL_SCALE_ENGINES}}"
# ksqlDB recovers via Kafka-changelog rebuild, so it is in scope. Feldera is not:
# the pinned open-source image gates checkpointing to the enterprise edition, so
# there is nothing to recover and the cell is reported n.a. (scripts/run/run_d5_recovery.py).
D5_ENGINES="${D5_ENGINES-reference materialize risingwave ksqldb flink spark feldera}"
D5_ROWS="${D5_ROWS:-20000}"
NATIVE_ROWS="${NATIVE_ROWS:-20000}"
NATIVE_CHECKPOINTS="${NATIVE_CHECKPOINTS:-4}"
RUN_NATIVE="${RUN_NATIVE:-1}"
MUTATION_ROWS="${MUTATION_ROWS:-50000}"
RUN_MUTATION="${RUN_MUTATION:-1}"
D7S_ROWS="${D7S_ROWS:-200000}"
RUN_D7S_SWEEP="${RUN_D7S_SWEEP:-1}"
MATRIX_ENSURE_SUTS="${MATRIX_ENSURE_SUTS:-1}"
COMPOSE_BASE="docker/streaming-engines.yml"
COMPOSE_DURABLE="docker/streaming-engines.durable.yml"
MATRIX_COMPOSE_ARGS="${MATRIX_COMPOSE_ARGS:--f ${COMPOSE_BASE} -f ${COMPOSE_DURABLE}}"
# One stamp per matrix invocation drives both the ksqlDB service id (scopes its
# metastore) and the run suffix (scopes the ksqlDB adapter's object/topic names,
# read from KSQL_RUN_SUFFIX in src/adapters/ksqldb.py). Deterministic given the
# stamp; unique per invocation so a prior run's Kafka topics can't collide.
MATRIX_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
export EQUITABLE_INVOCATION_ID="${EQUITABLE_INVOCATION_ID:-matrix_${MATRIX_STAMP}}"
export KSQL_SERVICE_ID="${KSQL_SERVICE_ID:-verifiable_stream_matrix_${MATRIX_STAMP}}"
export KSQL_RUN_SUFFIX="${KSQL_RUN_SUFFIX:-matrix_${MATRIX_STAMP}}"

_append_service() {
  case " $1 " in
    *" $2 "*) printf "%s" "$1" ;;
    *) printf "%s" "${1:+$1 }$2" ;;
  esac
}

_engine_services() {
  local arg="$1" services="" e
  if [ "$arg" = "all" ]; then
    echo "materialize risingwave kafka ksqldb feldera flink-jobmanager flink-taskmanager flink-sql-gateway spark-master spark-worker spark-driver"
    return 0
  fi
  for e in $arg; do
    case "$e" in
      materialize) services="$(_append_service "$services" materialize)" ;;
      risingwave)  services="$(_append_service "$services" risingwave)" ;;
      ksqldb)      services="$(_append_service "$services" kafka)"; services="$(_append_service "$services" ksqldb)" ;;
      feldera)     services="$(_append_service "$services" feldera)" ;;
      # Flink reads its input from Kafka; both clusters are scored deployments.
      flink)       for s in kafka flink-jobmanager flink-taskmanager flink-sql-gateway; do
                     services="$(_append_service "$services" "$s")"; done ;;
      spark)       for s in spark-master spark-worker spark-driver; do
                     services="$(_append_service "$services" "$s")"; done ;;
      reference) ;;
      *) ;;
    esac
  done
  echo "$services"
}

_compose() { docker compose $MATRIX_COMPOSE_ARGS "$@"; }

# The readiness probes live in benchmark.sh, so its scoped engine resets wait on
# exactly the same condition this section does. The sentinel keeps the source
# from dispatching a stage or imposing `set -e` on the section loop below.
# shellcheck source=scripts/run/benchmark.sh
EQUITABLE_BENCHMARK_SOURCE_ONLY=1 . "$ROOT/scripts/run/benchmark.sh"

_wait_ready() {
  local tmo="$1" services="$2"
  local deadline=$((SECONDS + tmo)) ok s
  while :; do
    ok=1
    for s in $services; do equitable_probe "$s" || ok=0; done
    if [ "$ok" = 1 ]; then return 0; fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "  TIMEOUT (${tmo}s) waiting for SUT services: ${services}"
      _compose ps || true
      return 1
    fi
    sleep 5
  done
}

_ensure_services() {
  local services="$1"
  [ -z "$services" ] && return 0
  [ "$MATRIX_ENSURE_SUTS" = "0" ] && return 0
  command -v docker >/dev/null 2>&1 || { echo "  ERROR: docker not found for SUT services: $services"; return 1; }
  echo "[ensure] SUT services ready: $services"
  # --no-recreate: ENSURE means start what is not running, never replace what is.
  # A bare `up -d` recreates any container whose config hash has drifted, which
  # mid-campaign throws away a live engine. RisingWave then boots against an
  # already-populated durable volume, fails bootstrap recovery with "worker
  # expired", and exits; ksqlDB goes with it, and the section times out waiting
  # for engines that compose itself just killed. Engine state is reset
  # by benchmark.sh, not as a side effect of readiness.
  _compose up -d --no-recreate $services || return 1
  _wait_ready 300 "$services"
}

run_ready() {
  local services="$1"; shift
  if _ensure_services "$services"; then
    run "$@"
  else
    echo
    echo "FAIL: could not ready SUT services for: $*"
    FAILURES=$((FAILURES + 1))
  fi
}

FAILURES=0
run() {  # run one section command, relaying but not aborting on FAIL
  echo
  echo "------------------------------------------------------------"
  echo "\$ $*"
  echo "------------------------------------------------------------"
  "$@" || FAILURES=$((FAILURES + 1))
}

echo "############################################################"
echo "# Step 7 run matrix"
echo "# engines=$ENGINES domains=$DOMAINS diags=$DIAGS repeats=$REPEATS max_rows=$MAX_ROWS"
echo "# run_grid=$RUN_GRID d5_engines='${D5_ENGINES}' scale_rows='${SCALE_ROWS}' scale_repeats=$SCALE_REPEATS"
echo "# ksql_service_id=${KSQL_SERVICE_ID} ksql_run_suffix=${KSQL_RUN_SUFFIX}"
echo "############################################################"

# --- 7.1 the diagnostic grid, repeated -------------------------------------
if [ "$RUN_GRID" = 1 ]; then
  for rep in $(seq 1 "$REPEATS"); do
    echo
    echo "==================== grid repeat $rep / $REPEATS ===================="
    run_ready "$(_engine_services "$ENGINES")" env EQUITABLE_REPEAT_INDEX="$rep" \
      $PY scripts/run/run_diagnostic.py --sweep --resume-existing \
        --engine "$ENGINES" --domain "$DOMAINS" --diagnostic "$DIAGS" \
        --co-oracle --max-rows "$MAX_ROWS"
  done
else
  echo; echo "[skip] correctness grid disabled (RUN_GRID=$RUN_GRID)"
fi

# --- 7.2 D5 crash/restart (containerized engines; durable overlay) ---------
if [ -n "$D5_ENGINES" ]; then
  for rep in $(seq 1 "$REPEATS"); do
    for eng in $D5_ENGINES; do
      d5_services="$(_engine_services "$eng")"
      case "$eng" in reference|feldera) d5_services="" ;; esac
      # --resume on every engine: an output that outlives the crash (Flink's
      # result topic, Spark's files) reads back intact whether or not state was
      # restored, so only rows ingested after recovery can show a lost state.
      run_ready "$d5_services" env EQUITABLE_REPEAT_INDEX="$rep" \
        $PY scripts/run/run_d5_recovery.py --sweep --resume-existing --resume \
          --engine "$eng" --domain "$DOMAINS" --rows "$D5_ROWS"
    done
  done
else
  echo; echo "[skip] D5 section disabled (D5_ENGINES empty)"
fi

# --- 7.3 workload-size scaling knob ----------------------------------------
# Repeated balanced grid at each requested row count. Table 8 reports verdict
# stability across these size points; collected timings remain internal.
if [ -n "$SCALE_ROWS" ]; then
  for rows in $SCALE_ROWS; do
    if [ "$RUN_GRID" = 1 ] && [ "$rows" = "$MAX_ROWS" ]; then
      continue
    fi
    repeat_var="SCALE_REPEATS_${rows}"
    row_repeats="${!repeat_var:-$SCALE_REPEATS}"
    for rep in $(seq 1 "$row_repeats"); do
      for eng in $SCALE_ENGINES; do
        echo "[scale] rows=$rows repeat=$rep/$row_repeats engine=$eng"
        run_ready "$(_engine_services "$eng")" env EQUITABLE_REPEAT_INDEX="$rep" \
          $PY scripts/run/run_diagnostic.py --sweep --resume-existing \
              --engine "$eng" --domain "$DOMAINS" --diagnostic "$DIAGS" \
              --co-oracle --max-rows "$rows"
      done
    done
  done
else
  echo; echo "[skip] scaling section disabled (SCALE_ROWS empty)"
fi

# --- 7.4 stronger native semantics, separate from portable D4 and D6 --------
if [ "$RUN_NATIVE" = "1" ]; then
  run_ready "$(_engine_services all)" $PY scripts/run/run_native_semantics.py \
      --engine all --domain all --probe all --max-rows "$NATIVE_ROWS" \
      --checkpoints "$NATIVE_CHECKPOINTS" --sweep
else
  echo; echo "[skip] native semantics section disabled"
fi

# --- 7.5 mutation sensitivity, one isolated mutant per condition -----------
if [ "$RUN_MUTATION" = "1" ]; then
  run $PY scripts/run/run_detection_power.py --domain all --max-rows "$MUTATION_ROWS"
else
  echo; echo "[skip] mutation-detection section disabled"
fi

# --- 7.6 D7S sensitivity frontier and genuine failing negative control -----
if [ "$RUN_D7S_SWEEP" = "1" ]; then
  run $PY scripts/run/run_d7s_sweep.py --domain all --max-rows "$D7S_ROWS"
else
  echo; echo "[skip] D7S sweep section disabled"
fi

echo
if [ "$FAILURES" -eq 0 ]; then
  echo "PASS: run matrix complete, every section green. Runs under: runs/"
  exit 0
else
  # Say what a nonzero section actually means. Every section runs under
  # --sweep, so a divergence VERDICT: FAIL never lands here -- that is the
  # measurement working. Reaching this line means cells were BLOCKED: an engine
  # was unreachable, a queue overflowed, a restart raced. Those cells hold no
  # verdict and must be re-run. Calling that "FAILURES" led to a lane being read
  # as a divergent engine when it was a half-started container.
  echo "DONE WITH BLOCKED CELLS: $FAILURES section(s) produced cells with no"
  echo "verdict (artifact_valid=false). Divergences are not counted here -- they"
  echo "are recorded as VERDICT: FAIL and exit zero."
  echo "Find them with: grep -l '\"artifact_valid\": false' runs/*/summary.json"
  exit 1
fi
