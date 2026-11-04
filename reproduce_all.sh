#!/usr/bin/env bash
# ===========================================================================
# reproduce_all.sh -- the single public entry point for the benchmark.
#
# Run `./reproduce_all.sh help` for the short workflow. The scripts under
# scripts/ are internal; users do not invoke them.
#
# Provenance written along the way:
#   - data/normalized/<domain>/manifest.json  (params + input_sha256 + output
#     sha256 + git commit)  -- the per-artifact fingerprint
#   - data/fixtures/<domain>/manifest.json     (base-log digest + seed + rates +
#     per-fixture sha256)  -- the perturbation fingerprint
#   - runs/<UTC>_verify_*/summary.json         -- module checks
#   - runs/<UTC>_oracle_verification_<domain>/summary.json  -- per-domain PASS
# Everything is deterministic: re-running yields byte-identical logs and
# fixtures (compare manifest sha256).
#
# Internal preparation flags (used by the benchmark coordinator):
#   ./reproduce_all.sh --verify     # rebuild data and cross-validate all oracles
#   ./reproduce_all.sh --smoke      # also smoke-test all 7 engine adapters
#                                   #   (auto-starts the SUT containers from
#                                   #    docker/streaming-engines.yml and waits
#                                   #    for each to be ready before smoking)
#   ./reproduce_all.sh --from-frozen-data # validate a bundled offline data snapshot
# Prereqs: Python 3.12.12 with requirements.txt installed, plus Docker for
# engine options and internet for uncached fetches. The
# Backblaze quarterly zip (~1 GB) and Binance monthly trade files (several GB)
# download on first run and are cached under data/raw/.
# ===========================================================================
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
cd "$ROOT"

# Chosen before command dispatch: the background workflow (__complete) calls
# confirm_structural_outliers, which runs "$PY", before any later assignment
# would be reached, and under set -u an unset PY would abort that step.
if [ -n "${PYTHON:-}" ]; then
  PY="$PYTHON"
elif [ -x ".venv/bin/python" ]; then
  PY=".venv/bin/python"
else
  PY="python3"
fi

PUBLIC_ENGINES="reference materialize risingwave ksqldb flink spark feldera"

public_usage() {
  cat <<'EOF'
EquiTable reproduction

  ./reproduce_all.sh                              complete workflow in background
  ./reproduce_all.sh setup                        create the pinned .venv
  ./reproduce_all.sh check-machine                verify the benchmark machine
  ./reproduce_all.sh describe                     print the fixed run counts
  ./reproduce_all.sh data                         rebuild data and verify oracles
  ./reproduce_all.sh check                        validate all seven engines
  ./reproduce_all.sh correctness [engine]         50k rows x 3 repeats
  ./reproduce_all.sh confirmation [engine]        1M rows x 1 repeat
  ./reproduce_all.sh scaling <engine>             five sizes x 3 repeats
  ./reproduce_all.sh results                      collect and compare results
  ./reproduce_all.sh quick [engine]               one domain, 10k rows, reference plus one
                                                  engine (default materialize), under an hour
  ./reproduce_all.sh render                       render the paper's nine tables and three
                                                  figures from the locked results
  ./reproduce_all.sh status [--all]               show job and scientific progress
                                                  (--all also lists earlier runs' jobs)
  ./reproduce_all.sh log [job_name]               follow a background log
  ./reproduce_all.sh stop [job_name]              stop a background job

With no arguments, the complete workflow covers setup, machine checks, data
and oracle checks, engine validation, all 4,795 runs, and result validation.
results/ holds the locked results the paper reports. A complete run compares
its outcomes with that lock, cell by cell, and writes
runs/benchmark_runs/<run id>/reproduction_report.json; it never replaces
the lock. render writes the nine tables the paper prints to tables/ and its
five figures to figures/, from results/ alone; it needs no engine and no
benchmark run.
A benchmark run that finished measuring is finalized, never measured again:
after a fix to collection or comparison code, commit it and re-run.
EQUITABLE_NEW_CAMPAIGN=1 ./reproduce_all.sh discards the current benchmark run.
Every long command starts a resumable background job; use status and log to
monitor it.
EOF
}

phase_name() {
  case "$1" in
    correctness|grid50k) echo grid50k ;;
    confirmation|confirm1m) echo confirm1m ;;
    scaling) echo scaling ;;
    *) echo "ERROR: expected correctness, confirmation, or scaling." >&2; return 2 ;;
  esac
}

benchmark() {
  bash scripts/run/benchmark.sh "$@"
}

validate_public_engine() {
  local engine="$1" allow_all="${2:-0}"
  if [ "$allow_all" = 1 ] && [ "$engine" = all ]; then return 0; fi
  case " $PUBLIC_ENGINES " in
    *" $engine "*) ;;
    *) echo "ERROR: unknown engine: $engine" >&2; return 2 ;;
  esac
}

setup_environment() {
  local bootstrap="python3.12" actual_python expected_python build_constraint
  if [ ! -x "$ROOT/.venv/bin/python" ]; then
    command -v "$bootstrap" >/dev/null 2>&1 || {
      echo "ERROR: install Python 3.12.12 first (python3.12 was not found)." >&2
      return 1
    }
    # Inside a source snapshot .venv is a symlink to the primary checkout's
    # .venv (run_isolated.sh links it before anything has created it), and
    # `python -m venv` refuses a symlink: "Unable to create directory". Build
    # at the resolved path, which is the shared directory.
    "$bootstrap" -m venv "$("$bootstrap" -c \
      'import os, sys; print(os.path.realpath(sys.argv[1]))' "$ROOT/.venv")"
  fi
  actual_python="$("$ROOT/.venv/bin/python" -c 'import platform; print(platform.python_version())')"
  expected_python="$(tr -d '[:space:]' < "$ROOT/.python-version")"
  if [ "$actual_python" != "$expected_python" ]; then
    echo "ERROR: .venv uses Python $actual_python; expected $expected_python." >&2
    echo "Remove .venv and run setup again with Python $expected_python installed." >&2
    return 1
  fi
  "$ROOT/.venv/bin/python" -m pip install --upgrade "pip==25.3"
  build_constraint="$(mktemp "${TMPDIR:-/tmp}/equitable-build.XXXXXX")"
  printf '%s\n' 'setuptools==80.9.0' > "$build_constraint"
  if ! "$ROOT/.venv/bin/python" -m pip install \
      --build-constraint "$build_constraint" \
      --require-hashes -r requirements.txt; then
    rm -f "$build_constraint"
    return 1
  fi
  rm -f "$build_constraint"
  "$ROOT/.venv/bin/python" scripts/verify/verify_environment.py
}

machine_check() {
  local checker="$ROOT/.venv/bin/python" failures=0 server_platform=""
  local java_version="" flock_version=""
  if [ ! -x "$checker" ]; then
    echo "ERROR: run ./reproduce_all.sh setup first." >&2
    failures=1
  else
    "$checker" scripts/verify/verify_environment.py || failures=1
    "$checker" - <<'PY' || failures=1
import os
import platform
import shutil

gib = 1024 ** 3
memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
free = shutil.disk_usage(".").free
cpus = os.cpu_count() or 0
system = platform.system()
machine = platform.machine().lower()
print(f"host: {system} {machine}, {cpus} CPUs, {memory / gib:.1f} GiB RAM, "
      f"{free / gib:.1f} GiB free")
problems = []
if system != "Linux" or machine not in {"x86_64", "amd64"}:
    problems.append("the authoritative full run requires a Linux AMD64 host")
if cpus < 8:
    problems.append("at least 8 logical CPUs are required")
if memory < 32 * gib:
    problems.append("at least 32 GiB RAM is required")
if free < 100 * gib:
    problems.append("at least 100 GiB free disk space is required")
for problem in problems:
    print(f"ERROR: {problem}")
raise SystemExit(1 if problems else 0)
PY
  fi

  if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: Docker is not installed." >&2
    failures=1
  elif ! server_platform="$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}' 2>/dev/null)"; then
    echo "ERROR: the Docker daemon is not available." >&2
    failures=1
  elif [ "$server_platform" != "linux/amd64" ]; then
    echo "ERROR: Docker must run Linux/AMD64 containers; found $server_platform." >&2
    failures=1
  else
    echo "docker: $server_platform"
    docker compose -f docker/streaming-engines.yml \
      -f docker/streaming-engines.durable.yml config -q || failures=1
  fi

  if ! command -v flock >/dev/null 2>&1 ||
     ! flock_version="$(flock --version 2>&1)"; then
    echo "ERROR: util-linux flock is required for atomic benchmark locks." >&2
    failures=1
  else
    printf '%s\n' "$flock_version" | sed -n '1p'
  fi

  if ! command -v java >/dev/null 2>&1 ||
     ! java_version="$(java -version 2>&1)"; then
    echo "ERROR: Java is required by the in-process PyFlink and PySpark deployments." >&2
    failures=1
  else
    printf '%s\n' "$java_version" | sed -n '1p'
  fi

  [ "$failures" -eq 0 ] || {
    echo "FAIL: this machine is not ready for the authoritative benchmark." >&2
    return 1
  }
  echo "PASS: benchmark machine is ready."
}

# Which stage is the workflow in RIGHT NOW.
#
# Derived from the stage markers and the live jobs rather than reported by the
# workflow itself: that way it is correct for a run already in flight, it cannot
# go stale if a job is killed, and it needs no cooperation from the code running
# inside the source snapshot.
current_stage() {
  local dir="runs/benchmark_runs/current" stage="" detail="" marker engine live=""
  local driver_live=0 pid
  pid="$(cat runs/jobs/reproduce_all/pid 2>/dev/null || true)"
  if [ "$(cat runs/jobs/reproduce_all/status 2>/dev/null)" = RUNNING ] &&
     [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    driver_live=1
  fi

  if [ ! -s "$dir/prepare.complete" ]; then
    stage="1/4 data"; detail="init + prepare (5 domains ingesting concurrently)"
  else
    for marker in reset-engines durability smoke validation; do
      if [ ! -s "$dir/$marker.complete" ]; then
        stage="2/4 check"; detail="$marker"; break
      fi
    done
    if [ -z "$stage" ]; then
      for engine in $REQUIRED_LANES; do
        lane_job_live "lane_$engine" && live="$live $engine"
      done
      if [ -n "$live" ]; then
        stage="3/4 engines"; detail="waiting on:$live"
      else
        stage="4/4 results"; detail="collect and compare with the locked results"
      fi
    fi
  fi

  if [ "$driver_live" = 1 ]; then
    printf 'stage: %s -- %s\n' "$stage" "$detail"
  else
    printf 'stage: %s -- %s  [workflow not running]\n' "$stage" "$detail"
  fi
}

run_everything() {
  local env_file="runs/benchmark_runs/current/benchmark.env" saved_commit=""
  local saved_id="" official_id="" finalized="runs/benchmark_runs/current/official-finalize.complete"
  if [ "${EQUITABLE_MACHINE_CHECK_OK:-0}" != 1 ]; then machine_check; fi
  if [ -f "$env_file" ]; then
    saved_commit="$(sed -n 's/^EQUITABLE_SOURCE_COMMIT=//p' "$env_file")"
    saved_id="$(sed -n 's/^EQUITABLE_BENCHMARK_RUN_ID=//p' "$env_file")"
  fi
  if [ -f results/official_results.json ]; then
    official_id="$(.venv/bin/python -c \
      'import json; print(json.load(open("results/official_results.json"))["benchmark_run_id"])')"
  fi

  # ---- stage 1: data -------------------------------------------------------
  # The five domain ingests and their oracle cross-checks already run
  # concurrently inside prepare.
  #
  # A campaign that finished measuring at another commit is finalized, not
  # measured again. A HEAD that changes finalization code only
  # (scripts/results/_finalizer.py) finalizes the measured evidence; one that
  # changes measurement code is refused, because starting over would discard
  # every cell.
  if [ "${EQUITABLE_NEW_CAMPAIGN:-0}" = 1 ] || [ ! -f "$env_file" ] ||
     { [ -n "$official_id" ] && [ "$saved_id" = "$official_id" ] &&
       [ -s "$finalized" ]; }; then
    benchmark init
  elif [ "$saved_commit" != "$(git rev-parse HEAD)" ]; then
    if campaign_measured "$saved_id" "$saved_commit"; then
      if ! "$PY" scripts/results/_finalizer.py check "$saved_commit"; then
        echo "ERROR: benchmark run $saved_id finished measuring and was never finalized." >&2
        echo "Starting over here would discard every one of its cells. Finalize it" >&2
        echo "from a commit whose measurement code matches ${saved_commit:0:12}, or" >&2
        echo "discard it:  EQUITABLE_NEW_CAMPAIGN=1 ./reproduce_all.sh" >&2
        return 1
      fi
      echo "benchmark run $saved_id finished measuring at ${saved_commit:0:12}; finalizing it here"
      finalize_campaign
      return
    fi
    benchmark init
  fi
  if [ ! -s runs/benchmark_runs/current/prepare.complete ]; then
    benchmark prepare
  fi

  # ---- stage 2: gates ------------------------------------------------------
  # reset -> durability -> smoke -> validation. The first three are strictly
  # ordered and cannot be parallelised: smoke needs the engines up, and
  # validation refuses to start before smoke passes (require_stage). Inside
  # validation the boundary check does run concurrently with the
  # native/mutation/D7S matrix -- it is in-process against the reference engine
  # and starts no container, so the two cannot contend.
  if [ ! -s runs/benchmark_runs/current/reset-engines.complete ] ||
     [ ! -s runs/benchmark_runs/current/durability.complete ] ||
     [ ! -s runs/benchmark_runs/current/smoke.complete ] ||
     [ ! -s runs/benchmark_runs/current/validation.complete ]; then
    benchmark check
  fi

  # ---- stage 3: engines ----------------------------------------------------
  launch_engine_lanes
  wait_for_lanes $REQUIRED_LANES || return 1
  require_lanes_complete $REQUIRED_LANES || return 1

  # Re-measure structurally-disagreeing cells before the evidence is locked.
  # The marker ends measurement: campaign_measured requires it, so a later
  # HEAD may finalize this campaign but never skips this step.
  confirm_structural_outliers
  git rev-parse HEAD > runs/benchmark_runs/current/structural-confirmation.complete

  # ---- stage 4: results ----------------------------------------------------
  finalize_campaign
}

# Stage 4 alone: collect one measured campaign and compare it with the lock.
finalize_campaign() {
  # Correctness promises all seven engines. Confirmation and scaling promise the
  # seven minus a DECLARED exclusion; the collectors record it and the promotion
  # gate verifies that the recorded universe matches exactly. Do not narrow a
  # collector to whatever happened to finish first: an undeclared narrowing is
  # rejected, and the first successful finish permanently locks the evidence.
  export BENCHMARK_ENGINES_GRID
  export BENCHMARK_EXCLUDE_CONFIRM BENCHMARK_EXCLUDE_SCALING
  BENCHMARK_ENGINES_GRID="$(echo "$ENGINE_LANES" | tr ' ' ',')"
  BENCHMARK_EXCLUDE_CONFIRM="$(echo "$EXCLUDED_CONFIRM_ENGINES" | tr ' ' ',')"
  BENCHMARK_EXCLUDE_SCALING="$(echo "$EXCLUDED_SCALING_ENGINES" | tr ' ' ',')"
  echo "collecting over -- correctness: $BENCHMARK_ENGINES_GRID"
  echo "                  confirmation: all except $BENCHMARK_EXCLUDE_CONFIRM"
  echo "                  scaling:      all except $BENCHMARK_EXCLUDE_SCALING"
  benchmark finish
}

# One lane per engine, all concurrent, each running correctness, confirmation,
# and the scale sweep in turn. The checkpointed Flink cluster needs about a
# minute per formulation at a million rows, so Flink runs as an ordinary lane
# and every cohort covers all seven engines.
ENGINE_LANES="reference materialize risingwave ksqldb flink spark feldera"
EXCLUDED_CONFIRM_ENGINES="${EXCLUDED_CONFIRM_ENGINES-}"
EXCLUDED_SCALING_ENGINES="${EXCLUDED_SCALING_ENGINES-}"
REQUIRED_LANES="$ENGINE_LANES"

# Has this campaign finished measuring? Every gate passed at its commit, every
# lane it owns finished, and the structural re-measurement ran. Only then may a
# later commit finalize it (run_everything) instead of resuming measurement.
campaign_measured() {
  local run_id="$1" commit="$2" dir="runs/benchmark_runs/current" marker engine
  [ -s "$dir/prepare.complete" ] || return 1
  for marker in reset-engines durability smoke validation structural-confirmation; do
    [ "$(tr -d '[:space:]' < "$dir/$marker.complete" 2>/dev/null)" = "$commit" ] ||
      return 1
  done
  for engine in $REQUIRED_LANES; do
    [ "$(cat "runs/jobs/lane_$engine/owner" 2>/dev/null)" = "$run_id" ] || return 1
    [ "$(cat "runs/jobs/lane_$engine/status" 2>/dev/null)" = DONE ] || return 1
  done
}

# The commit a complete-workflow job runs at. Normally HEAD. A campaign still
# measuring resumes at its own commit when HEAD changes finalization code only:
# measurement at HEAD would fail require_source_snapshot, and a new campaign
# would throw away the cells already measured. It finalizes with that commit's
# code; if that fails, re-running here finalizes with HEAD's.
measuring_commit() {
  local env_file="runs/benchmark_runs/current/benchmark.env" run_id commit head
  head="$(git rev-parse HEAD)"
  if [ "${EQUITABLE_NEW_CAMPAIGN:-0}" != 1 ] && [ -f "$env_file" ] &&
     [ ! -s runs/benchmark_runs/current/official-finalize.complete ]; then
    run_id="$(sed -n 's/^EQUITABLE_BENCHMARK_RUN_ID=//p' "$env_file")"
    commit="$(sed -n 's/^EQUITABLE_SOURCE_COMMIT=//p' "$env_file")"
    if [ "$commit" != "$head" ] && ! campaign_measured "$run_id" "$commit" &&
       "$PY" scripts/results/_finalizer.py check "$commit" >/dev/null 2>&1; then
      echo "benchmark run $run_id is still measuring; resuming it at its own commit ${commit:0:12}" >&2
      head="$commit"
    fi
  fi
  printf '%s\n' "$head"
}

lane_job_live() {
  local pid; pid="$(cat "runs/jobs/$1/pid" 2>/dev/null || true)"
  [ "$(cat "runs/jobs/$1/status" 2>/dev/null)" = RUNNING ] &&
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

# Start a lane unless one from this benchmark run is already going or finished.
# Job names are reused between runs, so a lane left DONE by a previous run would
# otherwise be skipped and its engine silently omitted.
start_lane() {
  local name="$1" owner run_id; shift
  run_id="$(sed -n 's/^EQUITABLE_BENCHMARK_RUN_ID=//p' \
    runs/benchmark_runs/current/benchmark.env 2>/dev/null)"
  owner="$(cat "runs/jobs/$name/owner" 2>/dev/null || true)"
  if [ "$owner" = "$run_id" ]; then
    lane_job_live "$name" && { echo "[skip] $name already running"; return 0; }
    [ "$(cat "runs/jobs/$name/status" 2>/dev/null)" = DONE ] &&
      { echo "[skip] $name already DONE"; return 0; }
  elif lane_job_live "$name"; then
    echo "ERROR: $name is running for a different benchmark run ($owner)." >&2
    return 1
  fi
  scripts/run/job.sh start "$name" "$@" >/dev/null || return 1
  mkdir -p "runs/jobs/$name"
  printf '%s\n' "$run_id" > "runs/jobs/$name/owner"
  echo "[start] $name"
}

launch_lane() {
  local engine="$1" snapshot
  snapshot="$ROOT/runs/source_snapshots/$(git rev-parse HEAD)"
  [ -d "$snapshot" ] || snapshot="$ROOT"
  start_lane "lane_$engine" bash -c \
    "cd '$snapshot' && export MATRIX_ENSURE_SUTS=0 && bash scripts/run/benchmark.sh lane $engine"
}

launch_engine_lanes() {
  local engine
  echo "starting engine lanes (${ENGINE_LANES// /, }), all concurrent; no lane waits on another"
  for engine in $ENGINE_LANES; do
    launch_lane "$engine" || return 1
  done
}

# Lanes check disk only between phases, and one phase can run for a day, so a
# disk that fills mid-phase can take Kafka and then the ksqlDB and RisingWave
# lanes down with no status recorded. The coordinator is the one process awake
# for the whole campaign, so it watches while it waits.
BENCHMARK_MIN_FREE_GIB="${BENCHMARK_MIN_FREE_GIB:-40}"
BENCHMARK_STOP_FREE_GIB="${BENCHMARK_STOP_FREE_GIB:-15}"
BENCHMARK_RECLAIM_COOLDOWN_S="${BENCHMARK_RECLAIM_COOLDOWN_S:-1800}"
last_disk_reclaim=0

free_gib() {
  df -Pk . | awk 'NR==2 {print int($4 / 1048576)}'
}

# Below BENCHMARK_MIN_FREE_GIB, reclaim in two steps, cheapest first.
#
# Step one is Flink's dead retained checkpoints. It runs on every low-disk tick
# with no cooldown because it only removes directories no live job owns, so it
# disturbs nothing. It comes first because docker_flink_checkpoints is the
# volume that grows fastest; resetting Feldera while it fills drains the wrong
# engine.
#
# Step two is Feldera's compiled programs, at most once per cooldown, because it
# is disruptive: a Feldera cell in flight when that happens blocks and its lane
# retries it. Below BENCHMARK_STOP_FREE_GIB even after both, stop every lane
# while `job.sh stop` can still write STOPPED, and fail the workflow.
guard_disk_while_waiting() {
  local free now engine
  free="$(free_gib)"
  [ "$free" -ge "$BENCHMARK_MIN_FREE_GIB" ] && return 0
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] disk: ${free} GiB free, below ${BENCHMARK_MIN_FREE_GIB}; reaping dead Flink checkpoints"
  EQUITABLE_BENCHMARK_SOURCE_ONLY=1 bash -c \
    'source scripts/run/benchmark.sh; reap_flink_checkpoints' || true
  free="$(free_gib)"
  [ "$free" -ge "$BENCHMARK_MIN_FREE_GIB" ] && return 0
  now="$(date +%s)"
  if [ "$((now - last_disk_reclaim))" -ge "$BENCHMARK_RECLAIM_COOLDOWN_S" ]; then
    last_disk_reclaim="$now"
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] disk: ${free} GiB free, still below ${BENCHMARK_MIN_FREE_GIB}; resetting Feldera to reclaim its compiled programs"
    EQUITABLE_BENCHMARK_SOURCE_ONLY=1 bash -c \
      'source scripts/run/benchmark.sh; reset_one_engine_volume feldera' || true
    free="$(free_gib)"
  fi
  [ "$free" -ge "$BENCHMARK_STOP_FREE_GIB" ] && return 0
  echo "ERROR: only ${free} GiB free; stopping every lane before the disk fills." >&2
  for engine in "$@"; do
    lane_job_live "lane_$engine" || continue
    # Cancel before the stop: once the lane's process group is gone there is
    # nothing left that knows which session-cluster jobs belonged to it, and an
    # orphan keeps checkpointing against the disk being saved.
    [ "$engine" = flink ] && EQUITABLE_BENCHMARK_SOURCE_ONLY=1 bash -c \
      'source scripts/run/benchmark.sh; cancel_flink_jobs' || true
    scripts/run/job.sh stop "lane_$engine"
  done
  # The stopped Flink lane leaves its last checkpoints behind; take them now so
  # the operator comes back to free space rather than to a full disk.
  EQUITABLE_BENCHMARK_SOURCE_ONLY=1 bash -c \
    'source scripts/run/benchmark.sh; reap_flink_checkpoints' || true
  echo "Free disk space, then re-run ./reproduce_all.sh; finished cells are kept." >&2
  return 1
}

# A lane exits 3 only after require_free_disk waited out BENCHMARK_DISK_WAIT_S,
# and a stopped lane does not start itself again; without a restart the
# workflow would run on until every other lane finished and then refuse to
# finalize. The coordinator is the one process still awake, so once space is
# back above the floor it restarts the lane, which resumes and re-runs only
# cells with no verdict yet. The cap keeps a lane that hits the floor every
# time from cycling forever; past it the lane stays FAILED and finalize
# refuses. Restarting a job truncates its log, so the previous log is kept
# beside it first.
BENCHMARK_DISK_RESTARTS="${BENCHMARK_DISK_RESTARTS:-3}"
disk_restarts=""

restart_disk_stopped_lanes() {
  local engine dir used done_engine free
  for engine in "$@"; do
    dir="runs/jobs/lane_$engine"
    [ "$(cat "$dir/exit_code" 2>/dev/null)" = 3 ] || continue
    lane_job_live "lane_$engine" && continue
    used=0
    for done_engine in $disk_restarts; do
      [ "$done_engine" = "$engine" ] && used=$((used + 1))
    done
    [ "$used" -lt "$BENCHMARK_DISK_RESTARTS" ] || continue
    free="$(free_gib)"
    if [ "$free" -lt "$BENCHMARK_MIN_FREE_GIB" ]; then
      guard_disk_while_waiting "$@" || return 1
      free="$(free_gib)"
      [ "$free" -ge "$BENCHMARK_MIN_FREE_GIB" ] || continue
    fi
    disk_restarts="$disk_restarts $engine"
    cp "$dir/log.txt" "$dir/log.disk-stop-$((used + 1)).txt" 2>/dev/null || true
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] lane_$engine stopped for disk; ${free} GiB free now, restarting it ($((used + 1))/${BENCHMARK_DISK_RESTARTS}); finished cells are kept"
    launch_lane "$engine" || return 1
  done
}

wait_for_lanes() {
  local engine live waited=0 outstanding
  echo "waiting for required lanes: $*"
  while :; do
    restart_disk_stopped_lanes "$@" || return 1
    live=0
    for engine in "$@"; do lane_job_live "lane_$engine" && live=1; done
    [ "$live" = 0 ] && break
    sleep 60
    waited=$((waited + 60))
    guard_disk_while_waiting "$@" || return 1
    # A coordinator that prints nothing for two days is indistinguishable from a
    # hung one, both to a human and to the stall check in `job.sh status`. Say
    # what is still outstanding every half hour so silence always means trouble.
    if [ "$((waited % 1800))" = 0 ]; then
      outstanding=""
      for engine in "$@"; do
        lane_job_live "lane_$engine" && outstanding="$outstanding $engine"
      done
      echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] still waiting on:$outstanding"
    fi
  done
  echo "lanes finished: $*"
}

# wait_for_lanes returns as soon as no lane is still RUNNING -- and a lane that
# FAILED, was STOPPED, or died leaves the wait exactly like one that finished.
# The very next step is `benchmark finish`, which promotes and locks the
# evidence the first time it succeeds; after that no later run can replace it.
# A lane stopped or killed mid-run would otherwise leave its engine with a
# partial grid and nothing between that and a permanent lock. Locking is
# irreversible, so refuse rather than guess.
require_lanes_complete() {
  local engine status incomplete=""
  for engine in "$@"; do
    status="$(cat "runs/jobs/lane_$engine/status" 2>/dev/null || echo MISSING)"
    [ "$status" = DONE ] || incomplete="${incomplete}  lane_${engine}: ${status}
"
  done
  [ -z "$incomplete" ] && return 0
  echo "ERROR: refusing to finalize -- these lanes did not complete:" >&2
  printf '%s' "$incomplete" >&2
  echo "The first successful finalize LOCKS the results and later runs" >&2
  echo "cannot replace it. Fix the lane, restart it, and re-run the workflow." >&2
  return 1
}

# A cell that scored a verdict against an engine mid-reset is indistinguishable
# from a real divergence once results are collected, and `benchmark finish` locks
# the evidence permanently. The watchdog TERMs the matrix shell, bash does not
# pass that to the python diagnostic underneath, and an orphaned diagnostic
# would keep writing summaries while the lane wiped the engine's volume out
# from under it. equitable_reap_engine_workers stops those workers before the
# reset, so this should find nothing on a fresh run.
#
# It stays because "should" is not "does". Structural disagreement -- a key the
# other side never had, or a duplicated key -- is the shape engine-state damage
# takes, and it is rare. Re-measuring the few cells that show it costs minutes
# and is the difference between locking a wrong result and locking a confirmed
# one. A genuine structural divergence
# reproduces and is confirmed; a damaged cell is replaced, because
# benchmark_status keys a cell by (phase, engine, domain, diagnostic, rows,
# repeat) and keeps the highest run id. Nothing is deleted and every
# re-measurement is printed.
#
# The cap is a circuit breaker. If structural disagreement is widespread it is a
# property of the run rather than damage to a few cells, and re-running it would
# be futile and a way to launder a systematic problem into a clean-looking
# table. Above the cap this reports and changes nothing.
CONFIRM_STRUCTURAL_CAP="${CONFIRM_STRUCTURAL_CAP:-25}"

confirm_structural_outliers() {
  local specs total=0 done_n=0
  specs="$("$PY" - "$CONFIRM_STRUCTURAL_CAP" <<'PYEOF'
import json, os, sys, glob

cap = int(sys.argv[1])
run_id = ""
try:
    for line in open(os.path.join("runs", "benchmark_runs", "current",
                                  "benchmark.env")):
        if line.startswith("EQUITABLE_BENCHMARK_RUN_ID="):
            run_id = line.strip().split("=", 1)[1]
except OSError:
    pass

latest = {}
for path in glob.glob(os.path.join("runs", "*", "summary.json")):
    try:
        s = json.load(open(path))
    except (OSError, ValueError):
        continue
    if not isinstance(s, dict) or s.get("campaign_id") != run_id:
        continue
    if s.get("artifact_valid") is not True or s.get("error"):
        continue
    rows_value = s.get("requested_max_rows", s.get("rows"))
    # A sweep's aggregate summary carries a LIST of row counts. It is not a cell
    # and cannot be re-measured as one, so it is not a candidate; the individual
    # cells behind it are examined on their own.
    if not isinstance(rows_value, int):
        continue
    key = (s.get("campaign_phase"), s.get("engine"), s.get("domain"),
           s.get("diagnostic"), rows_value, s.get("repeat_index", 0))
    run = os.path.basename(os.path.dirname(path))
    # Same tie-break as benchmark_status: the highest run id is the live one, so
    # a cell already superseded by a clean re-run is not examined again.
    if key not in latest or run > latest[key][0]:
        latest[key] = (run, s)


def structural(summary):
    for block in ("primary", "recovery"):
        b = summary.get(block)
        if not isinstance(b, dict):
            continue
        for level in ("hourly", "daily"):
            c = b.get(level)
            if not isinstance(c, dict):
                continue
            if (int(c.get("only_in_online") or 0)
                    or int(c.get("only_in_oracle") or 0)
                    or int(c.get("duplicate_online_keys") or 0)
                    or int(c.get("duplicate_oracle_keys") or 0)):
                return True
            if int((c.get("non_equivalence") or {}).get(
                    "structural_violations") or 0):
                return True
    return False


rows = sorted((r for r in latest.values() if structural(r[1])),
              key=lambda r: r[0])
if len(rows) > cap:
    sys.stderr.write("  structural disagreement in %d cells exceeds the cap of "
                     "%d; reporting without re-measuring.\n" % (len(rows), cap))
    raise SystemExit(0)
for run, s in rows:
    print("\t".join(str(x) for x in (
        s.get("invocation_id") or "", s.get("campaign_id") or "",
        s.get("campaign_phase") or "", s.get("engine") or "",
        s.get("domain") or "", s.get("diagnostic") or "",
        s.get("requested_max_rows", s.get("rows")) or 0,
        s.get("repeat_index", 0), run)))

PYEOF
)" || return 0

  # Empty covers both "nothing disagreed structurally" and "the cap declined to
  # act", which the detector has already explained on stderr.
  [ -n "$specs" ] || { echo "structural confirmation: no cells re-measured"; return 0; }
  total="$(printf '%s\n' "$specs" | wc -l)"
  echo "structural confirmation: re-measuring $total cell(s) that disagree structurally"
  echo "  (a real divergence reproduces and is confirmed; a cell damaged by"
  echo "   engine state is replaced -- nothing is deleted)"
  while IFS="$(printf '\t')" read -r inv cid phase engine domain diag rows rep run; do
    [ -n "$engine" ] || continue
    echo "  re-measuring $phase/$engine/$domain/$diag rows=$rows repeat=$rep (was $run)"
    EQUITABLE_CAMPAIGN_ID="$cid" \
    EQUITABLE_CAMPAIGN_PHASE="$phase" \
    EQUITABLE_INVOCATION_ID="$inv" \
    EQUITABLE_REPEAT_INDEX="$rep" \
      "$PY" scripts/run/run_diagnostic.py --sweep --engine "$engine" \
        --domain "$domain" --diagnostic "$diag" --co-oracle --max-rows "$rows" \
      || echo "  WARNING: re-measuring $engine/$domain/$diag exited nonzero; the" \
              "original result stands and is reported as it was recorded."
    done_n=$((done_n + 1))
  done <<EOSPECS
$specs
EOSPECS
  echo "structural confirmation: re-measured $done_n cell(s)"
}

run_complete_workflow() {
  setup_environment
  machine_check
  benchmark describe
  EQUITABLE_MACHINE_CHECK_OK=1 run_everything
}

run_data_stage() {
  local env_file="runs/benchmark_runs/current/benchmark.env" saved_commit=""
  setup_environment
  if [ -f "$env_file" ]; then
    saved_commit="$(sed -n 's/^EQUITABLE_SOURCE_COMMIT=//p' "$env_file")"
  fi
  if [ ! -f "$env_file" ] || [ "$saved_commit" != "$(git rev-parse HEAD)" ]; then
    benchmark init
  fi
  benchmark prepare
}

run_engine_check_stage() {
  setup_environment
  machine_check
  benchmark check
}

run_scientific_stage() {
  local stage="$1" engine="$2" phase
  setup_environment
  machine_check
  phase="$(phase_name "$stage")"
  benchmark run "$phase" "$engine"
}

run_results_stage() {
  setup_environment
  benchmark finish
}

start_background() {
  local name="$1" checker="python3" job_dir pid status
  shift
  for job_dir in "$ROOT"/runs/jobs/*/; do
    [ -d "$job_dir" ] || continue
    status="$(cat "$job_dir/status" 2>/dev/null || true)"
    pid="$(cat "$job_dir/pid" 2>/dev/null || true)"
    if [ "$status" = "RUNNING" ] && [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      echo "ERROR: another benchmark job is active: $(basename "$job_dir") (pid $pid)" >&2
      echo "Wait for it or stop it with ./reproduce_all.sh stop $(basename "$job_dir")." >&2
      return 1
    fi
  done
  if [ -x "$ROOT/.venv/bin/python" ]; then checker="$ROOT/.venv/bin/python"; fi
  "$checker" -c \
    'from src.provenance import source_dirty; raise SystemExit(1 if source_dirty() else 0)' || {
      echo "ERROR: commit source and configuration changes before starting." >&2
      exit 1
    }
  EQUITABLE_LAUNCH_COMMIT="${LAUNCH_COMMIT:-$(git rev-parse HEAD)}" \
    scripts/run/job.sh start "$name" bash scripts/run/run_isolated.sh "$@"
}

# Render the paper's nine tables (LaTeX fragments) and five figures (PNG) from
# the locked evidence under results/. Reads results/, config/, src/ and
# data/incidents.yaml only; needs no engine, no network and no benchmark run.
# Tables go to tables/, figures to figures/, both at the repository root.
run_render_stage() {
  local py="${EQUITABLE_PYTHON:-$ROOT/.venv/bin/python}" generator
  if [ ! -x "$py" ]; then
    echo "ERROR: run ./reproduce_all.sh setup first." >&2
    return 1
  fi
  "$py" scripts/results/verify_promotions.py --require-lock || return 1
  export EQUITABLE_TABLES_DIR="$ROOT/tables"
  export EQUITABLE_FIGURES_DIR="$ROOT/figures"
  export EQUITABLE_FIGURE_FORMATS="png"
  for generator in \
      tab_01_related_work tab_02_domains tab_03_schema tab_04_components \
      tab_08_scaling tab_11_witnesses tab_12_validation tab_21_root_causes; do
    "$py" "scripts/figures/${generator}.py" || return 1
  done
  "$py" scripts/figures/results_experiments.py --tables-only || return 1
  for generator in \
      fig_02_design_space \
      fig_05_attribution fig_06_disorder; do
    "$py" "scripts/figures/${generator}.py" || return 1
  done
  echo "tables: $(ls "$ROOT/tables"/tab_*.tex | wc -l) LaTeX fragments under tables/"
  echo "figures: $(ls "$ROOT/figures"/fig_*.png | wc -l) PNG files under figures/"
  echo "docs/RESULTS.md maps each file to its table or figure in the paper."
}

# Quick path: one real domain (taxi, June 2024), 10,000 rows, the
# in-process reference backend plus one Docker engine, the seven portable
# diagnostics, both oracles cross-checked. It downloads about 60 MB, builds the
# taxi fixtures, and prints one verdict per cell. Under an hour on a laptop;
# a few minutes once the data exists. It writes under runs/quick_* only and
# never touches results/ or a benchmark run.
run_quick_path() {
  local engine="${1:-materialize}" py="${EQUITABLE_PYTHON:-$ROOT/.venv/bin/python}"
  local services="" invocation manifest log
  if [ ! -x "$py" ]; then
    echo "ERROR: run ./reproduce_all.sh setup first." >&2
    return 1
  fi
  case "$engine" in
    reference) services="" ;;
    materialize) services="materialize" ;;
    risingwave) services="risingwave" ;;
    ksqldb) services="kafka ksqldb" ;;
    flink) services="kafka flink-jobmanager flink-taskmanager flink-sql-gateway" ;;
    spark) services="spark-master spark-worker spark-driver" ;;
    feldera) services="feldera" ;;
    *) echo "ERROR: quick accepts reference, materialize, risingwave, ksqldb, flink, spark or feldera." >&2
       return 2 ;;
  esac
  log="data/normalized/transportation_taxi/log.parquet"
  manifest="data/normalized/transportation_taxi/manifest.json"
  echo "[1/4] taxi log: download June 2024 from the TLC archive and normalize 1,000,000 rows"
  if [ -f "$log" ]; then
    echo "      already present, skipped"
  else
    "$py" scripts/ingest/ingest_transportation_taxi.py --from-raw \
      --start 2024-06 --end 2024-06 || return 1
  fi
  "$py" - "$log" "$manifest" <<'PY' || return 1
import hashlib, json, sys
log, manifest = sys.argv[1], sys.argv[2]
want = json.load(open(manifest))["sha256"]
have = hashlib.sha256(open(log, "rb").read()).hexdigest()
if have != want:
    print(f"ERROR: {log} has sha256 {have}, the tracked manifest records {want}")
    raise SystemExit(1)
print(f"      sha256 matches the tracked manifest ({want[:12]}...)")
PY
  echo "[2/4] taxi fixtures: D1 duplicates, D2 reorder, D4 late rows, D7 restatements"
  if [ -f data/fixtures/transportation_taxi/d1_dup.parquet ]; then
    echo "      already present, skipped"
  else
    "$py" scripts/perturb/build_fixtures.py --domain transportation_taxi || return 1
  fi
  # The ingest and fixture scripts rewrite the tracked manifests with the
  # current commit and time. The checksums are what matters: compare them
  # with the committed manifests, then restore the committed files so the
  # quick path leaves the tree clean.
  if git -C "$ROOT" rev-parse --verify HEAD >/dev/null 2>&1; then
    "$py" - "$ROOT" <<'PY' || return 1
import json, subprocess, sys
root = sys.argv[1]
paths = ["data/normalized/transportation_taxi/manifest.json",
         "data/fixtures/transportation_taxi/manifest.json"]
def committed(path):
    return json.loads(subprocess.check_output(["git", "-C", root, "show", f"HEAD:{path}"]))
now_log, was_log = json.load(open(f"{root}/{paths[0]}")), committed(paths[0])
now_fx, was_fx = json.load(open(f"{root}/{paths[1]}")), committed(paths[1])
if now_log["sha256"] != was_log["sha256"]:
    print("ERROR: the normalized log differs from the committed manifest")
    raise SystemExit(1)
def digests(manifest):
    fixtures = manifest["fixtures"]
    if isinstance(fixtures, dict):
        return {name: entry.get("sha256") for name, entry in fixtures.items()}
    return {entry["name"]: entry.get("sha256") for entry in fixtures}
if digests(now_fx) != digests(was_fx):
    print("ERROR: a fixture differs from the committed manifest")
    print("  built:    ", digests(now_fx))
    print("  committed:", digests(was_fx))
    raise SystemExit(1)
print("      fixture checksums match the committed manifest")
subprocess.check_call(["git", "-C", root, "checkout", "--"] + paths)
PY
  fi
  if [ -n "$services" ]; then
    echo "[3/4] starting $engine with Docker Compose"
    docker compose -f docker/streaming-engines.yml up -d --wait $services || return 1
  else
    echo "[3/4] reference backend runs in process; no container to start"
  fi
  echo "[4/4] seven portable diagnostics at 10,000 rows, both oracles cross-checked"
  invocation="quick_$(date -u +%Y%m%dT%H%M%SZ)"
  local engines="reference"
  [ "$engine" = reference ] || engines="reference $engine"
  for e in $engines; do
    EQUITABLE_INVOCATION_ID="$invocation" "$py" -u scripts/run/run_diagnostic.py \
      --engine "$e" --domain transportation_taxi --diagnostic core \
      --co-oracle --max-rows 10000 --sweep || return 1
  done
  "$py" scripts/run/pilot_report.py --invocation "$invocation"
  echo "States: equivalent means the engine's table matched the oracle; divergent means"
  echo "it did not; unsupported means the engine cannot express that query."
  echo "The locked verdicts for these cells are in results/official_results.json;"
  echo "docs/RESULTS.md explains how to read them."
}

# Public commands. The older --flags below are private preparation stages used
# by scripts/run/benchmark.sh; keeping them here avoids a second public driver.
case "${1:-__default}" in
  help|-h|--help) public_usage; exit 0 ;;
  __default)
    LAUNCH_COMMIT="$(measuring_commit)"
    start_background reproduce_all __complete
    exit $?
    ;;
  setup) setup_environment; exit $? ;;
  check-machine) machine_check; exit $? ;;
  describe) benchmark describe; exit $? ;;
  data) start_background data __data; exit $? ;;
  check) start_background check __check; exit $? ;;
  correctness|confirmation)
    engine="${2:-all}"
    validate_public_engine "$engine" 1
    start_background "${1}_${engine}" __scientific "$1" "$engine"
    exit $?
    ;;
  scaling)
    engine="${2:-}"
    [ -n "$engine" ] || { echo "ERROR: scaling requires one engine." >&2; exit 2; }
    validate_public_engine "$engine"
    start_background "scaling_${engine}" __scientific scaling "$engine"
    exit $?
    ;;
  results) start_background results __results; exit $? ;;
  quick) run_quick_path "${2:-}"; exit $? ;;
  render) run_render_stage; exit $? ;;
  status)
    case "${2:-}" in
      ""|--all) ;;
      *) echo "ERROR: status accepts only --all." >&2; exit 2 ;;
    esac
    if [ -f runs/benchmark_runs/current/benchmark.env ]; then
      current_stage
      benchmark status
    fi
    scripts/run/job.sh status "${2:-}"
    exit $?
    ;;
  log) scripts/run/job.sh tail "${2:-reproduce_all}"; exit $? ;;
  stop) scripts/run/job.sh stop "${2:-reproduce_all}"; exit $? ;;
  __complete) run_complete_workflow; exit $? ;;
  __data) run_data_stage; exit $? ;;
  __check) run_engine_check_stage; exit $? ;;
  __scientific) run_scientific_stage "${2:-}" "${3:-}"; exit $? ;;
  __results) run_results_stage; exit $? ;;
  --*) ;;
  *) echo "ERROR: unknown command: $1" >&2; public_usage; exit 2 ;;
esac

export EQUITABLE_SOURCE_COMMIT="$(git rev-parse HEAD)"
export EQUITABLE_SOURCE_CLEAN=1
if ! python3 -c "from src.provenance import git_dirty; raise SystemExit(1 if git_dirty() else 0)"; then
  echo "ERROR: authoritative reproduction must start from a clean source tree." >&2
  echo "Generated data/results outputs are allowed; source/config/lock changes are not." >&2
  git status --short >&2
  exit 1
fi
REPRO_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
export EQUITABLE_INVOCATION_ID="${EQUITABLE_INVOCATION_ID:-reproduce_${REPRO_STAMP}}"
if [ -n "${PYTHON:-}" ]; then
  PY="$PYTHON"
elif [ -x ".venv/bin/python" ]; then
  PY=".venv/bin/python"
else
  PY="python3"
fi

WANT_VERIFY=0; WANT_SMOKE=0; FROM_FROZEN_DATA=0
for arg in "$@"; do
  case "$arg" in
    --verify)  WANT_VERIFY=1 ;;
    --smoke)   WANT_SMOKE=1 ;;
    --from-frozen-data) FROM_FROZEN_DATA=1 ;;
    *) echo "unknown flag: $arg"
       echo "internal usage: ./reproduce_all.sh [--verify] [--smoke] [--from-frozen-data]"
       exit 1 ;;
  esac
done

# ===== Pinned parameters (the base-dataset definition) =====================
AIRQ_START="2024-06-01"; AIRQ_END="2024-07-01"          # environment_airquality
TAXI_MONTH="2024-06"                                     # transportation_taxi
DRIVES_MONTH="2024-06"                                   # datacenter_drives
DRIVES_ZIP="data/raw/datacenter_drives/data_Q2_2024.zip"
DRIVES_URL="https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q2_2024.zip"
FIN_MONTH="2024-06"                                      # finance_markets
FIN_SYMBOLS="BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT ADAUSDT"
FOOTBALL_GAME="1"                                        # sports_football
DATA_DOMAINS="environment_airquality transportation_taxi datacenter_drives finance_markets sports_football"
# ===========================================================================

# ===== Container helpers used by the internal smoke stage ===================
# The SUT stack lives in docker/streaming-engines.yml (project name "docker",
# so containers are docker-<svc>-1). Readiness is decided by probing each
# engine's HOST endpoint -- exactly what the adapters connect to -- NOT the
# compose healthcheck: risingwave's healthcheck shells out to `psql`, which its
# image doesn't ship, so it sits "(unhealthy)" forever while actually serving on
# :4566. Probes: materialize :6877, risingwave :4566, kafka :19092 (TCP wire),
# feldera :8080/healthz, ksqldb :8088/info (HTTP). Callers set _COMPOSE_ARGS
# (which -f files) before invoking these.
COMPOSE_BASE="docker/streaming-engines.yml"
_COMPOSE_ARGS="-f ${COMPOSE_BASE}"

_require_docker() {
  command -v docker >/dev/null 2>&1 && return 0
  echo "  ERROR: docker not found -- install Docker to run the containerized engines." >&2
  return 1
}

_compose() { docker compose $_COMPOSE_ARGS "$@"; }

_probe() {  # $1=service -> exit 0 iff its host endpoint is serving
  case "$1" in
    materialize) timeout 2 bash -c '</dev/tcp/localhost/6877'  2>/dev/null ;;
    risingwave)  timeout 2 bash -c '</dev/tcp/localhost/4566'  2>/dev/null ;;
    kafka)       timeout 2 bash -c '</dev/tcp/localhost/19092' 2>/dev/null ;;
    feldera)     curl -fsS -o /dev/null http://localhost:8080/healthz 2>/dev/null ;;
    ksqldb)      curl -fsS -o /dev/null http://localhost:8088/info    2>/dev/null ;;
    flink-jobmanager)  curl -fsS http://localhost:8081/overview 2>/dev/null | grep -q '"taskmanagers":[1-9]' ;;
    flink-sql-gateway) curl -fsS -o /dev/null http://localhost:8083/v2/info 2>/dev/null ;;
    spark-driver)      curl -fsS http://localhost:8090/status 2>/dev/null | grep -q '"ready": true' ;;
    *)           return 0 ;;
  esac || return 1
  # An open listener is not liveness: an engine whose internal services have
  # died can keep accepting connections, so require a real query from every
  # engine for which engine_probe.py has one.
  # Wrapped in `timeout` so a probe that blocks is a failed probe, not a
  # startup that never returns: a wedged engine may not enforce its own
  # statement_timeout, which is how it got to be wedged.
  timeout 25 "$PY" "$ROOT/scripts/run/engine_probe.py" \
    --engine "$1" --timeout 10 >/dev/null 2>&1
}

# _wait_ready <timeout_s> "<services>"  -- block until every named service's host
# endpoint answers, or 1 on timeout (prints status). Never trips `set -e` mid-poll.
_wait_ready() {
  local tmo="$1" services="$2"
  local deadline=$((SECONDS + tmo)) ok s
  echo "waiting for containers to become ready (timeout ${tmo}s) ..."
  while :; do
    ok=1
    for s in $services; do _probe "$s" || ok=0; done
    if [ "$ok" = 1 ]; then echo "  containers ready: ${services}."; return 0; fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "  TIMEOUT (${tmo}s) waiting for: ${services}. Status:"; _compose ps
      echo "  Logs of a stuck service:  docker compose ${_COMPOSE_ARGS} logs <service>"
      return 1
    fi
    sleep 5
  done
}

echo "Validating pinned reproduction environment with $PY ..."
$PY scripts/verify/verify_environment.py


echo "############################################################"
echo "# Module verification -- schema/engine/oracle"
echo "############################################################"
# Prove the measuring instruments themselves are correct before trusting them:
# the neutral schema generalizes across layouts, and engine == pandas == DuckDB
# on a synthetic log.
$PY scripts/verify/verify_schema.py
$PY scripts/verify/verify_engine_oracle.py
# Capability claims get measured too, not just numeric ones. Spark's D7 cell is
# reported as "cannot express the view", which is a negative about a shipping
# system, so the run re-derives it instead of trusting a code comment: the probe
# builds the restate view four ways and records what Spark does with each.
# Exits non-zero if any streaming formulation ever starts working, which would
# make the reported D7 cell for Spark wrong -- stopping the run is the point.
# Set EQUITABLE_SKIP_SPARK_RESTATE=1 to skip it on a machine without pyspark.
if [ "${EQUITABLE_SKIP_SPARK_RESTATE:-0}" = 0 ]; then
  $PY scripts/run/verify_spark_restate.py
else
  echo "skipping Spark D7 capability probe (EQUITABLE_SKIP_SPARK_RESTATE=1)"
fi

if [ "$FROM_FROZEN_DATA" = 0 ]; then
echo
echo "############################################################"
echo "# Normalized domain logs -- regenerate all 5 base workloads"
echo "############################################################"
# The five domains share no state: each reads its own raw input and writes only
# data/normalized/<domain>/. Running them concurrently changes nothing about the
# artifacts -- each ingest is deterministic given (raw input, pinned params) --
# so the manifests and their sha256 digests are identical either way. Output is
# captured per domain and replayed in a fixed order so the log stays readable
# and reviewable rather than interleaved.
echo "==== ingesting all 5 domains concurrently ===="
_ing_log="$(mktemp -d "${TMPDIR:-/tmp}/equitable-ingest.XXXXXX")"
$PY scripts/ingest/ingest_environment_airquality.py --from-raw \
  --start "$AIRQ_START" --end "$AIRQ_END" \
  > "$_ing_log/environment_airquality" 2>&1 &
_ing_airq=$!
$PY scripts/ingest/ingest_transportation_taxi.py --from-raw \
  --start "$TAXI_MONTH" --end "$TAXI_MONTH" \
  > "$_ing_log/transportation_taxi" 2>&1 &
_ing_taxi=$!
if [ -f "$DRIVES_ZIP" ]; then
  $PY scripts/ingest/ingest_datacenter_drives.py --from-zip "$DRIVES_ZIP" \
    --month "$DRIVES_MONTH" > "$_ing_log/datacenter_drives" 2>&1 &
else
  $PY scripts/ingest/ingest_datacenter_drives.py --url "$DRIVES_URL" \
    --month "$DRIVES_MONTH" > "$_ing_log/datacenter_drives" 2>&1 &
fi
_ing_drives=$!
$PY scripts/ingest/ingest_finance_markets.py --month "$FIN_MONTH" \
  --symbols $FIN_SYMBOLS > "$_ing_log/finance_markets" 2>&1 &
_ing_fin=$!
$PY scripts/ingest/ingest_sports_football.py --game "$FOOTBALL_GAME" \
  > "$_ing_log/sports_football" 2>&1 &
_ing_ball=$!

_ing_failed=0
for _pair in "environment_airquality:$_ing_airq" "transportation_taxi:$_ing_taxi" \
             "datacenter_drives:$_ing_drives" "finance_markets:$_ing_fin" \
             "sports_football:$_ing_ball"; do
  _d="${_pair%%:*}"; _pid="${_pair##*:}"
  if wait "$_pid"; then _mark="OK"; else _mark="FAILED"; _ing_failed=1; fi
  echo "==== $_d ($_mark) ===="
  cat "$_ing_log/$_d" || true
done
rm -rf "$_ing_log"
if [ "$_ing_failed" != 0 ]; then
  echo "ERROR: at least one domain ingest failed; see the sections above." >&2
  exit 1
fi

echo
echo "############################################################"
echo "# Perturbation fixtures -- rate-based, one set per domain"
echo "############################################################"
# Perturb each base log into the per-diagnostic replay logs (D1 duplicates,
# D2 bounded reorder, D4 late) under data/fixtures/<domain>/, with rates read
# from config/default.yaml. Deterministic: (base-log digest, seed, rates) ->
# byte-identical parquet.
$PY scripts/perturb/build_fixtures.py
else
  echo "Using bundled normalized logs and fixtures; regeneration from raw inputs is skipped."
fi

echo "# Validate base/fixture hashes, schema, source IDs, order, and finite values"
snapshot_args=()
if [ -n "${EQUITABLE_DATA_SNAPSHOT:-}" ]; then
  snapshot_args=(--snapshot "$EQUITABLE_DATA_SNAPSHOT")
fi
$PY scripts/verify/verify_data_artifacts.py --fixtures "${snapshot_args[@]}"

if [ "$WANT_VERIFY" = 1 ]; then
  echo
  echo "############################################################"
  echo "# Oracle cross-validation -- engine == pandas == DuckDB, all 5"
  echo "############################################################"
  # Each domain's cross-validation reads that domain's log and writes its own
  # runs/<UTC>_oracle_verification_<domain>/ directory, so the five are
  # independent. The oracle cache is safe under concurrency: build_or_load_oracle
  # takes an exclusive flock on the cache key before writing (src/oracle/cache.py).
  # This stage reports PASS/FAIL verdicts and makes no timing claim, so
  # contention cannot affect what it concludes.
  _ver_log="$(mktemp -d "${TMPDIR:-/tmp}/equitable-verify.XXXXXX")"
  _ver_pids=""
  for d in $DATA_DOMAINS; do
    if [ -n "${EQUITABLE_INVOCATION_ID:-}" ] &&
       $PY scripts/run/find_collection.py --kind oracle_verification \
         --invocation-id "$EQUITABLE_INVOCATION_ID" --domain "$d" \
         >/dev/null 2>&1; then
      echo "reusing completed oracle verification for $d" > "$_ver_log/$d"
      ( exit 0 ) &
    else
      $PY scripts/run/run_oracle_verification.py --domain "$d" \
        > "$_ver_log/$d" 2>&1 &
    fi
    _ver_pids="$_ver_pids $d:$!"
  done

  _ver_failed=0
  for _pair in $_ver_pids; do
    _d="${_pair%%:*}"; _pid="${_pair##*:}"
    if wait "$_pid"; then _mark="PASS"; else _mark="FAILED"; _ver_failed=1; fi
    echo "==== verify $_d ($_mark) ===="
    cat "$_ver_log/$_d" || true
  done
  rm -rf "$_ver_log"
  if [ "$_ver_failed" != 0 ]; then
    echo "ERROR: oracle cross-validation failed for at least one domain." >&2
    exit 1
  fi
fi

if [ "$WANT_SMOKE" = 1 ]; then
  echo
  echo "############################################################"
  echo "# Adapter smoke tests -- all 7 SUTs on the air-quality slice"
  echo "############################################################"
  # Self-sufficient: bring up the SUTs and BLOCK until each is ready before
  # smoking, so `--smoke` never races a half-started engine into a
  # connection-refused FAIL. Flink and Spark run as clusters in the same compose
  # file. Images are digest-pinned (config/engines/pinned.yaml).
  _require_docker || { echo "  or smoke the in-process reference only: $PY scripts/adapters/smoke.py --engine reference"; exit 1; }
  _COMPOSE_ARGS="-f ${COMPOSE_BASE}"
  _smoke_services="materialize risingwave kafka ksqldb feldera flink-jobmanager flink-taskmanager flink-sql-gateway spark-master spark-worker spark-driver"
  echo "starting SUT containers: $_smoke_services"
  _compose up -d $_smoke_services
  _wait_ready 300 "$_smoke_services" || exit 1

  $PY scripts/adapters/smoke.py --all
fi

echo
echo "DONE. Base logs: data/normalized/<domain>/ ; fixtures: data/fixtures/<domain>/ ; results: runs/"
echo "Reproducibility check: re-run and diff the sha256 fields in"
echo "  data/normalized/*/manifest.json and data/fixtures/*/manifest.json."
