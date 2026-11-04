#!/usr/bin/env bash
# Run one fixed, fully traceable benchmark execution.
#
# run_matrix.sh sources this file for the SUT readiness probes below. Guard the
# shell options so sourcing cannot impose `set -e` on a caller that deliberately
# tolerates section failures, and see the sentinel above the dispatch at the end.
if [ -z "${EQUITABLE_BENCHMARK_SOURCE_ONLY:-}" ]; then
  set -euo pipefail
fi

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"
if [ -n "${PYTHON:-}" ]; then
  PY="$PYTHON"
elif [ -x "$ROOT/.venv/bin/python" ]; then
  PY="$ROOT/.venv/bin/python"
else
  PY=python3
fi
# --- SUT readiness ---------------------------------------------------------
# Probe the wire protocol the adapter actually speaks, not the compose
# healthcheck: the pinned RisingWave image ships no psql, so its healthcheck
# reported `unhealthy` for an entire benchmark run while the engine was serving
# fine. Both this file and run_matrix.sh wait on exactly this condition -- an
# adapter that connects during a restart window gets "Connection reset by peer"
# and the cell is recorded with artifact_valid=false, blocking it for the whole
# run instead of producing a verdict.
equitable_probe() {
  # Gate 1 -- is anything holding the port open at all? Cheap, and catches a
  # stopped or restarting container without paying for a client connection.
  case "$1" in
    materialize) timeout 2 bash -c '</dev/tcp/localhost/6877'  2>/dev/null || return 1 ;;
    risingwave)  timeout 2 bash -c '</dev/tcp/localhost/4566'  2>/dev/null || return 1 ;;
    kafka)       timeout 2 bash -c '</dev/tcp/localhost/19092' 2>/dev/null || return 1 ;;
    feldera)     curl -fsS -o /dev/null http://localhost:8080/healthz 2>/dev/null || return 1 ;;
    ksqldb)      curl -fsS -o /dev/null http://localhost:8088/info    2>/dev/null || return 1 ;;
    # The Flink and Spark clusters answer over HTTP, and each answer is itself
    # the serving check: a JobManager with a registered TaskManager, a gateway
    # that reports its version, a Spark driver whose control service is ready.
    flink-jobmanager)
      curl -fsS http://localhost:8081/overview 2>/dev/null \
        | grep -q '"taskmanagers":[1-9]' || return 1
      return 0 ;;
    flink-sql-gateway)
      curl -fsS -o /dev/null http://localhost:8083/v2/info 2>/dev/null || return 1
      return 0 ;;
    spark-driver)
      curl -fsS http://localhost:8090/status 2>/dev/null \
        | grep -q '"ready": true' || return 1
      return 0 ;;
    *)           return 0 ;;
  esac
  # Gate 2 -- can it still answer a statement? An open port is not liveness:
  # an engine whose internal services have died can pass gate 1, report
  # `healthy` to `docker ps`, and still block a lane on a query that never
  # returns. engine_probe.py issues a real statement and checks the answer,
  # which is the weakest check that separates "listening" from "serving".
  # `timeout` is the backstop for the probe itself: statement_timeout is the
  # engine's to enforce, and an engine wedged badly enough enforces nothing, so
  # an unwrapped probe can block instead of failing and take its caller with it.
  timeout 25 "$PY" "$ROOT/scripts/run/engine_probe.py" --engine "$1" --timeout 10 2>/dev/null
}

# equitable_wait_ready <timeout_seconds> "<service> [<service>...]"
equitable_wait_ready() {
  local tmo="$1" services="$2" deadline ok s
  [ -z "$services" ] && return 0
  deadline=$((SECONDS + tmo))
  while :; do
    ok=1
    for s in $services; do equitable_probe "$s" || ok=0; done
    [ "$ok" = 1 ] && return 0
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "  TIMEOUT (${tmo}s) waiting for SUT services: ${services}" >&2
      return 1
    fi
    sleep 5
  done
}
BENCHMARK_RUNS="$ROOT/runs/benchmark_runs"
CURRENT="$BENCHMARK_RUNS/current"
LOCK_ROOT="$ROOT/runs/jobs"
ENGINES_ALL="reference materialize risingwave ksqldb flink spark feldera"
ALL_ENGINE_CSV="${ENGINES_ALL// /,}"

# Project policy, applied to BOTH the complete workflow and the staged
# `results` path so the two cannot disagree: every cohort covers all seven
# engines. Flink's scale sweep was once out of scope because in-process PyFlink
# needed 6.4 h per million-row run; the checkpointed cluster needs about a
# minute per formulation. Written with ${VAR-default} so a narrower cohort stays
# one declared variable away, for example:
#
#   BENCHMARK_EXCLUDE_SCALING=flink ./reproduce_all.sh results
BENCHMARK_EXCLUDE_CONFIRM="${BENCHMARK_EXCLUDE_CONFIRM-}"
BENCHMARK_EXCLUDE_SCALING="${BENCHMARK_EXCLUDE_SCALING-}"
export BENCHMARK_EXCLUDE_CONFIRM BENCHMARK_EXCLUDE_SCALING

# The engine universe a cohort must cover, after any DECLARED exclusion. An
# exclusion is named by the caller (BENCHMARK_EXCLUDE_*), recorded by the
# collector as excluded_engines, and verified at promotion time; a cohort that
# merely omits an engine is still rejected. See scripts/results/_scope.py.
engine_csv_excluding() {
  local excluded="$1" engine keep=""
  for engine in $ENGINES_ALL; do
    case ",${excluded}," in
      *",${engine},"*) continue ;;
    esac
    keep="${keep:+$keep,}$engine"
  done
  printf '%s' "$keep"
}
confirm_engine_csv() {
  engine_csv_excluding "${BENCHMARK_EXCLUDE_CONFIRM:-}"
}
scaling_engine_csv() {
  engine_csv_excluding "${BENCHMARK_EXCLUDE_SCALING:-}"
}

usage() {
  echo "usage: benchmark.sh init [id]"
  echo "       benchmark.sh prepare"
  echo "       benchmark.sh {check|durability}"
  echo "       benchmark.sh run <grid50k|confirm1m|scaling> [engine]"
  echo "       benchmark.sh lane <engine> [phase...]"
  echo ""
  echo "  BENCHMARK_DOMAIN=<domain>  restrict a run/lane to one domain, so the"
  echo "                             same phase can be sharded five ways in"
  echo "                             parallel (default: all)"
  echo "       benchmark.sh {status|finish|describe}"
}

load_benchmark() {
  [ -f "$CURRENT/benchmark.env" ] || {
    echo "ERROR: no current benchmark run; run: $0 init" >&2
    exit 1
  }
  # shellcheck disable=SC1091
  source "$CURRENT/benchmark.env"
  EQUITABLE_CAMPAIGN_ID="$EQUITABLE_BENCHMARK_RUN_ID"
  export EQUITABLE_BENCHMARK_RUN_ID EQUITABLE_CAMPAIGN_ID EQUITABLE_SOURCE_COMMIT
  export EQUITABLE_SOURCE_CLEAN=1
  export EQUITABLE_COHORT_GRID EQUITABLE_COHORT_CONFIRM EQUITABLE_COHORT_SCALING
}

require_source_snapshot() {
  [ "$(git rev-parse HEAD)" = "$EQUITABLE_SOURCE_COMMIT" ] || {
    echo "ERROR: HEAD changed after benchmark initialization." >&2
    exit 1
  }
  "$PY" -c \
    'from src.provenance import git_dirty; raise SystemExit(1 if git_dirty() else 0)' || {
      echo "ERROR: source/configuration changed during the benchmark run." >&2
      git status --short >&2
      exit 1
    }
}

require_data_snapshot() {
  local snapshot="$CURRENT/data_snapshot.json"
  local marker="$CURRENT/prepare.complete" expected actual
  [ -f "$snapshot" ] && [ -s "$marker" ] || {
    echo "ERROR: preparation is not complete; finish the prepare job first." >&2
    exit 1
  }
  expected="$(tr -d '[:space:]' < "$marker")"
  actual="$($PY -c \
    'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' \
    "$snapshot")"
  [ "$actual" = "$expected" ] || {
    echo "ERROR: the data snapshot changed after preparation." >&2
    exit 1
  }
  "$PY" scripts/verify/verify_data_snapshot.py "$snapshot"
}

require_stage() {
  local stage="$1" marker="$CURRENT/$1.complete"
  [ -f "$marker" ] && [ "$(tr -d '[:space:]' < "$marker")" = \
      "$EQUITABLE_SOURCE_COMMIT" ] || {
    echo "ERROR: $stage has not completed successfully for this benchmark run." >&2
    exit 1
  }
}

# Non-fatal counterpart to require_stage: has this stage already passed for the
# current source commit? Used by `check` so a failure in a late gate does not
# force a rerun of the earlier ones, which includes a destructive `down -v`.
stage_complete() {
  local marker="$CURRENT/$1.complete"
  [ -s "$marker" ] &&
    [ "$(tr -d '[:space:]' < "$marker")" = "$EQUITABLE_SOURCE_COMMIT" ]
}

require_benchmark_ready() {
  require_data_snapshot
  require_stage reset-engines
  require_stage durability
  require_stage smoke
  require_stage validation
}

# Every engine service the campaign can drive, and the readiness probe set that
# covers them. Kept next to the compose invocations so the two cannot drift.
# Flink and Spark are scored as clusters: a JobManager, TaskManager, and SQL
# Gateway, and a Spark master, worker, and driver service.
ENGINE_SERVICES="materialize risingwave kafka ksqldb feldera flink-jobmanager flink-taskmanager flink-sql-gateway spark-master spark-worker spark-driver"

#: Where the Spark cluster keeps input, checkpoints, and output (bind mount).
SPARK_CLUSTER_ROOT="${EQUITABLE_SPARK_CLUSTER_ROOT:-/tmp/equitable_spark_cluster}"

# The Spark driver resumes whatever cell current.json names when it starts, so a
# reset that left the bind mount in place would bring the next run up on the
# previous run's checkpoints. The files are root-owned, so the wipe runs in a
# throwaway container of the cluster image.
wipe_spark_cluster_root() {
  mkdir -p "$SPARK_CLUSTER_ROOT"
  docker run --rm --entrypoint sh -v "$SPARK_CLUSTER_ROOT:/data" \
    equitable/spark-cluster:4.2.0 -c \
    'rm -rf /data/input /data/output /data/ckpt /data/ckpt_cells /data/current.json /data/current.tmp /data/wm_*; mkdir -p /data/input /data/output /data/ckpt; chmod 777 /data /data/input /data/output /data/ckpt'
}

# Bring the whole stack down AND back up under BOTH compose files. The overlay is
# not optional: without it RisingWave runs `playground` (in-memory) and Kafka gets
# an anonymous volume, so D5 restarts an engine that never persisted anything and
# every cell dies with "table or source not found" instead of returning a
# recovery verdict. Leaving the `up` to a manual step is what let that happen.
reset_engine_volumes() {
  docker compose -f docker/streaming-engines.yml \
    -f docker/streaming-engines.durable.yml down -v --remove-orphans
  wipe_spark_cluster_root || return 1
  docker compose -f docker/streaming-engines.yml \
    -f docker/streaming-engines.durable.yml up -d $ENGINE_SERVICES
  equitable_wait_ready 300 "$ENGINE_SERVICES" || {
    echo "ERROR: engines did not become ready after the reset." >&2
    docker compose -f docker/streaming-engines.yml \
      -f docker/streaming-engines.durable.yml ps >&2 || true
    return 1
  }
}

# Assert the durability overlay is actually in force for the engines whose D5
# result depends on it. `docker inspect` is the arbiter, not our intent: the
# campaign has already run 15 RisingWave D5 cells against a container that was
# started from the base compose alone.
# Silent form of the check below: 0 = durably configured, 1 = not.
durable_stack_ok() {
  local engine="$1" cmd mounts
  case "$engine" in
    risingwave)
      cmd="$(docker inspect docker-risingwave-1 --format '{{json .Config.Cmd}}' 2>/dev/null || echo '')"
      case "$cmd" in *playground*) return 1 ;; esac ;;
    ksqldb)
      mounts="$(docker inspect docker-kafka-1 --format '{{json .Mounts}}' 2>/dev/null || echo '')"
      case "$mounts" in *docker_kafka_data*) ;; *) return 1 ;; esac ;;
    flink)
      # Retained checkpoints live on the named volume both processes mount; a
      # TaskManager without it would restart with nothing to restore.
      mounts="$(docker inspect docker-flink-taskmanager-1 --format '{{json .Mounts}}' 2>/dev/null || echo '')"
      case "$mounts" in *flink_checkpoints*) ;; *) return 1 ;; esac ;;
    spark)
      # The driver checkpoints to the bind mount the worker and the adapter share.
      mounts="$(docker inspect docker-spark-driver-1 --format '{{json .Mounts}}' 2>/dev/null || echo '')"
      case "$mounts" in *\"/data\"*) ;; *) return 1 ;; esac ;;
  esac
}

# Only D5 needs durability, so a stack that came up without
# docker/streaming-engines.durable.yml is repaired here rather than failing the
# whole phase at the top of run_phase; reset_one_engine_volume otherwise runs
# only during scaling. Repair first, and fail only if the repair did not take.
# D5 targets for a lane: the lane's own engine. Every scored deployment that can
# be killed and restored is its own D5 target.
d5_targets() {
  echo "$1"
}

require_durable_stack() {
  local engine="$1"
  durable_stack_ok "$engine" && return 0
  echo "WARNING: $engine is not durably configured; repairing before the phase." >&2
  reset_one_engine_volume "$engine" || {
    echo "ERROR: could not bring $engine up durably." >&2
    return 1
  }
  durable_stack_ok "$engine" && return 0
  case "$engine" in
    risingwave)
      echo "ERROR: docker-risingwave-1 is still running 'playground' (in-memory)." >&2
      echo "       D5 cannot produce a recovery verdict against it. Bring the" >&2
      echo "       stack up with docker/streaming-engines.durable.yml." >&2 ;;
    ksqldb)
      echo "ERROR: docker-kafka-1 still has no docker_kafka_data volume; ksqlDB" >&2
      echo "       rebuilds its tables from the Kafka changelog, so Kafka must" >&2
      echo "       be durable." >&2 ;;
    flink)
      echo "ERROR: docker-flink-taskmanager-1 has no flink_checkpoints volume;" >&2
      echo "       D5 needs retained checkpoints to restore from." >&2 ;;
    spark)
      echo "ERROR: docker-spark-driver-1 is not up with /data bind-mounted;" >&2
      echo "       D5 needs the driver's checkpoints to restore from." >&2 ;;
  esac
  return 1
}

# Wait for an engine's worker processes to actually be gone.
#
# Both watchdogs TERM the MATRIX SHELL only. bash does not pass a signal on to
# the child it is waiting on, so the python diagnostic underneath keeps running,
# keeps its engine connections, and keeps writing summaries -- and once the
# matrix shell exits, `wait "$matrix_pid"` returns and the lane proceeds to
# reset the engine while that orphan is still going. Its cells are then scored
# against an engine being torn down and rebuilt underneath them.
#
# A blocked cell is harmless (it is retried); a cell that scores a verdict
# against a half-reset engine is a wrong result that looks exactly like a real
# divergence, typically carrying keys the oracle never had.
#
# The orphan is reparented to init the moment the matrix shell dies, so it
# cannot be found by walking children. It is matched on its own argv instead,
# which is unambiguous because one lane owns one engine.
equitable_reap_engine_workers() {
  local engine="$1" deadline="${2:-60}" pids pid waited=0 alive
  # Restricted to actual python workers. `pgrep -f` matches any command line
  # containing the pattern, including a shell that merely mentions it, so the
  # comm check is what keeps this from reaping a bystander. (Written after a
  # bare `pkill -f` on this same pattern killed the shell that ran it.)
  pids=""
  for pid in $(pgrep -f "run_(diagnostic|d5_recovery)\.py .*--engine ${engine}( |\$)" 2>/dev/null || true); do
    [ "$pid" = "$$" ] && continue
    case "$(cat "/proc/$pid/comm" 2>/dev/null)" in
      python*) pids="$pids $pid" ;;
    esac
  done
  pids="$(printf '%s' "$pids" | sed 's/^ *//')"
  [ -n "$pids" ] || return 0
  echo "[$engine] $(printf '%s\n' $pids | wc -l) worker(s) still running after the phase stopped;" \
       "stopping them before the reset so no cell is scored against a half-reset engine."
  for pid in $pids; do kill -TERM "$pid" 2>/dev/null || true; done
  while [ "$waited" -lt "$deadline" ]; do
    alive=0
    for pid in $pids; do kill -0 "$pid" 2>/dev/null && alive=1; done
    [ "$alive" = 0 ] && return 0
    sleep 1
    waited=$((waited + 1))
  done
  echo "[$engine] worker(s) did not exit in ${deadline}s; sending KILL" >&2
  for pid in $pids; do kill -KILL "$pid" 2>/dev/null || true; done
  sleep 2
  return 0
}

FLINK_REST="${FLINK_REST:-http://localhost:8081}"
FLINK_CHECKPOINT_VOLUME="${FLINK_CHECKPOINT_VOLUME:-docker_flink_checkpoints}"

# Job ids the JobManager still owns, one per line. A job in a terminal state is
# not live: its checkpoint directory will never be read again.
flink_live_job_ids() {
  curl -fsS --max-time 10 "$FLINK_REST/jobs" 2>/dev/null \
    | "$PY" -c 'import json,sys
try:
    jobs = json.load(sys.stdin).get("jobs", [])
except Exception:
    sys.exit(1)
dead = {"FINISHED", "CANCELED", "FAILED"}
print("\n".join(j["id"] for j in jobs if j.get("status") not in dead))' 2>/dev/null || return 1
}

# Cancel every live Flink job before the lane that submitted it goes away.
#
# `job.sh stop` kills the lane's process group, but a Flink job runs inside the
# session cluster, not under the lane, so a stopped lane would leave the job it
# submitted running and checkpointing onto the very volume the disk guard is
# trying to drain. Same orphan class as equitable_reap_engine_workers above,
# one level up: the thing doing the work is not a child of the thing being
# killed.
cancel_flink_jobs() {
  local ids id n=0
  ids="$(flink_live_job_ids)" || return 0
  [ -n "$ids" ] || return 0
  for id in $ids; do
    curl -fsS --max-time 30 -X PATCH "$FLINK_REST/jobs/$id?mode=cancel" >/dev/null 2>&1 \
      && n=$((n + 1))
  done
  [ "$n" -gt 0 ] && echo "[flink] cancelled $n orphaned job(s) on the session cluster"
  return 0
}

# Remove retained checkpoint directories that belong to no live job.
#
# execution.checkpointing.num-retained bounds checkpoints within a job, but
# RETAIN_ON_CANCELLATION keeps the survivors forever once the job ends, and the
# harness submits one job per cell, so over a campaign the dead directories
# grow to hundreds of gigabytes and fill the disk.
#
# Safe because D5 only ever restores a job from its own retained checkpoint
# (src/adapters/flink_cluster.py, _await_restore): once a job reaches a terminal
# state nothing reads its directory again. A live job's directory is kept, and
# the 32-hex-character test means only a Flink job directory can ever match.
reap_flink_checkpoints() {
  local keep
  docker volume inspect "$FLINK_CHECKPOINT_VOLUME" >/dev/null 2>&1 || return 0
  # A REST failure means an unknown live set, so reap nothing rather than risk
  # deleting the checkpoint a running job is about to restore from.
  keep="$(flink_live_job_ids)" || {
    echo "[flink] JobManager did not answer; skipping the checkpoint reap." >&2
    return 0
  }
  docker run --rm -v "$FLINK_CHECKPOINT_VOLUME:/cp" -e KEEP="$keep" \
    alpine sh -c '
cd /cp || exit 0
del=0
for d in */; do
  d=${d%/}
  for k in $KEEP; do [ "$d" = "$k" ] && continue 2; done
  case "$d" in *[!0-9a-f]* ) continue ;; esac
  [ ${#d} -eq 32 ] || continue
  rm -rf -- "$d" && del=$((del+1))
done
[ "$del" -gt 0 ] && echo "[flink] reaped $del dead checkpoint director$([ "$del" = 1 ] && echo y || echo ies)"
exit 0' 2>/dev/null || true
  return 0
}

# Reset ONE engine's container and durable volume, leaving every other engine
# untouched. The global reset above tears down the whole compose project, so
# using it while another engine's shard is mid-run destroys that run. Each
# engine owns exactly one named volume (docker/streaming-engines.durable.yml),
# which is what makes the scoped reset equivalent for that engine.
reset_one_engine_volume() {
  local engine="$1" service volume
  case "$engine" in
    materialize) service=materialize; volume=docker_mz_data ;;
    risingwave)  service=risingwave;  volume=docker_rw_data ;;
    feldera)     service=feldera;     volume=docker_feldera_data ;;
    # ksqlDB rebuilds its tables from the Kafka changelog, so the durable state
    # to clear is Kafka's, and both services must come down together.
    ksqldb)      service="ksqldb kafka"; volume=docker_kafka_data ;;
    flink)       service="flink-jobmanager flink-taskmanager flink-sql-gateway"
                 volume=docker_flink_checkpoints ;;
    # Spark's state is a bind mount, not a volume: stop the cluster, wipe it,
    # and bring the cluster back.
    spark)
      echo "resetting engine state: spark (services: spark-master spark-worker spark-driver)"
      docker compose -f docker/streaming-engines.yml \
        -f docker/streaming-engines.durable.yml rm -sfv spark-driver spark-worker spark-master >/dev/null 2>&1 || true
      wipe_spark_cluster_root || return 1
      docker compose -f docker/streaming-engines.yml \
        -f docker/streaming-engines.durable.yml up -d spark-master spark-worker spark-driver >/dev/null 2>&1 || true
      equitable_wait_ready 300 "spark-driver" || {
        echo "ERROR: spark did not come back after its scoped reset." >&2
        return 1
      }
      return 0 ;;
    *) return 0 ;;
  esac
  echo "resetting engine state: $engine (services: $service, volume: $volume)"
  docker compose -f docker/streaming-engines.yml \
    -f docker/streaming-engines.durable.yml rm -sfv $service >/dev/null 2>&1 || true
  docker volume rm "$volume" >/dev/null 2>&1 || true
  # `docker volume rm` fails while any container still references the volume,
  # and swallowing that brings the engine back up on the state the reset was
  # supposed to clear -- a wrong verdict rather than a loud failure.
  if docker volume inspect "$volume" >/dev/null 2>&1; then
    for _ in 1 2 3 4 5; do
      sleep 2
      docker volume rm "$volume" >/dev/null 2>&1 || true
      docker volume inspect "$volume" >/dev/null 2>&1 || break
    done
  fi
  if docker volume inspect "$volume" >/dev/null 2>&1; then
    echo "ERROR: $volume survived the reset for $engine; refusing to run cells" >&2
    echo "       against state the reset did not clear." >&2
    return 1
  fi
  docker compose -f docker/streaming-engines.yml \
    -f docker/streaming-engines.durable.yml up -d $service >/dev/null 2>&1 || true
  # `up -d` returns as soon as the container is CREATED, not when the engine is
  # accepting queries. Without this wait the first cells of the phase connect
  # into the restart window and are recorded as blocked rather than as verdicts.
  equitable_wait_ready 300 "$service" || {
    echo "ERROR: $engine did not come back after its scoped reset; refusing to" >&2
    echo "       run cells against a half-started engine." >&2
    return 1
  }
}

validate_engine() {
  case " $ENGINES_ALL " in
    *" $1 "*) ;;
    *) echo "ERROR: unknown engine: $1" >&2; exit 2 ;;
  esac
}

# --- wedged-engine watchdog -------------------------------------------------
# An engine can die while keeping its port open and its process alive; a lane
# then blocks on one query indefinitely, no job status changes, and every cell
# it had left to run is lost.
#
# What makes acting on this safe is that the probe is a real statement rather
# than a socket: a merely busy engine still answers. Materialize answers
# SELECT 1 while saturating ten cores, and Spark's driver answers /status
# without taking its setup lock, so a busy driver still answers too, while a
# driver out of file descriptors does not. Engines with no liveness endpoint
# of their own here -- flink, reference -- are not watched at all, so a
# legitimately slow engine cannot be killed by this.
EQUITABLE_WATCHDOG="${EQUITABLE_WATCHDOG:-1}"
EQUITABLE_WATCHDOG_STRIKES="${EQUITABLE_WATCHDOG_STRIKES:-15}"
# Seconds between probes. Only tests change this; one strike is one minute.
EQUITABLE_WATCHDOG_INTERVAL="${EQUITABLE_WATCHDOG_INTERVAL:-60}"
WATCHDOG_PID=""
WATCHDOG_MARKER=""

start_engine_watchdog() {
  local engine="$1" target="$2" marker="$3"
  WATCHDOG_PID=""
  [ "$EQUITABLE_WATCHDOG" = 1 ] || return 0
  case "$engine" in
    materialize|risingwave|ksqldb|feldera|spark) ;;
    *) return 0 ;;
  esac
  rm -f "$marker"
  (
    strikes=0
    while kill -0 "$target" 2>/dev/null; do
      sleep "$EQUITABLE_WATCHDOG_INTERVAL"
      kill -0 "$target" 2>/dev/null || exit 0
      # Wrapped for the same reason as the readiness probe, and it matters more
      # here: an unwrapped probe that blocks stops the watchdog counting at all,
      # which is silently identical to having no watchdog.
      if timeout 30 "$PY" "$ROOT/scripts/run/engine_probe.py" \
           --engine "$engine" --timeout 15 >/dev/null 2>&1; then
        strikes=0
        continue
      fi
      strikes=$((strikes + 1))
      echo "[watchdog $engine] no answer to a probe query" \
           "($strikes/$EQUITABLE_WATCHDOG_STRIKES)"
      [ "$strikes" -lt "$EQUITABLE_WATCHDOG_STRIKES" ] && continue
      echo "[watchdog $engine] no answer for" \
           "$((strikes * EQUITABLE_WATCHDOG_INTERVAL))s;" \
           "stopping the phase so it can be retried against a fresh engine."
      : > "$marker"
      kill -TERM "$target" 2>/dev/null || true
      exit 0
    done
  ) &
  WATCHDOG_PID=$!
}

stop_engine_watchdog() {
  [ -n "$WATCHDOG_PID" ] || return 0
  kill "$WATCHDOG_PID" 2>/dev/null || true
  wait "$WATCHDOG_PID" 2>/dev/null || true
  WATCHDOG_PID=""
}

SHARD_LOCK_FD=""

release_shard_lock() {
  [ -n "${SHARD_LOCK_FD:-}" ] || return 0
  flock -u "$SHARD_LOCK_FD" 2>/dev/null || true
  exec {SHARD_LOCK_FD}>&-
  SHARD_LOCK_FD=""
}

acquire_shard_lock() {
  local lock="$1" phase="$2" engine="$3" domain="${4:-all}"
  command -v flock >/dev/null 2>&1 || {
    echo "ERROR: util-linux flock is required to serialize benchmark shards." >&2
    return 2
  }
  [ -z "${SHARD_LOCK_FD:-}" ] || {
    echo "ERROR: a shard lock is already held by this process." >&2
    return 2
  }
  mkdir -p "$(dirname "$lock")"
  exec {SHARD_LOCK_FD}>"$lock" || {
    echo "ERROR: cannot open shard lock file: $lock" >&2
    SHARD_LOCK_FD=""
    return 2
  }
  if ! flock -n "$SHARD_LOCK_FD"; then
    echo "ERROR: shard already running: $phase/$engine/$domain" >&2
    release_shard_lock
    return 1
  fi
}

cleanup_phase_processes() {
  if [ -n "${WATCHDOG_PID:-}" ]; then
    kill "$WATCHDOG_PID" 2>/dev/null || true
    wait "$WATCHDOG_PID" 2>/dev/null || true
    WATCHDOG_PID=""
  fi
  if [ -n "${matrix_pid:-}" ]; then
    kill "$matrix_pid" 2>/dev/null || true
    wait "$matrix_pid" 2>/dev/null || true
    matrix_pid=""
  fi
  release_shard_lock
}

run_phase() {
  local phase="$1" engine="${2:-all}" invocation rows repeats scale_rows
  local run_grid d5_engines code lock matrix_pid=""
  # One domain, or all five. Sharding a phase by domain is what lets the Flink
  # lanes run five ways wide instead of one cell at a time; every shard-scoped
  # name below carries the tag so concurrent shards never share a file.
  local domain="${BENCHMARK_DOMAIN:-all}" dtag=""
  [ "$domain" = all ] || dtag="-$domain"
  load_benchmark
  require_source_snapshot
  require_benchmark_ready
  [ "$engine" = all ] || validate_engine "$engine"
  if [ "$phase" = scaling ] && [ "$engine" = all ]; then
    echo "ERROR: run one scaling engine at a time to preserve scaling isolation." >&2
    exit 2
  fi
  # The persistent file is never removed. Kernel ownership of its open file
  # descriptor makes acquisition and owner publication one atomic operation.
  lock="$LOCK_ROOT/.benchmark-${phase}-${engine}${dtag}.flock"
  acquire_shard_lock "$lock" "$phase" "$engine" "$domain" || exit $?
  # The matrix runs in the background so the watchdog can observe its pid. Kill
  # and reap both children before releasing the shard lock on an interruption;
  # otherwise a replacement runner could overlap a child that is still exiting.
  trap 'cleanup_phase_processes' EXIT
  trap 'cleanup_phase_processes; exit 130' INT
  trap 'cleanup_phase_processes; exit 143' TERM

  case "$phase" in
    grid50k)
      invocation="$EQUITABLE_COHORT_GRID"; rows=50000; repeats=3
      run_grid=1; scale_rows=""; d5_engines="$(d5_targets "$engine")" ;;
    confirm1m)
      invocation="$EQUITABLE_COHORT_CONFIRM"; rows=1000000; repeats=1
      run_grid=1; scale_rows=""; d5_engines="$(d5_targets "$engine")" ;;
    scaling)
      invocation="$EQUITABLE_COHORT_SCALING"; rows=50000; repeats=3
      run_grid=0; scale_rows="10000 50000 200000 500000 1000000"
      d5_engines="" ;;
    *) echo "ERROR: unknown phase: $phase" >&2; exit 2 ;;
  esac

  # A D5 result is only meaningful on a durably-configured engine; check before
  # spending the phase, not after.
  if [ -n "$d5_engines" ]; then
    for d5_engine in $d5_engines; do
      require_durable_stack "$d5_engine" || {
        release_shard_lock
        trap - EXIT INT TERM
        return 1
      }
    done
  fi

  case "$phase:$engine" in
    scaling:materialize|scaling:risingwave|scaling:ksqldb|scaling:feldera)
      reset_one_engine_volume "$engine" || {
        release_shard_lock
        trap - EXIT INT TERM
        return 1
      }
      ;;
  esac

  echo "benchmark_run=$EQUITABLE_CAMPAIGN_ID phase=$phase engine=$engine domain=$domain"
  code=0
  WATCHDOG_MARKER="$LOCK_ROOT/.watchdog-${phase}-${engine}${dtag}"
  EQUITABLE_INVOCATION_ID="$invocation" \
  EQUITABLE_CAMPAIGN_PHASE="$phase" \
  KSQL_SERVICE_ID="verifiable_stream_${EQUITABLE_CAMPAIGN_ID}_${phase}" \
  KSQL_RUN_SUFFIX="${EQUITABLE_CAMPAIGN_ID}_${phase}_${engine}${dtag}" \
  ENGINES="$engine" DOMAINS="$domain" DIAGS=core \
  REPEATS="$repeats" MAX_ROWS="$rows" RUN_GRID="$run_grid" \
  SCALE_ROWS="$scale_rows" SCALE_REPEATS="$repeats" \
  SCALE_ENGINES="$engine" D5_ENGINES="$d5_engines" D5_ROWS="$rows" \
  RUN_NATIVE=0 RUN_MUTATION=0 RUN_D7S_SWEEP=0 PYTHON="$PY" \
    bash scripts/run/run_matrix.sh &
  matrix_pid=$!
  start_engine_watchdog "$engine" "$matrix_pid" "$WATCHDOG_MARKER"
  wait "$matrix_pid" || code=$?
  stop_engine_watchdog
  # A phase the watchdog stopped exits on a signal, not on 1, which would skip
  # the blocked-cell retry in run_lane. Normalize it, and bring the engine back
  # before that retry runs so it is not aimed at the same wedge.
  if [ -f "$WATCHDOG_MARKER" ]; then
    rm -f "$WATCHDOG_MARKER"
    echo "[$engine] phase $phase stopped by the watchdog; resetting the engine."
    # Orphaned workers first: resetting the volume out from under a live
    # diagnostic is what turns a blocked cell into a wrong verdict.
    equitable_reap_engine_workers "$engine" 60
    reset_one_engine_volume "$engine" || \
      echo "ERROR: could not reset $engine after the watchdog fired." >&2
    code=1
  fi

  release_shard_lock
  trap - EXIT INT TERM
  return "$code"
}


# --- one engine's phases, in order ------------------------------------------
# Phases are independent: each runs regardless of how the last one scored, and
# the lane's exit code is the worst of them. A phase that ends with blocked
# cells (one over-long ksqlDB batch, say) therefore does not cancel the phases
# after it.
#
# Scaling lanes are not serialized against one another. Nothing measured needs
# isolation: Table 8 reports verdict stability only, not throughput, and
# throughput_eligible in collect_scaling.py keys off `executed and status !=
# unsupported`. reset_one_engine_volume is scoped to one engine's services and
# one named volume, so concurrent scaling engines do not touch each other's
# Docker state, and no lane waits on another lane.
#
# What that costs: cells run under whatever host load their siblings create,
# and contention shows up as blocked cells rather than as wrong verdicts (a
# blocked cell has no verdict at all -- see the retry loop in run_lane). No
# queue caps concurrent scaling lanes; size the campaign to the host instead.

# How many times a phase that ended with blocked cells is re-run before the lane
# gives up. Each retry re-runs only the blocked cells (--resume-existing), so the
# cost is bounded by how many actually blocked. 0 disables retries.
BENCHMARK_PHASE_RETRIES="${BENCHMARK_PHASE_RETRIES:-2}"
# A server that just timed out should not be hammered by the retry immediately.
BENCHMARK_RETRY_DELAY_S="${BENCHMARK_RETRY_DELAY_S:-30}"

prepare_phase_retry() {
  local engine="$1" phase="$2"
  if [ "$BENCHMARK_RETRY_DELAY_S" -gt 0 ]; then
    echo "[lane $engine] cooling down ${BENCHMARK_RETRY_DELAY_S}s before retrying $phase"
    sleep "$BENCHMARK_RETRY_DELAY_S"
  fi
  case "$engine" in
    materialize|risingwave|ksqldb|feldera)
      if ! equitable_wait_ready 120 "$engine"; then
        echo "[lane $engine] query probe still fails; resetting before retrying $phase"
        reset_one_engine_volume "$engine"
      fi
      ;;
    spark)
      # A retry aimed at a wedged driver re-blocks every cell it retries, at
      # 600 s each, so make sure the driver answers before retrying.
      if ! equitable_wait_ready 120 "spark-driver"; then
        echo "[lane spark] driver still not ready; resetting before retrying $phase"
        reset_one_engine_volume spark
      fi
      ;;
  esac
}

# --- disk guard -------------------------------------------------------------
# A full host kills Kafka's log dir and then the ksqlDB and RisingWave lanes
# mid-cell, and neither can write its status or exit code, so `status` shows
# both as blank. Feldera's Rust compilation tree is the usual culprit: every
# distinct program the campaign compiles adds a crate to the container layer,
# and one lane can reach hundreds of gigabytes. Check free space at every phase
# boundary and stop a lane while there is still room to record why.
BENCHMARK_MIN_FREE_GIB="${BENCHMARK_MIN_FREE_GIB:-40}"

equitable_free_gib() {
  df -Pk "$ROOT" | awk 'NR==2 {print int($4 / 1048576)}'
}

# A low reading waits before it stops the lane. Seven concurrent lanes peak
# right at the floor, dipping a few GiB below it for minutes at a time, and a
# lane that stopped at once inside such a dip would stay stopped until the
# coordinator restarted it. Waiting holds no cell open, and the coordinator
# keeps reclaiming space meanwhile. The default stays under job.sh's one-hour
# stall check, so a waiting lane is never reported STALLED.
BENCHMARK_DISK_WAIT_S="${BENCHMARK_DISK_WAIT_S:-1800}"
BENCHMARK_DISK_POLL_S="${BENCHMARK_DISK_POLL_S:-60}"

# Returns 3, "stopped for disk", which the lane keeps distinct from 1 (blocked
# cells, retried) and 2 (usage error). reproduce_all.sh restarts a lane that
# exits 3 once space comes back (restart_disk_stopped_lanes).
require_free_disk() {
  local engine="$1" phase="$2" free waited=0 poll="$BENCHMARK_DISK_POLL_S"
  free="$(equitable_free_gib)"
  [ "$free" -ge "$BENCHMARK_MIN_FREE_GIB" ] && return 0
  if [ "$engine" = feldera ]; then
    echo "[lane feldera] only ${free} GiB free; resetting Feldera to drop its compiled programs before $phase"
    reset_one_engine_volume feldera || true
    free="$(equitable_free_gib)"
    [ "$free" -ge "$BENCHMARK_MIN_FREE_GIB" ] && return 0
  fi
  [ "$poll" -ge 1 ] 2>/dev/null || poll=1
  echo "[lane $engine] $(date -u +%Y-%m-%dT%H:%M:%SZ) only ${free} GiB free before $phase; waiting up to ${BENCHMARK_DISK_WAIT_S}s for ${BENCHMARK_MIN_FREE_GIB} GiB"
  while [ "$waited" -lt "$BENCHMARK_DISK_WAIT_S" ]; do
    sleep "$poll"
    waited=$((waited + poll))
    free="$(equitable_free_gib)"
    if [ "$free" -ge "$BENCHMARK_MIN_FREE_GIB" ]; then
      echo "[lane $engine] $(date -u +%Y-%m-%dT%H:%M:%SZ) ${free} GiB free after ${waited}s; starting $phase"
      return 0
    fi
  done
  echo "[lane $engine] STOPPING before $phase: ${free} GiB free after waiting ${waited}s, below BENCHMARK_MIN_FREE_GIB=${BENCHMARK_MIN_FREE_GIB}." >&2
  echo "[lane $engine] Free disk space, then re-run ./reproduce_all.sh; finished cells are kept." >&2
  return 3
}

run_lane() {
  local engine="$1"; shift
  local phases="${*:-grid50k confirm1m scaling}" phase code worst=0 attempt
  validate_engine "$engine"
  for phase in $phases; do
    case "$phase" in
      grid50k|confirm1m|scaling) ;;
      *) echo "ERROR: unknown phase: $phase" >&2; return 2 ;;
    esac
    echo
    echo "======================================================================"
    echo "[lane $engine] phase $phase  ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
    echo "======================================================================"
    if ! require_free_disk "$engine" "$phase"; then
      worst=3
      break
    fi
    code=0
    "$0" run "$phase" "$engine" || code=$?
    # A blocked cell is far more often transient than not: HTTP read timeouts
    # or a one-off 500 under host load, which produce a verdict on a plain
    # re-run. --resume-existing means a retry re-runs only the blocked cells, so
    # spend that here instead of leaving the operator a manual second pass.
    # Exit 1 is the blocked-cell code; exit 2 is a usage/config error worth
    # surfacing immediately rather than retrying.
    attempt=1
    while [ "$code" = 1 ] && [ "$attempt" -le "$BENCHMARK_PHASE_RETRIES" ]; do
      echo "[lane $engine] phase $phase exited 1 -- retry $attempt/$BENCHMARK_PHASE_RETRIES (blocked cells only)"
      if ! require_free_disk "$engine" "$phase"; then
        code=3
        break
      fi
      code=0
      if prepare_phase_retry "$engine" "$phase"; then
        "$0" run "$phase" "$engine" || code=$?
      else
        code=1
      fi
      attempt=$((attempt + 1))
    done
    # Dropping the container after each phase is what bounds the compilation
    # tree to one phase's programs instead of the whole lane's.
    if [ "$engine" = feldera ]; then
      reset_one_engine_volume feldera ||
        echo "WARNING: could not reset feldera after $phase; its compiled programs stay on disk." >&2
    fi
    if [ "$code" = 3 ]; then
      worst=3
      break
    fi
    if [ "$code" != 0 ]; then
      echo "[lane $engine] phase $phase exited $code -- CONTINUING to the next phase."
      [ "$code" -gt "$worst" ] && worst="$code"
    fi
  done
  echo
  echo "[lane $engine] finished phases: $phases (worst exit $worst)"
  return "$worst"
}

# Sourced purely for the probes above (run_matrix.sh); do not dispatch a stage.
if [ -n "${EQUITABLE_BENCHMARK_SOURCE_ONLY:-}" ]; then
  return 0
fi

case "${1:-}" in
  init)
    "$PY" -c \
      'from src.provenance import source_dirty; raise SystemExit(1 if source_dirty() else 0)' || {
      echo "ERROR: commit or remove source/configuration changes before initializing." >&2
      exit 1
    }
    # The collectors, promotion and lock run only after days of measurement.
    # They are exercised by the test suite, so run it before committing that
    # time to them: the eight-diagnostic grid would have failed here, not after
    # 4,795 runs.
    "$PY" -m unittest discover -s tests -t . >/dev/null 2>&1 || {
      echo "ERROR: the test suite fails; fix it before starting a campaign:" >&2
      echo "  $PY -m unittest discover -s tests -t ." >&2
      exit 1
    }
    mkdir -p "$BENCHMARK_RUNS"
    id="${2:-benchmark_$(date -u +%Y%m%dT%H%M%SZ)}"
    case "$id" in *[!a-zA-Z0-9_]*|'')
      echo "ERROR: benchmark run id may contain only letters, numbers, and underscores." >&2
      exit 2 ;;
    esac
    dir="$BENCHMARK_RUNS/$id"
    [ ! -e "$dir" ] || { echo "ERROR: benchmark run already exists: $id" >&2; exit 1; }
    mkdir -p "$dir"
    commit="$(git rev-parse HEAD)"
    {
      printf 'EQUITABLE_BENCHMARK_RUN_ID=%q\n' "$id"
      printf 'EQUITABLE_SOURCE_COMMIT=%q\n' "$commit"
      printf 'EQUITABLE_COHORT_GRID=%q\n' "${id}_grid50k"
      printf 'EQUITABLE_COHORT_CONFIRM=%q\n' "${id}_confirm1m"
      printf 'EQUITABLE_COHORT_SCALING=%q\n' "${id}_scaling"
    } > "$dir/benchmark.env"
    ln -sfn "$id" "$CURRENT"
    echo "initialized $id at $commit"
    echo "next: $0 prepare"
    ;;
  prepare)
    load_benchmark
    require_source_snapshot
    : > "$CURRENT/prepare.complete"
    : > "$CURRENT/reset-engines.complete"
    : > "$CURRENT/smoke.complete"
    : > "$CURRENT/validation.complete"
    prepare_args=(--verify)
    if [ -s "$CURRENT/data_snapshot.json" ] &&
       "$PY" scripts/verify/verify_data_snapshot.py \
         "$CURRENT/data_snapshot.json" --full; then
      echo "resuming preparation from the verified frozen data snapshot"
      prepare_args=(--from-frozen-data --verify)
    fi
    EQUITABLE_INVOCATION_ID="${EQUITABLE_CAMPAIGN_ID}_prepare" \
    EQUITABLE_CAMPAIGN_PHASE=prepare \
    EQUITABLE_DATA_SNAPSHOT="$CURRENT/data_snapshot.json" \
      ./reproduce_all.sh "${prepare_args[@]}"
    "$PY" -c \
      'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' \
      "$CURRENT/data_snapshot.json" > "$CURRENT/prepare.complete"
    ;;
  reset-engines)
    load_benchmark
    require_source_snapshot
    require_data_snapshot
    : > "$CURRENT/reset-engines.complete"
    reset_engine_volumes
    printf '%s\n' "$EQUITABLE_SOURCE_COMMIT" > \
      "$CURRENT/reset-engines.complete"
    ;;
  smoke)
    load_benchmark
    require_source_snapshot
    require_data_snapshot
    require_stage reset-engines
    : > "$CURRENT/smoke.complete"
    EQUITABLE_INVOCATION_ID="${EQUITABLE_CAMPAIGN_ID}_smoke" \
    EQUITABLE_CAMPAIGN_PHASE=smoke \
      ./reproduce_all.sh --from-frozen-data --smoke
    printf '%s\n' "$EQUITABLE_SOURCE_COMMIT" > "$CURRENT/smoke.complete"
    ;;
  run)
    case "${2:-}" in
      grid50k|confirm1m|scaling) run_phase "$2" "${3:-all}" ;;
      *) usage; exit 2 ;;
    esac
    ;;
  lane)
    shift
    [ -n "${1:-}" ] || { usage; exit 2; }
    run_lane "$@"
    ;;
  validation)
    load_benchmark
    require_source_snapshot
    require_data_snapshot
    require_stage smoke
    : > "$CURRENT/validation.complete"
    # The boundary check runs entirely in-process against the reference engine
    # and the pandas oracle -- it starts no container and shares no engine state
    # with the native/mutation/D7S matrix, so the two are genuinely independent
    # and run concurrently. Both exit codes are honoured; a failure in either
    # leaves validation.complete empty and the stage unpassed.
    boundary_log="$CURRENT/validation-boundary.log"
    EQUITABLE_INVOCATION_ID="${EQUITABLE_CAMPAIGN_ID}_validation" \
    EQUITABLE_CAMPAIGN_PHASE=validation \
      "$PY" scripts/run/run_boundary.py --domain all --max-rows 5000 \
      > "$boundary_log" 2>&1 &
    boundary_pid=$!

    matrix_code=0
    EQUITABLE_INVOCATION_ID="${EQUITABLE_CAMPAIGN_ID}_validation" \
    EQUITABLE_CAMPAIGN_PHASE=validation RUN_GRID=0 SCALE_ROWS="" D5_ENGINES="" \
    RUN_NATIVE=1 RUN_MUTATION=1 RUN_D7S_SWEEP=1 PYTHON="$PY" \
      bash scripts/run/run_matrix.sh || matrix_code=$?

    boundary_code=0
    wait "$boundary_pid" || boundary_code=$?
    echo "--- boundary check (ran concurrently) ---"
    cat "$boundary_log"
    [ "$boundary_code" -eq 0 ] || {
      echo "ERROR: boundary check failed (exit $boundary_code)." >&2; exit 1; }
    [ "$matrix_code" -eq 0 ] || {
      echo "ERROR: validation matrix failed (exit $matrix_code)." >&2; exit 1; }

    # RQ5's evidence, at the primary grid's pinned 50,000 rows so the family is
    # not read beside a cohort an order of magnitude larger. It runs after the
    # matrix rather than beside it because it rebuilds each engine's views once
    # per member per arm, which would contend with the native probes for the
    # same containers.
    family_code=0
    EQUITABLE_INVOCATION_ID="${EQUITABLE_CAMPAIGN_ID}_validation" \
    EQUITABLE_CAMPAIGN_PHASE=validation \
      "$PY" scripts/run/run_query_family.py || family_code=$?
    [ "$family_code" -eq 0 ] || {
      echo "ERROR: query family failed (exit $family_code)." >&2; exit 1; }

    # Three follow-up experiments, run one after another on an otherwise idle
    # host (validation precedes every lane), each promoted as its own block:
    #   disorder_sweep     how much disorder an arrival-ordered latest needs
    #                      before it publishes a wrong value (R2's magnitude);
    #   parallelism_sweep  D3 replay drift at parallelism 1, 2, 4 and 8;
    #   formulation_cost   wall time of the event-time repair against the
    #                      arrival-ordered form. It must stay serialized: a
    #                      timing shared with a lane measures contention.
    for experiment in run_disorder_sweep run_parallelism_sweep \
                      run_formulation_cost; do
      experiment_code=0
      EQUITABLE_INVOCATION_ID="${EQUITABLE_CAMPAIGN_ID}_validation" \
      EQUITABLE_CAMPAIGN_PHASE=validation \
        "$PY" "scripts/run/${experiment}.py" || experiment_code=$?
      [ "$experiment_code" -eq 0 ] || {
        echo "ERROR: ${experiment} failed (exit $experiment_code)." >&2; exit 1; }
    done

    printf '%s\n' "$EQUITABLE_SOURCE_COMMIT" > \
      "$CURRENT/validation.complete"
    ;;
  durability)
    # D5's prerequisite, enforced instead of merely documented. verify_persistence.py
    # was referenced in three comments and invoked by nothing, so the run reached
    # D5 with RisingWave in-memory and burned 15 cells discovering it.
    load_benchmark
    require_source_snapshot
    require_data_snapshot
    require_stage reset-engines
    : > "$CURRENT/durability.complete"
    durability_failed=0
    for engine in materialize risingwave ksqldb flink spark; do
      require_durable_stack "$engine" || { durability_failed=1; continue; }
      echo "== durability: $engine =="
      # 300s, not the 90s default: ksqlDB rebuilds its tables from the Kafka
      # changelog on restart and the default is thin for that.
      "$PY" scripts/run/verify_persistence.py --engine "$engine" --wait 300 || {
        echo "ERROR: $engine does not survive restart with state intact;" >&2
        echo "       D5 cannot produce a recovery verdict against it." >&2
        durability_failed=1
      }
    done
    # Feldera is deliberately excluded: the pinned open-source image gates
    # checkpointing to the enterprise edition, its D5 cells are recorded N/A,
    # and that is a measured result (scripts/run/run_d5_recovery.py), not a stack
    # misconfiguration to gate on.
    [ "$durability_failed" -eq 0 ] || exit 1
    printf '%s\n' "$EQUITABLE_SOURCE_COMMIT" > "$CURRENT/durability.complete"
    ;;
  check)
    load_benchmark
    for stage in reset-engines durability smoke validation; do
      if stage_complete "$stage"; then
        echo "[skip] $stage already passed for this run"
      else
        "$0" "$stage" || exit 1
      fi
    done
    ;;
  status)
    load_benchmark
    "$PY" scripts/run/benchmark_status.py --run-id "$EQUITABLE_CAMPAIGN_ID"
    ;;
  collect|promote|finish)
    # Finalization reads evidence and measures nothing, so it lives under
    # scripts/results/, where a fix is a finalization-only commit that the
    # measured campaign survives (scripts/results/_finalizer.py).
    exec bash "$ROOT/scripts/results/finalize.sh" "$@"
    ;;
  describe)
    echo "correctness:  50k rows x 7 engines x 5 domains x 8 diagnostics x 3 = 840 runs"
    echo "confirmation: 1M rows x 7 engines x 5 domains x 8 diagnostics x 1 = 280 runs"
    echo "scaling:      5 sizes x 7 engines x 5 domains x 7 diagnostics x 3 = 3675 runs"
    echo "total: 4795 fresh, non-reused scientific runs"
    ;;
  *) usage; exit 2 ;;
esac
