#!/usr/bin/env bash
# Finalize the current campaign: collect its cells and, when no evidence is
# locked yet, promote and lock them; otherwise compare them with the lock.
#
# These stages only read evidence, so they accept either the commit that
# measured the campaign or a descendant that changed nothing a measurement
# stage executes (scripts/results/_finalizer.py). A finalizer bug found after
# a multi-day campaign is then fixed by one commit and a re-run of this script,
# not by measuring every cell again.
#
#   finalize.sh collect | promote | finish
#
# EQUITABLE_FINALIZE_SOURCE_ONLY=1 loads the functions without dispatching,
# like benchmark.sh's sentinel.
if [ -z "${EQUITABLE_FINALIZE_SOURCE_ONLY:-}" ]; then
  set -euo pipefail
fi

SELF="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/$(basename -- "${BASH_SOURCE[0]}")"
cd "$(git -C "$(dirname -- "$SELF")" rev-parse --show-toplevel)"
# benchmark.sh supplies load_benchmark, the stage markers and the engine sets.
# The sentinel stops it from dispatching a stage of its own.
# shellcheck source=scripts/run/benchmark.sh
EQUITABLE_BENCHMARK_SOURCE_ONLY=1 . scripts/run/benchmark.sh

usage() {
  echo "usage: finalize.sh {collect|promote|finish}"
}

# The finalization counterpart of require_source_snapshot: HEAD is the commit
# that measured the campaign, or a clean descendant changing finalization only.
require_finalizable_source() {
  "$PY" scripts/results/_finalizer.py check "$EQUITABLE_SOURCE_COMMIT" || exit 1
}


if [ -n "${EQUITABLE_FINALIZE_SOURCE_ONLY:-}" ]; then
  return 0
fi

case "${1:-}" in
  collect)
    load_benchmark
    require_finalizable_source
    require_benchmark_ready
    "$PY" scripts/verify/verify_data_snapshot.py \
      "$CURRENT/data_snapshot.json" --full
    "$PY" scripts/verify/verify_oracle_cache.py \
      --campaign-id "$EQUITABLE_CAMPAIGN_ID"
    if "$PY" scripts/results/_finalizer.py find \
        --evidence-commit "$EQUITABLE_SOURCE_COMMIT" --kind design_space_collect \
        --invocation-id "$EQUITABLE_COHORT_GRID" \
        --required-engines "${BENCHMARK_ENGINES_GRID:-$ALL_ENGINE_CSV}" \
        >/dev/null 2>&1; then
      echo "reusing completed correctness collection"
    else
      "$PY" scripts/figures/collect_design_space.py \
        --invocation-id "$EQUITABLE_COHORT_GRID" --rows 50000 \
        --d5-rows 50000 --min-repeats 3 --repeats 3 \
        --engines "${BENCHMARK_ENGINES_GRID:-}"
    fi
    if "$PY" scripts/results/_finalizer.py find \
        --evidence-commit "$EQUITABLE_SOURCE_COMMIT" --kind design_space_collect \
        --invocation-id "$EQUITABLE_COHORT_CONFIRM" \
        --required-engines "$(confirm_engine_csv)" \
        >/dev/null 2>&1; then
      echo "reusing completed confirmation collection"
    else
      "$PY" scripts/figures/collect_design_space.py \
        --invocation-id "$EQUITABLE_COHORT_CONFIRM" --rows 1000000 \
        --d5-rows 1000000 --min-repeats 1 --repeats 1 \
        --exclude-engines "${BENCHMARK_EXCLUDE_CONFIRM:-}"
    fi
    if "$PY" scripts/results/_finalizer.py find \
        --evidence-commit "$EQUITABLE_SOURCE_COMMIT" --kind scaling_collect \
        --invocation-id "$EQUITABLE_COHORT_SCALING" \
        --required-engines "$(scaling_engine_csv)" \
        >/dev/null 2>&1; then
      echo "reusing completed scaling collection"
    else
      "$PY" scripts/figures/collect_scaling.py \
        --invocation-id "$EQUITABLE_COHORT_SCALING" \
        --min-repeats 3 --repeats 3 \
        --exclude-engines "${BENCHMARK_EXCLUDE_SCALING:-}"
    fi
    ;;
  promote)
    load_benchmark
    require_finalizable_source
    require_benchmark_ready
    [ ! -f results/official_results.json ] || {
      echo "ERROR: official evidence is locked; reproduction runs cannot be promoted." >&2
      exit 1
    }
    grid="$($PY scripts/results/_finalizer.py find \
      --evidence-commit "$EQUITABLE_SOURCE_COMMIT" --kind design_space_collect \
      --invocation-id "$EQUITABLE_COHORT_GRID" \
      --required-engines "$ALL_ENGINE_CSV")"
    confirm="$($PY scripts/results/_finalizer.py find \
      --evidence-commit "$EQUITABLE_SOURCE_COMMIT" --kind design_space_collect \
      --invocation-id "$EQUITABLE_COHORT_CONFIRM" \
      --required-engines "$(confirm_engine_csv)")"
    scaling="$($PY scripts/results/_finalizer.py find \
      --evidence-commit "$EQUITABLE_SOURCE_COMMIT" --kind scaling_collect \
      --invocation-id "$EQUITABLE_COHORT_SCALING" \
      --required-engines "$(scaling_engine_csv)")"
    "$PY" scripts/results/promote_results.py "$grid" \
      --block design_space --feeds "design-space and attribution figures, root-cause table, result prose"
    "$PY" scripts/results/promote_results.py "$confirm" \
      --block design_space_confirmation --feeds "abstract, RQ2, RQ3"
    "$PY" scripts/results/promote_results.py "$scaling" \
      --block scaling --feeds "Tab 8 and RQ3 prose"
    for domain in environment_airquality transportation_taxi datacenter_drives \
                  finance_markets sports_football; do
      oracle="$($PY scripts/results/_finalizer.py find \
        --evidence-commit "$EQUITABLE_SOURCE_COMMIT" --kind oracle_verification \
        --invocation-id "${EQUITABLE_CAMPAIGN_ID}_prepare" --domain "$domain")"
      "$PY" scripts/results/promote_results.py "$oracle" \
        --block oracle_verification --feeds "validity table and result prose"
    done
    boundary="$($PY scripts/results/_finalizer.py find \
      --evidence-commit "$EQUITABLE_SOURCE_COMMIT" --kind boundary \
      --invocation-id "${EQUITABLE_CAMPAIGN_ID}_validation")"
    mutation="$($PY scripts/results/_finalizer.py find \
      --evidence-commit "$EQUITABLE_SOURCE_COMMIT" --kind detection_power \
      --invocation-id "${EQUITABLE_CAMPAIGN_ID}_validation")"
    d7s="$($PY scripts/results/_finalizer.py find \
      --evidence-commit "$EQUITABLE_SOURCE_COMMIT" --kind d7s_sweep \
      --invocation-id "${EQUITABLE_CAMPAIGN_ID}_validation")"
    native="$($PY scripts/results/_finalizer.py find \
      --evidence-commit "$EQUITABLE_SOURCE_COMMIT" --kind native_semantics \
      --invocation-id "${EQUITABLE_CAMPAIGN_ID}_validation")"
    family="$($PY scripts/results/_finalizer.py find \
      --evidence-commit "$EQUITABLE_SOURCE_COMMIT" --kind query_family \
      --invocation-id "${EQUITABLE_CAMPAIGN_ID}_validation")"
    "$PY" scripts/results/promote_results.py "$boundary" \
      --block boundary --feeds "validity table"
    "$PY" scripts/results/promote_results.py "$mutation" \
      --block mutation --feeds "validity table and result prose"
    "$PY" scripts/results/promote_results.py "$d7s" \
      --block d7s_sweep --feeds "artifact only; no manuscript exhibit"
    "$PY" scripts/results/promote_results.py "$native" \
      --block native_semantics --feeds "RQ4 prose"
    "$PY" scripts/results/promote_results.py "$family" \
      --block query_family --feeds "attribution figure and RQ5 prose"
    for block in disorder_sweep parallelism_sweep formulation_cost; do
      found="$($PY scripts/results/_finalizer.py find \
        --evidence-commit "$EQUITABLE_SOURCE_COMMIT" --kind "$block" \
        --invocation-id "${EQUITABLE_CAMPAIGN_ID}_validation")"
      "$PY" scripts/results/promote_results.py "$found" \
        --block "$block" --feeds "disorder figure, parallelism and cost table, RQ3 and RQ5 prose"
    done
    ;;
  finish)
    "$SELF" collect
    if [ -f results/official_results.json ]; then
      "$PY" scripts/results/verify_promotions.py --require-lock
      load_benchmark
      official_id="$($PY -c \
        'import json; print(json.load(open("results/official_results.json"))["benchmark_run_id"])')"
      if [ "$EQUITABLE_CAMPAIGN_ID" = "$official_id" ]; then
        echo "resuming final rendering for the official benchmark run"
      else
        echo "official evidence is locked; validating this run as a reproduction"
        "$PY" scripts/results/verify_reproduction.py \
          --run-id "$EQUITABLE_CAMPAIGN_ID"
      fi
    else
      "$SELF" promote
      "$PY" scripts/results/verify_promotions.py
      "$PY" scripts/results/lock_official_results.py
    fi
    load_benchmark
    official_id="$($PY -c \
      'import json; print(json.load(open("results/official_results.json"))["benchmark_run_id"])')"
    if [ "$EQUITABLE_CAMPAIGN_ID" = "$official_id" ]; then
      "$PY" -c \
        'import hashlib, pathlib; p=pathlib.Path("results/official_results.json"); print(hashlib.sha256(p.read_bytes()).hexdigest())' \
        > "$CURRENT/official-finalize.complete"
    fi
    ;;
  *) usage; exit 2 ;;
esac
