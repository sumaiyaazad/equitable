#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# job.sh -- run any long command in the background with a log + status file.
#
# Generic: works for ingest, verification, the engine matrix, anything.
# State lives in runs/jobs/<name>/ (log.txt, status, pid, exit_code).
#
#   scripts/run/job.sh start  <name> <command...>   # launch in background
#   scripts/run/job.sh status [name]                # list all, or detail one
#   scripts/run/job.sh tail   <name>                # follow the live log
#   scripts/run/job.sh stop   <name>                # kill it
#
# Example:
#   scripts/run/job.sh start oracle_aq \
#       python scripts/run/run_oracle_verification.py --domain airquality
#   scripts/run/job.sh status
#   scripts/run/job.sh tail oracle_aq
# ---------------------------------------------------------------------------
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
JOBS="$ROOT/runs/jobs"
sub="${1:-}"; shift || true

# Seconds without CPU progress before a RUNNING lane is called stalled.
STALL_SECONDS="${EQUITABLE_STALL_SECONDS:-3600}"

fmt_duration() {
  local s="$1"
  if [ "$s" -ge 3600 ]; then
    printf '%dh%02dm' "$((s / 3600))" "$(((s % 3600) / 60))"
  else
    printf '%dm' "$((s / 60))"
  fi
}

# Job names are reused between campaigns, so records from an earlier run sit
# in this table looking exactly like the current one's, and a job left STOPPED
# by an earlier run would read as a failure of the run in progress. Lanes carry
# an `owner`, other jobs carry a `source_commit`, so use whichever is present.
prior_run_note() {
  local dir="$1" name="$2" status="$3" owner commit
  local env_file="$ROOT/runs/benchmark_runs/current/benchmark.env"
  [ -f "$env_file" ] || return 0
  local run_id source_commit
  run_id="$(sed -n 's/^EQUITABLE_BENCHMARK_RUN_ID=//p' "$env_file")"
  source_commit="$(sed -n 's/^EQUITABLE_SOURCE_COMMIT=//p' "$env_file")"
  owner="$(cat "$dir/owner" 2>/dev/null || true)"
  commit="$(cat "$dir/source_commit" 2>/dev/null || true)"
  if [ -n "$owner" ]; then
    [ "$owner" = "$run_id" ] || printf '  [prior run]'
    return 0
  fi
  if [ -n "$commit" ]; then
    [ "$commit" = "$source_commit" ] || printf '  [prior run]'
    return 0
  fi
  # A lane of the current run always gets an owner written just after it starts;
  # one with neither marker, and not currently running, belongs to an older run.
  case "$name" in
    lane_*) [ "$status" = RUNNING ] || printf '  [prior run]' ;;
  esac
}

# Total CPU seconds consumed by every process in a job's process group.
job_cpu_seconds() {
  ps -g "$1" -o cputimes= 2>/dev/null | awk '{s+=$1} END {print s+0}'
}

# Identity of the process group's current worker set: the youngest start time in
# the group, in jiffies since boot. Two samples that disagree here describe
# DIFFERENT processes, so their CPU totals are not comparable and a repeated
# total means nothing.
job_cpu_epoch() {
  local pid newest=0 st
  for pid in $(ps -g "$1" -o pid= 2>/dev/null); do
    st="$(awk '{print $22}' "/proc/$pid/stat" 2>/dev/null)"
    [ -n "$st" ] || continue
    [ "$st" -gt "$newest" ] 2>/dev/null && newest="$st"
  done
  printf '%s\n' "$newest"
}

# Flag a RUNNING lane only when its process group has stopped consuming CPU.
#
# Log mtime is not liveness. Python block-buffers a redirected stdout, so a
# low-output lane can go a day or more without writing one line while its
# worker holds a core and completes cells; reporting that as STALLED trains the
# reader to ignore the flag. A wedged engine looks the opposite way round: the
# client blocks on a query that never returns and the whole group stops
# accumulating CPU. CPU progress separates the two cases that log timing
# cannot.
#
# Supervisors are exempt: reproduce_all spends the campaign asleep in
# wait_for_lanes and burns almost no CPU, so measuring it this way would call a
# healthy coordinator stalled forever. Its liveness is its pid plus the lanes
# it is waiting on.
stall_note() {
  local dir="$1" name="$2" pid="$3" now cpu epoch prev_t prev_cpu prev_epoch
  case "$name" in lane_*) ;; *) return 0 ;; esac
  [ -n "$pid" ] || return 0
  now="$(date +%s)"
  cpu="$(job_cpu_seconds "$pid")"
  epoch="$(job_cpu_epoch "$pid")"
  prev_t=""; prev_cpu=""; prev_epoch=""
  if [ -r "$dir/.cpu_sample" ]; then
    read -r prev_t prev_cpu prev_epoch < "$dir/.cpu_sample" || true
  fi
  # CPU seconds are a whole-number counter over the LIVE processes in the group,
  # so equality alone does not mean "no progress". An I/O-bound lane accrues
  # about one second per fifteen, and any engine reset or worker turnover starts
  # the group's total again from zero and sweeps back through the same small
  # integers. materialize was reported STALLED for 15h48m on exactly that
  # coincidence: a freshly restarted worker held 11 seconds at the instant of the
  # sample, matching a value written by a different process a night earlier.
  # Anchoring the baseline to the worker set makes the comparison mean what it says.
  if [ -n "$prev_cpu" ] && [ "$cpu" = "$prev_cpu" ] \
     && [ -n "$prev_epoch" ] && [ "$epoch" = "$prev_epoch" ]; then
    if [ "$((now - prev_t))" -ge "$STALL_SECONDS" ]; then
      printf '  [STALLED %s with no CPU progress -- check the engine]' \
        "$(fmt_duration "$((now - prev_t))")"
    fi
    return 0
  fi
  printf '%s %s %s\n' "$now" "$cpu" "$epoch" > "$dir/.cpu_sample"
}

case "$sub" in
  start)
    name="${1:-}"; shift || true
    [ -z "$name" ] && { echo "usage: job.sh start <name> <command...>"; exit 1; }
    dir="$JOBS/$name"; mkdir -p "$dir"
    if [ -f "$dir/pid" ] && kill -0 "$(cat "$dir/pid" 2>/dev/null)" 2>/dev/null; then
      echo "job '$name' is already running (pid $(cat "$dir/pid"))"; exit 1
    fi
    echo "RUNNING" > "$dir/status"
    rm -f "$dir/exit_code" "$dir/source_commit" "$dir/source_snapshot"
    date -u +"started %Y-%m-%dT%H:%M:%SZ : $*" > "$dir/log.txt"
    # nohup is available on macOS and Linux. Use setsid as an extra isolation
    # layer when the host provides it, but do not require the Linux-only command.
    runner=(nohup)
    if command -v setsid >/dev/null 2>&1; then
      runner+=(setsid)
    fi
    # The child records its OWN pid. setsid forks whenever the caller is already
    # a process-group leader, in which case $! is a parent that exits at once --
    # `status` would then report a healthy multi-day campaign as LOST and the
    # concurrency guard in reproduce_all.sh would stop guarding. Observed, not
    # hypothetical. $! remains the fallback if the child never gets that far.
    rm -f "$dir/pid"
    "${runner[@]}" bash -c '
      dir="$1"; shift
      printf "%s\n" "$$" > "$dir/pid"
      EQUITABLE_JOB_DIR="$dir" "$@" >> "$dir/log.txt" 2>&1
      code=$?
      echo "$code" > "$dir/exit_code"
      if [ "$code" = 0 ]; then echo "DONE"; else echo "FAILED (exit $code)"; fi > "$dir/status"
    ' _ "$dir" "$@" </dev/null >/dev/null 2>&1 &
    launcher=$!
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      [ -s "$dir/pid" ] && break
      sleep 0.5
    done
    [ -s "$dir/pid" ] || printf '%s\n' "$launcher" > "$dir/pid"
    echo "started '$name' (pid $(cat "$dir/pid"))"
    echo "  log:    runs/jobs/$name/log.txt"
    echo "  status: ./reproduce_all.sh status"
    echo "  log:    ./reproduce_all.sh log $name"
    ;;

  status)
    # --all is a listing flag, not a job name; strip it before the name check
    # or `status --all` looks up a job called "--all" and fails.
    show_all=0
    if [ "${1:-}" = "--all" ]; then show_all=1; shift; fi
    name="${1:-}"
    if [ -n "$name" ]; then
      dir="$JOBS/$name"
      [ -d "$dir" ] || { echo "no job '$name'"; exit 1; }
      st="$(cat "$dir/status" 2>/dev/null || echo '?')"
      pid="$(cat "$dir/pid" 2>/dev/null || echo '-')"
      # An empty status file means the job died while the disk was full: the
      # wrapper could open the file but not write to it. Say so instead of
      # printing a blank; the file is left as found.
      [ -n "$st" ] || st="DIED (no status written; the disk may have been full, check the log)"
      if [ "$st" = "RUNNING" ] && ! kill -0 "$pid" 2>/dev/null; then
        st="LOST (process ended without an exit record)"
        echo "$st" > "$dir/status"
      fi
      echo "$name : $st  (pid $pid)"
      if [ -s "$dir/source_commit" ]; then
        source_commit="$(cat "$dir/source_commit")"
        echo "source commit: ${source_commit:0:12}"
      fi
      echo "--- last 12 log lines ---"
      tail -n 12 "$dir/log.txt" 2>/dev/null || true
    else
      [ -d "$JOBS" ] || { echo "no jobs yet"; exit 0; }
      # Shards that agree collapse to one line; shards that differ are named
      # individually, so a diverging shard stays visible instead of being lost
      # among lines that repeat a status already on screen. Jobs belonging to
      # an earlier run are counted, not listed, since they say nothing about
      # the run in progress.
      declare -A GROUP_MEMBERS=() GROUP_ORDER=()
      order=""
      prior_count=0
      prior_names=""
      for d in "$JOBS"/*/; do
        [ -d "$d" ] || continue
        n="$(basename "$d")"
        st="$(cat "$d/status" 2>/dev/null || echo '?')"
        pid="$(cat "$d/pid" 2>/dev/null || echo '-')"
        [ -n "$st" ] || st="DIED (no status written; check log)"
        if [ "$st" = "RUNNING" ] && ! kill -0 "$pid" 2>/dev/null; then
          st="LOST (check log)"
          echo "$st" > "$d/status"
        elif [ "$st" = "RUNNING" ]; then
          st="$st$(stall_note "$d" "$n" "$pid")"
        fi
        prior="$(prior_run_note "$d" "$n" "$st")"
        if [ -n "$prior" ] && [ "$show_all" = 0 ]; then
          prior_count=$((prior_count + 1))
          prior_names="$prior_names $n"
          continue
        fi
        st="$st$prior"
        if [ -s "$d/source_commit" ]; then
          source_commit="$(cat "$d/source_commit")"
          st="$st  [source ${source_commit:0:12}]"
        fi
        # A sharded lane is lane_<engine>_<phase>_<domain>; group on the phase
        # prefix so the domain is the only thing that varies within a group.
        if [[ "$n" =~ ^(lane_[a-z0-9]+_(correctness|confirmation)_)(.+)$ ]]; then
          key="${BASH_REMATCH[1]}|$st"
          member="${BASH_REMATCH[3]}"
        else
          key="$n|$st"
          member=""
        fi
        if [ -z "${GROUP_MEMBERS[$key]+set}" ]; then
          order="$order$key
"
        fi
        GROUP_MEMBERS[$key]="${GROUP_MEMBERS[$key]-}${member} "
      done

      printf "%-34s %s\n" "JOB" "STATUS"
      while IFS= read -r key; do
        [ -n "$key" ] || continue
        name="${key%%|*}"
        st="${key#*|}"
        members="$(printf '%s' "${GROUP_MEMBERS[$key]}" | tr -s ' ' | sed 's/ $//')"
        count="$(printf '%s' "$members" | wc -w | tr -d ' ')"
        if [ -z "$members" ]; then
          printf "%-34s %s\n" "$name" "$st"
        elif [ "$count" = 1 ]; then
          printf "%-34s %s\n" "${name}${members}" "$st"
        else
          printf "%-34s %s\n" "${name}* ($count)" "$st"
          printf "%-34s   %s\n" "" "$(printf '%s' "$members" | sed 's/ /, /g')"
        fi
      done <<< "$order"
      if [ "$prior_count" -gt 0 ]; then
        printf "%-34s %s\n" "($prior_count from earlier runs)" \
          "not listed; re-run with --all"
      fi
    fi
    ;;

  tail)
    name="${1:-}"; [ -z "$name" ] && { echo "usage: job.sh tail <name>"; exit 1; }
    tail -f "$JOBS/$name/log.txt"
    ;;

  stop)
    name="${1:-}"; [ -z "$name" ] && { echo "usage: job.sh stop <name>"; exit 1; }
    dir="$JOBS/$name"; pid="$(cat "$dir/pid" 2>/dev/null || echo '')"
    if [ -n "$pid" ]; then
      kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
      echo "STOPPED" > "$dir/status"
      echo "stopped '$name' (pid $pid)"
    else
      echo "no pid recorded for '$name'"
    fi
    ;;

  *)
    echo "usage: job.sh {start|status|tail|stop} ..."
    echo "  start  <name> <command...>"
    echo "  status [name]"
    echo "  tail   <name>"
    echo "  stop   <name>"
    exit 1
    ;;
esac
