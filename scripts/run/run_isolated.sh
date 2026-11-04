#!/usr/bin/env bash
# Run one benchmark action from an immutable Git worktree.
#
# The worktree pins executable source and configuration to the launch commit.
# Large/generated state remains in the primary checkout through explicit
# symlinks, so a commit or pull in that checkout cannot alter a running campaign.
set -euo pipefail

PRIMARY_ROOT="$(git rev-parse --show-toplevel)"
# The launcher records HEAD before backgrounding this job, so a commit landing
# between launch and here cannot repoint the campaign at newer source.
SOURCE_COMMIT="${EQUITABLE_LAUNCH_COMMIT:-}"
if [ -n "$SOURCE_COMMIT" ]; then
  SOURCE_COMMIT="$(git -C "$PRIMARY_ROOT" rev-parse --verify --quiet \
    "${SOURCE_COMMIT}^{commit}" || true)"
fi
if [ -z "$SOURCE_COMMIT" ]; then
  SOURCE_COMMIT="$(git -C "$PRIMARY_ROOT" rev-parse HEAD)"
fi
SNAPSHOT_PARENT="$PRIMARY_ROOT/runs/source_snapshots"
SNAPSHOT_ROOT="$SNAPSHOT_PARENT/$SOURCE_COMMIT"

# Large or generated state stays in the primary checkout and is linked in.
SHARED_PATHS="runs results .venv data/raw data/oracle_cache
  data/normalized/environment_airquality
  data/normalized/transportation_taxi
  data/normalized/datacenter_drives
  data/normalized/finance_markets
  data/normalized/sports_football
  data/fixtures/environment_airquality
  data/fixtures/transportation_taxi
  data/fixtures/datacenter_drives
  data/fixtures/finance_markets
  data/fixtures/sports_football"


link_shared_path() {
  local relative="$1" link target
  link="$SNAPSHOT_ROOT/$relative"
  target="$PRIMARY_ROOT/$relative"
  if [ -L "$link" ] && [ "$(readlink "$link")" = "$target" ]; then
    return 0
  fi
  if [ -e "$link" ] || [ -L "$link" ]; then
    rm -rf -- "$link"
  fi
  mkdir -p "$(dirname "$link")" "$target"
  ln -s "$target" "$link"
}

# Remove one snapshot worktree. The shared symlinks are unlinked by name first
# so no removal step can ever descend into the primary checkout's real data.
discard_snapshot() {
  local snapshot="$1" shared
  for shared in $SHARED_PATHS; do
    if [ -L "$snapshot/$shared" ]; then
      rm -f -- "$snapshot/$shared"
    fi
  done
  rm -rf -- "$snapshot"
  git -C "$PRIMARY_ROOT" worktree prune
}

# Snapshots are one full checkout each, so keep only the ones still in use: this
# job's own, any live job's, and the active campaign's pinned commit.
prune_unused_snapshots() {
  local job status pid path keep env_file campaign_commit=""
  keep=" $SNAPSHOT_ROOT "
  env_file="$PRIMARY_ROOT/runs/benchmark_runs/current/benchmark.env"
  if [ -f "$env_file" ]; then
    campaign_commit="$(sed -n 's/^EQUITABLE_SOURCE_COMMIT=//p' "$env_file")"
    if [ -n "$campaign_commit" ]; then
      keep="$keep $SNAPSHOT_PARENT/$campaign_commit "
    fi
  fi
  for job in "$PRIMARY_ROOT"/runs/jobs/*/; do
    [ -d "$job" ] || continue
    status="$(cat "$job/status" 2>/dev/null || true)"
    if [ "$status" != RUNNING ]; then continue; fi
    pid="$(cat "$job/pid" 2>/dev/null || true)"
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then continue; fi
    path="$(cat "$job/source_snapshot" 2>/dev/null || true)"
    if [ -n "$path" ]; then keep="$keep $path "; fi
  done
  for path in "$SNAPSHOT_PARENT"/*/; do
    path="${path%/}"
    [ -d "$path" ] || continue
    case "$keep" in *" $path "*) continue ;; esac
    echo "pruning unused source snapshot: $(basename "$path")"
    discard_snapshot "$path"
  done
}

prepare_snapshot() {
  local actual shared
  mkdir -p "$SNAPSHOT_PARENT"

  if [ ! -e "$SNAPSHOT_ROOT/.git" ]; then
    # An interrupted `git worktree add` leaves a directory with no .git file.
    # Discard it and retry instead of wedging this commit forever.
    if [ -e "$SNAPSHOT_ROOT" ]; then
      echo "discarding incomplete source snapshot: $SNAPSHOT_ROOT" >&2
      discard_snapshot "$SNAPSHOT_ROOT"
    fi
    git -C "$PRIMARY_ROOT" worktree prune
    git -C "$PRIMARY_ROOT" worktree add --detach "$SNAPSHOT_ROOT" "$SOURCE_COMMIT"
  fi

  actual="$(git -C "$SNAPSHOT_ROOT" rev-parse HEAD)"
  if [ "$actual" != "$SOURCE_COMMIT" ]; then
    echo "ERROR: source snapshot commit mismatch: $actual != $SOURCE_COMMIT" >&2
    return 1
  fi

  for shared in $SHARED_PATHS; do
    link_shared_path "$shared"
  done

  prune_unused_snapshots
}

prepare_snapshot
echo "source snapshot: $SOURCE_COMMIT"
echo "snapshot path:   $SNAPSHOT_ROOT"

code=0
if [ -n "${EQUITABLE_JOB_DIR:-}" ]; then
  printf '%s\n' "$SOURCE_COMMIT" > "$EQUITABLE_JOB_DIR/source_commit"
  printf '%s\n' "$SNAPSHOT_ROOT" > "$EQUITABLE_JOB_DIR/source_snapshot"
fi

(
  cd "$SNAPSHOT_ROOT"
  EQUITABLE_SOURCE_COMMIT="$SOURCE_COMMIT" \
  EQUITABLE_SOURCE_CLEAN=1 \
  EQUITABLE_PRIMARY_ROOT="$PRIMARY_ROOT" \
    bash ./reproduce_all.sh "$@"
) || code=$?

exit "$code"
