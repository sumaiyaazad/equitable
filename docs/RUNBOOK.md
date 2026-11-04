# Reproduction runbook

This document is the operator's guide: how to prepare a machine, run the
quick path or the complete benchmark run, monitor it, change source while it runs,
recover from failures, and read the comparison at the end. Use only
`./reproduce_all.sh`. The scripts under `scripts/` are internal:
`benchmark.sh` enforces the stage and provenance gates, `run_matrix.sh`
defines the fixed experiment matrix, and `job.sh` records background jobs.

## The fixed experiment

| Result group | Cells | Runs |
|---|---|---:|
| Correctness: 50,000 events, 3 repeats | 7 backends x 5 domains x 8 diagnostics, one cell each | 840 |
| Confirmation: 1,000,000 events, 1 repeat | 7 backends x 5 domains x 8 diagnostics | 280 |
| Scaling: 10k, 50k, 200k, 500k, 1M events, 3 repeats | 7 backends x 5 domains x 7 diagnostics x 5 sizes | 3,675 |
| Total | | 4,795 |

D5, crash recovery, is part of correctness and confirmation: the process that
holds the query state is killed, the engine must restore it from a durable
checkpoint, and every run then ingests the rest of the log so the final
maintained table is compared, not only the restored prefix. Materialize,
RisingWave, ksqlDB, Flink and Spark run it. The reference backend and Feldera record an
unsupported capability result. Scaling leaves D5 out because it is a portable
size sweep; crash recovery is a lifecycle experiment.

The validation stage, which runs before any grid lane, also runs three follow-up experiments and locks each as its own
block: `disorder_sweep` (`scripts/run/run_disorder_sweep.py`),
`parallelism_sweep` (`scripts/run/run_parallelism_sweep.py`) and
`formulation_cost` (`scripts/run/run_formulation_cost.py`).

## The machine

Use one dedicated Linux x86_64 machine with at least 8 logical CPUs, 32 GiB
of memory and 100 GiB of free SSD space; `check-machine` enforces these
minimums. Install Docker Engine with Compose v2, Git, Python 3.12, Java and
util-linux `flock`. The host that produced the locked results had 16 logical
CPUs, 125 GiB of memory and about 250 GB free; the Flink checkpoint volume
alone reached about 100 GB during the scaling sweep.

A `.venv` is specific to its machine and is never copied between hosts.

## Getting the repository and the data

```bash
git clone <repository url> equitable
cd equitable
./reproduce_all.sh setup           # pinned Python environment
./reproduce_all.sh check-machine   # CPUs, memory, disk, Docker, Java, flock
./reproduce_all.sh describe        # the fixed run counts
```

The repository carries the source, the pinned dependency and container
definitions, the data manifests and the locked results under `results/`. It
does not carry raw downloads, normalized logs, fixtures, oracle caches, run
state, `.venv` or Docker storage. The data stage downloads every raw source
named in [docs/DATASETS.md](DATASETS.md), rebuilds the normalized logs and fixtures, and
verifies each output against the checksums in the tracked manifests, so a
reproduction replays byte-identical inputs. Network access is needed for the
downloads, for Python packages and for the pinned container images.

## The quick path

```bash
./reproduce_all.sh quick             # taxi, 10,000 rows, reference plus Materialize
./reproduce_all.sh quick flink       # any of materialize, risingwave, ksqldb, flink, spark, feldera, reference
./reproduce_all.sh render           # the paper's nine tables and three figures
```

`quick` runs in the foreground and takes under an hour the first time, most
of it the taxi download and the fixture build; later runs take minutes. It
downloads the June 2024 TLC records, normalizes them to the one-million-row
log, checks the log's checksum against `data/normalized/transportation_taxi/manifest.json`,
builds the taxi fixtures, starts the chosen engine with Docker Compose, and
runs the seven portable diagnostics (D1 duplicates, D2 reordering, D3 replay
determinism, D4 late data, D6 Top-N, D7 restatements, D8 hopping windows) at
10,000 events on the reference backend and on that engine, with both oracles
cross-checked. It prints one row per cell with the verdict, the exact-match
ratio of the hourly maintained table, and whether the arrival-ordered
formulation, the engine's native latest-value operator, also passed. Its summaries live under `runs/quick_*` and are never locked.

`render` needs no engine and no data: it checks the lock and regenerates
every generated table and figure from `results/`.

## The complete run

```bash
./reproduce_all.sh
```

One resumable background job creates the environment, checks the host,
initializes the run, prepares and verifies the data, validates all seven
backends, executes all 4,795 runs with one lane per engine, collects the
results, and compares it cell by cell with the lock.

```bash
./reproduce_all.sh status     # stage, counts per result group, job state
./reproduce_all.sh log        # follow the live log; Ctrl-C stops only the tail
```

If the machine restarts or the job fails, run the same command again.
Completed cells are validated and kept; missing cells resume. An interrupted
preparation reuses its verified frozen data snapshot and its completed
per-domain oracle checks. The same source commit and the same host are
required throughout.

### Source changes during a run, and the git commands to avoid

Every background stage executes from a detached worktree pinned to the launch
commit, under `runs/source_snapshots/`. You may therefore edit and commit in
the primary checkout while a run is active: the running process keeps using
its snapshot, and a later launch picks up the new commit and starts a new
benchmark run when the source differs. Data, oracle caches, run summaries and
locked results are shared between the snapshot and the primary checkout;
executable source and configuration are not.

That sharing also covers the tracked manifests under `data/normalized/` and
`data/fixtures/`. A command that rewrites tracked files reaches the running
benchmark and aborts its next phase when the data snapshot check sees the
change. So, while a run is active:

- do not `git pull`, `git checkout` or `git stash`;
- commit freely; pull after the run finishes.

Old source snapshots are pruned at each launch; the current snapshot, the
active run's pinned commit and any snapshot a live job holds are kept.

### Never run an engine command beside an active run

Materialize, RisingWave, Feldera and ksqlDB create their maintained views
and tables under fixed names. Two runs against the same container drop and
recreate each other's views, and both results become untrustworthy without either failing.
`benchmark.sh` holds a per-lane lock; `scripts/run/run_diagnostic.py`,
`run_d5_recovery.py`, `scripts/adapters/smoke.py` and the quick path invoked
directly do not. While any grid lane, pilot or quick run is active, do not
start another. Check first:

```bash
pgrep -af 'run_diagnostic.py|run_d5_recovery.py|smoke.py'
```

### Pilots

A pilot is a one-domain, one-repeat sweep outside the benchmark-run
machinery. Its summaries carry no run identifier, so they can never be
locked. Use one to see whether engines diverge at all before committing a
host to the full benchmark:

```bash
EQUITABLE_INVOCATION_ID=pilot_$(date -u +%Y%m%dT%H%M%SZ) \
  .venv/bin/python -u scripts/run/run_diagnostic.py \
    --engine all --domain environment_airquality --diagnostic core \
    --co-oracle --max-rows 50000 --sweep

python scripts/run/pilot_report.py --invocation pilot_...
python scripts/run/pilot_report.py --invocation pilot_A --against pilot_B
bash scripts/run/pilot_status.sh
```

About 42 cells; budget a couple of hours. `pilot_status.sh` reports whether a
pilot is still alive and gives one verdict: `INCOMPLETE (n/42)`, `COMPLETE
BUT UNHEALTHY` (some cells are `blocked`, a harness failure to fix before the
benchmark), or `OK`. Divergent cells are results, not failures.

### Moving to another host

Avoid changing hosts after measurements begin: the final results require one
host and platform across all result groups; a group whose runs differ in any of these is rejected.
If little progress has been made, stop every job, keep the old directory as a
record, clone on the new host and start from there.

```bash
./reproduce_all.sh status
./reproduce_all.sh stop reproduce_all    # repeat `stop JOB` for every RUNNING job
./reproduce_all.sh status
```

## While it runs

Once or twice a day, from another session:

```bash
./reproduce_all.sh status
free -h
df -h .
docker ps --format 'table {{.Names}}\t{{.Status}}'
docker stats --no-stream
tail -n 100 runs/jobs/reproduce_all/log.txt
```

A healthy run has the four initial checks `done`, a `RUNNING` job, and counts
that increase over time: 120 correctness, 40 confirmation and 525 scaling
runs per engine. A long 1M-row cell can hold a count still for hours; that is
fine while the log advances and the active engine uses CPU.

The cell states mean:

| State | Meaning |
|---|---|
| `equivalent` | the engine's maintained table equals the oracle's result set under the contract, key by key within the tolerance |
| `known_divergence` | an expected, documented finding, not an execution failure |
| `divergence` | a disagreement to review before it is used as a finding |
| `repeat-unstable` | complete repeats mix pass and fail; a valid result that records instability |
| `unsupported` | the engine cannot express the query as a declarative relational view; valid only where the capability matrix says so |
| `blocked` | the run artifact is invalid or incomplete; fix or rerun |

Investigate at once when the job says `FAILED` or `LOST`, when `blocked`
grows or one cell fails repeatedly, when a container is `Exited`,
`Restarting` or `unhealthy`, when free disk nears 50 GiB or the kernel kills
a process, when counts and the log stop moving while the active engine is
idle, when another workload takes CPU, memory, disk or Docker resources
during scaling, or when the recorded source snapshot is missing or its commit
differs from the job's. Save `status`, the last 200 log lines, `docker ps -a`,
`docker stats`, `free`, `df`, `git status` and `git rev-parse HEAD` before
restarting anything. After fixing the cause, rerun `./reproduce_all.sh`.

## Completion and the comparison

The run is complete when `status` shows all initial checks done, `840/840`
correctness, `280/280` confirmation, `3675/3675` scaling, and the job `DONE`.
`status` then reports `reproduction comparison: PASS` or `FAIL`, and the
report at `runs/benchmark_runs/<run id>/reproduction_report.json` lists every
check by name.

PASS means every cell matched the lock, apart from three documented sources
of run-to-run variation that the report names instead of comparing exactly:
the D3 replay cells of Flink and RisingWave and any cell the lock records as
repeat-unstable may land on pass, fail or repeat-unstable; Flink's ranking and filter
members of the query family, whose missing retraction depends on how updates
fall into batches, may score attributable, repaired or repair-divergent; and
only Flink must show zero drift keys at parallelism one, since RisingWave
drifts at every level. Timing ratios are reported separately because they
depend on the hardware. A reproduction never replaces
`results/official_results.json`.

The lock records the commit of the tree that measured it. This repository
starts from a snapshot of that tree, so that commit is not in its history;
the comparison checks that the fresh run was measured by one clean commit
with the same configuration and dependency checksums as the lock.

## Step-by-step alternative

Each public command below starts one background stage; wait for `status` to
report completion before the next. `correctness` and `confirmation` take an
optional engine and run all seven when it is omitted; `scaling` takes one
engine, so it is run once per engine.

```bash
./reproduce_all.sh data            # download, normalize, fixtures, oracle checks
./reproduce_all.sh check           # validate the seven backends and run the follow-ups
./reproduce_all.sh correctness
./reproduce_all.sh confirmation
./reproduce_all.sh scaling reference
./reproduce_all.sh scaling materialize
./reproduce_all.sh scaling risingwave
./reproduce_all.sh scaling ksqldb
./reproduce_all.sh scaling flink
./reproduce_all.sh scaling spark
./reproduce_all.sh scaling feldera
./reproduce_all.sh results         # collect and compare with the lock
```

These stages are serialized. `./reproduce_all.sh` with no arguments instead
runs one lane per engine concurrently; phases inside a lane stay ordered, and a kernel
mutex keeps any two scaling phases from overlapping. A scaling phase can
still overlap a slower engine's correctness phase, so recorded timings are
not isolated rates and no throughput claim rests on them.

`results` refuses a result group that is incomplete, mixes commits, has too
few repeats, changes an input checksum, contains blocked, missing or
mixed-capability cells, or fails a determinism check. Because the results are
already locked, it writes the reproduction report and locks nothing. Keep
the reproduction checkout, with its run summaries, as the audit record.

## Regenerating the tables and figures

```bash
./reproduce_all.sh render
```

Writes the nine tables to `tables/` and the three figures to `figures/`, from
the locked results under `results/` alone. The [README](../README.md) lists each generator and its output file;
[docs/RESULTS.md](RESULTS.md) maps each block to what reads it.

## Expected time

On the host that produced the locked results, preparation and validation
took about 4 hours and the 4,795 grid runs about 53 hours with all seven
lanes running concurrently; 98% of expected-table lookups reused a cached
oracle table once the caches were built. A machine with less memory or fewer
cores takes longer, and the first data stage adds the download time of the
five raw sources.
