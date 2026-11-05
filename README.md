# EquiTable

EquiTable is a correctness benchmark for continuous queries over event
streams. The queries are grouped aggregates, COUNT, SUM, MIN, MAX, MEAN and
the latest value per key and per event-time bucket, which a stream
processing engine maintains incrementally as a table over an append-only
log: the standing result of a continuous query, kept current as events
arrive. EquiTable replays one normalized
event log into the engine under test and into two independent offline
oracles, pandas and DuckDB, and compares the engine's maintained table with
the oracle's result set key by key under a written equivalence contract. The
contract fixes source-event identity for deduplication, UTC event-time
bucketing, tie-breaking, finite-precision arithmetic and restatement
semantics. Eight diagnostics perturb the replay: duplicate delivery,
out-of-order arrival, non-deterministic floating-point reduction, late data
within the allowed lateness, crash and checkpoint recovery, Top-N ranking
over the maintained view, restatements that retract and replace an earlier
event, and overlapping hopping windows. Every order-sensitive query runs in
two formulations, the arrival-ordered view, which takes the latest row by
arrival order, and the smallest repair
that satisfies the contract, which orders by event time with a stable
identity tie-break; a pass is therefore attributed to the engine or to the
query. The benchmark covers six released engines across SQL streaming
(ksqlDB), structured streaming (Spark), streaming dataflow (Flink) and
incremental view maintenance (Materialize, RisingWave, Feldera), plus an
in-process reference implementation, on five real-world domains of one
million events each. This
repository is the artifact of the paper *EquiTable: Expressibility Is Not Equivalence in Maintained Streaming Aggregates*: the code, the
pinned environment, the ingest scripts and data manifests, the locked results, and the generators
of the paper's measured tables and figures.

## What is in the box

The paper's measured tables and figures are generated from the locked results
under `results/` by one script each under `scripts/figures/`; the generators only
read locked results. `./reproduce_all.sh render` writes the
tables to `tables/` and the figures to `figures/`. Table III, the diagnostic
suite, and Table IV, the claim levels and verdict states, have no generator.
Figures 1 and 2 are drawn, not measured, so they are not included.

| Paper | Content | Generator | Output | Reads |
|---|---|---|---|---|
| Table I | One confirmed public incident per correctness condition | `tab_11_witnesses.py` | `tables/tab_11_witnesses.tex` | `data/incidents.yaml` |
| Table II | The normalized row schema | `tab_03_schema.py` | `tables/tab_03_schema.tex` | `src/schema/record.py` |
| Table V | The seven execution backends and their pinned releases | `tab_04_components.py` | `tables/tab_04_components.tex` | `config/engines/pinned.yaml`, `src/adapters/` |
| Table VI | The five domains | `tab_02_domains.py` | `tables/tab_02_domains.tex` | `config/domains/*.yaml` |
| Table VII | The defect ledger: every divergent cell (engine, domain, diagnostic) and its root cause: engine defect, documented semantics, or precision trade-off | `tab_21_root_causes.py` | `tables/tab_21_root_causes.tex` | `results/design_space/`, `results/query_family/` |
| Table VIII | Validity controls on the benchmark itself: oracle agreement, schema boundary checks, mutation detection | `tab_12_validation.py` | `tables/tab_12_validation.tex` | `results/oracle_verification/`, `results/boundary/`, `results/mutation/`, `results/d7s_sweep/` |
| Table IX | Verdict stability across workload sizes, 10k to 1M events | `tab_08_scaling.py` | `tables/tab_08_scaling.tex` | `results/scaling/` |
| Table X | Floating-point replay drift by degree of parallelism, and the wall-time cost of the repaired query against the arrival-ordered one | `results_experiments.py --tables-only` | `tables/tab_22_parallel_cost.tex` | `results/parallelism_sweep/`, `results/formulation_cost/` |
| Table XI | Comparison with prior benchmarks | `tab_01_related_work.py` | `tables/tab_01_related_work.tex` | curated in the script |
| Figure 3 | Contract equivalence per engine, diagnostic and domain: the 240 cells of the main grid | `fig_02_design_space.py` | `figures/fig_02_design_space.png` | `results/design_space/` |
| Figure 4 | Attribution: whether a pass belongs to the engine, to the repaired query, or to both | `fig_05_attribution.py` | `figures/fig_05_attribution.png` | `results/design_space/`, `results/query_family/` |
| Figure 5 | Keys whose arrival-ordered latest value is wrong, as out-of-order delivery grows from none to a full shuffle | `fig_06_disorder.py` | `figures/fig_06_disorder.png` | `results/disorder_sweep/` |

The numbers (cell counts, pass rates, drift keys,
timings) come from the same blocks under `results/`;
[docs/RESULTS.md](docs/RESULTS.md) maps each block to the tables, figures,
and text that read it.

## Requirements

| Item | Minimum, enforced by `check-machine` | The host that produced the locked results |
|---|---|---|
| Operating system | Linux, x86_64 | Ubuntu 24.04, kernel 6.8 |
| CPUs | 8 logical | 16 logical |
| Memory | 32 GiB | 125 GiB |
| Free disk | 100 GiB | about 250 GB; the Flink checkpoint volume alone reached about 100 GB during the scaling sweep |
| Software | Docker Engine with Compose v2, Git, Python 3.12, Java, util-linux `flock` | Python 3.12.12, Docker Compose v5 |
| Network | downloads of about 2.6 GB of raw source data, Python packages, pinned container images | |

Expected time on the measured host:

| Command | Time |
|---|---|
| `setup` | minutes |
| `quick` | under an hour, including the taxi download |
| `render` | about one minute |
| the complete run (`./reproduce_all.sh`) | about 4 hours of preparation and validation, then about 53 hours for the 4,795 grid runs |

A machine with fewer cores or less memory takes longer.

## Quick path

Three commands run the benchmark end to end on real data:

```bash
./reproduce_all.sh setup      # create the pinned Python environment
./reproduce_all.sh quick      # taxi, 10,000 rows, reference plus Materialize
./reproduce_all.sh render    # the paper's nine tables and three figures
```

`quick` downloads the June 2024 taxi trip records from the TLC archive (about
60 MB), normalizes them to the one-million-event log and checks its checksum
against the tracked manifest, builds the taxi fixtures (duplicates,
reordering, late data, restatements), starts the chosen engine with Docker
Compose, and runs the seven portable diagnostics at 10,000 events on the
in-process reference backend and on that engine, with both oracles
cross-checked. It prints one verdict per cell: whether the engine's
maintained table equals the oracle's result set under the contract. `quick materialize`
(the default), `quick flink`, `quick spark`, `quick risingwave`, `quick ksqldb`, or `quick feldera`
chooses another engine. The quick run writes under `runs/quick_*` only and
never touches `results/`. It is smaller than the locked 50,000-row cells, so
a divergence that needs a rare trigger may not appear in it; the locked
verdicts for the same engine and diagnostics are in
`results/design_space/`.

`render` reads `results/` only. It needs no engine, no network, and no
benchmark run.

## Full reproduction

```bash
./reproduce_all.sh            # the complete workflow, in the background
./reproduce_all.sh status     # progress by stage and result group
./reproduce_all.sh log        # follow the live log
```

The complete command creates the pinned environment, checks the machine,
downloads and verifies the data, validates all seven backends, runs the
three result groups, collects the results, and compares it cell by cell
with the locked results under `results/`:

| Result group | Design | Runs |
|---|---|---:|
| Correctness | 50,000 rows, 7 backends, 5 domains, 8 diagnostics, 3 repeats | 840 |
| Confirmation | 1,000,000 rows, same cells, 1 repeat | 280 |
| Scaling | 10k, 50k, 200k, 500k and 1M rows, 7 backends, 5 domains, 7 diagnostics, 3 repeats | 3,675 |
| Total | | 4,795 |

The validation stage also runs the three follow-up experiments the paper
reports: the disorder sweep (Figure 5), the parallelism sweep and the
formulation cost (Table X). Flink and Spark run as clusters on the host,
each with a separate worker process, so the crash-recovery diagnostic kills
the process that holds the query state and requires the engine to restore it
from a durable checkpoint and then ingest the rest of the log.

The work runs as a resumable background job. If the machine restarts or the
job fails, run the same command again; validated cells are kept and missing
cells resume. When every lane is done, `status` reports
`reproduction comparison: PASS` or `FAIL` and writes the report to
`runs/benchmark_runs/<run id>/reproduction_report.json`.

**What PASS means.** Every cell of the fresh run, one per engine, domain and
diagnostic, has the same verdict as the locked cell, except for three documented sources of run-to-run variation,
which the report lists instead of comparing exactly:

1. the D3 replay cells of Flink and RisingWave, and any cell the lock records
   as repeat-unstable, may land on pass, fail, or repeat-unstable;
2. Flink's ranking and filter members of the query family, whose missing
   retraction depends on how updates fall into batches, may score
   attributable, repaired, or repair-divergent;
3. only Flink must show zero drift keys at parallelism one, since RisingWave
   drifts at every parallelism.

A fresh run never replaces `results/official_results.json`. Timing ratios
are reported separately, because they depend on the hardware.

## Reading the locked results

`results/official_results.json` is the lock. It names the benchmark run
(`benchmark_run_id`), the source commit, the host, the Python version, the
checksums of the benchmark configuration and of the dependency lock, and
the sixteen result blocks. Each entry names one block, the run that
produced it, and the checksum of its manifest.
The source commit belongs to the development repository from which this
tree was exported, so it does not resolve here; the file checksums are what
verify the results.

Each block lives under `results/<block>/<run id>/` and holds three files:
`summary.json` with the result group and its validity checks, a CSV with one row
per cell, and `promotion_manifest.json` with the checksum of every file. The
main grid is `results/design_space/<run id>/design_space.csv`: one row per
engine, domain, and diagnostic, with the columns `status` (`pass`, `fail`,
`repeat-unstable`, `unsupported`, `blocked`), `repeats` and `repeats_pass`,
`hourly_emr` (the exact-match ratio of the hourly table, the share of keys
whose aggregates equal the oracle's), `reason`, and `run_id`. The paper's six verdict states map onto these as follows:
equivalent is `pass`, divergent is `fail`, repeat-unstable keeps its name,
unsupported and blocked keep their names, and not applicable is D5 on the
two backends that cannot pose the recovery question.

[docs/RESULTS.md](docs/RESULTS.md) lists which block feeds which table and figure.

## Regenerating the tables and figures

```bash
./reproduce_all.sh render
```

The command checks the lock, then runs the generators listed above. Tables
are LaTeX fragments under `tables/`, figures are PNG files under `figures/`.
The generators read `results/`, `config/`, `src/` and `data/incidents.yaml`,
and nothing else; the same scripts produced the tables and figures in the
paper.

## Repository layout

| Path | Purpose |
|---|---|
| `reproduce_all.sh` | the only program a user runs |
| `config/` | pinned benchmark, domain and engine definitions |
| `data/` | data manifests and the incident list; raw inputs, normalized logs, fixtures and the oracle cache are built here and are not tracked |
| `docker/` | pinned container definitions for the six engines |
| `src/` | the normalized schema, one adapter per engine (setup, ingest, read), the two oracles, the comparator, the diagnostics and provenance |
| `scripts/` | ingestion, fixture construction, execution, verification, collection and the table and figure generators; internal, called by `reproduce_all.sh` |
| `results/` | the locked results the paper reports |
| `tables/`, `figures/` | written by `render` |
| `tests/` | unit tests: `python -m unittest discover -s tests -t .` |
| `docs/` | the documents below |

## Documentation

- [docs/RUNBOOK.md](docs/RUNBOOK.md): machine setup, the complete run, monitoring, source changes during a run, failure recovery, the step-by-step alternative.
- [docs/RESULTS.md](docs/RESULTS.md): how results are locked, which block feeds which table, figure and sentence, and the locked results.
- [docs/SEMANTICS.md](docs/SEMANTICS.md): the equivalence contract: the normalized schema, UTC event-time bucketing, deduplication by source identity, the out-of-order policy, the definition of latest, the exact aggregates and their comparison tolerance, restatements.
- [docs/DATASETS.md](docs/DATASETS.md): the five sources, their licenses, fetch scripts, and the checksums of the normalized logs.

## Citing

The paper is under review. Until it appears, cite this repository by its
URL and commit. The paper title is *EquiTable: Expressibility Is Not Equivalence in Maintained Streaming Aggregates*.

## License

The code in this repository is released under the terms in `LICENSE`. The
raw source data is not redistributed: each ingest script downloads it from
its publisher under the publisher's terms, which [docs/DATASETS.md](docs/DATASETS.md)
lists per source.
