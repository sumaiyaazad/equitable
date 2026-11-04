# Results: how locked results become tables

The paper reads measured values only from `results/`. This document explains
how a run becomes locked results, which block feeds which table, figure and
sentence, and lists every locked block in order.

## From a run to locked results

A benchmark run writes one summary per cell, an engine, a domain and a
diagnostic, under `runs/`. Collection gathers the summaries of one result
group into a block, a result set with its provenance, and checks the whole
result group before anything is locked:

- one clean source commit for every cell, and the same configuration and
  dependency checksums;
- complete coverage of the engines, domains and diagnostics the fixed matrix
  requires, with the required number of repeats;
- unchanged input checksums and oracle cache keys across engines;
- no blocked or missing cells and no mixed capability observations.

A complete set of repeats that mixes pass and fail is a valid result and is
locked with the status `repeat-unstable`; it is a finding, not a failure.

The first complete `./reproduce_all.sh results` run copies each validated block to
`results/<block>/<run id>/` and writes `results/official_results.json`, the
lock. The lock names the benchmark run, the source commit, the host, and the
checksum of every locked block's manifest. After the lock exists, no later run
can add to or replace the results: a later run writes a reproduction report
under `runs/benchmark_runs/<run id>/` that compares its cells with the lock.

Each block directory holds `summary.json` (the result group and its validity
checks), one CSV with a row per cell, and `promotion_manifest.json` with the
checksum of every file. Table and figure generators only read
these files and nothing else: no run state, no git state, no engine.

## Which block feeds which table and figure

Table and figure numbers follow the conference format of the paper.

| Block | What it holds | Tables and figures |
|---|---|---|
| `design_space` | the 280 correctness cells, 50,000 rows, 3 repeats | Table VII, Figure 3, Figure 4 |
| `design_space_confirmation` | the same 280 cells at 1,000,000 rows, 1 repeat | results |
| `scaling` | 245 portable cells at 10k, 50k, 200k, 500k and 1M rows, 3 repeats | Table IX |
| `query_family` | the four-shape query family under ordered and shuffled delivery | Table VII, Figure 4 |
| `oracle_verification` | the pandas and DuckDB oracles against each other and against the reference backend on each domain at 1M events | Table VIII |
| `boundary` | the schema boundary checks per domain | Table VIII |
| `mutation` | every mutant (an adapter with one correctness condition deliberately broken) against the diagnostic that must detect it | Table VIII |
| `d7s_sweep` | the bounded-deletion sketch sweep, a control on the benchmark's own restatement oracle | Table VII | artifact only |
| `native_semantics` | watermark and Top-N probes through engine interfaces | results |
| `disorder_sweep` | keys whose arrival-ordered latest value is wrong, from ordered delivery to a full shuffle | Figure 5 |
| `parallelism_sweep` | D3 replay drift on Flink and RisingWave at parallelism 1, 2, 4 and 8 | Table X |
| `formulation_cost` | wall time of the event-time repair against the arrival-ordered form | Table X |

Tables I, II, V, VI and XI read no measured block: they come from the incident
list, the schema module, the pinned engine registry, the domain registry and
a curated comparison. Tables III and IV have no generator. Figures 1 and 2 are drawn, not measured, so they are not
included. `./reproduce_all.sh render` regenerates all of the rest; the [README](../README.md)
lists the generator and output file of each.

## Locked results

One row per locked block, all from benchmark run
`benchmark_20261002T213200Z`; the files are under `results/<block>/<run id>/`.

| Date (UTC) | Run ID | Git commit | Domain | Engine | Diagnostic/Block |
|---|---|---|---|---|---|
| 2026-10-05 | `20261005T061532277614Z_design_space_collect` | `b7ed8fc0bde0` | - | - | design_space |
| 2026-10-05 | `20261005T061638210769Z_design_space_collect` | `b7ed8fc0bde0` | - | - | design_space_confirmation |
| 2026-10-05 | `20261005T061744226858Z_scaling_collect` | `b7ed8fc0bde0` | - | - | scaling |
| 2026-10-05 | `20261002T214637421141Z_oracle_verification_environment_airquality` | `b7ed8fc0bde0` | environment_airquality | - | oracle_verification |
| 2026-10-05 | `20261002T214619296102Z_oracle_verification_transportation_taxi` | `b7ed8fc0bde0` | transportation_taxi | - | oracle_verification |
| 2026-10-05 | `20261002T214700270348Z_oracle_verification_datacenter_drives` | `b7ed8fc0bde0` | datacenter_drives | - | oracle_verification |
| 2026-10-05 | `20261002T214609769142Z_oracle_verification_finance_markets` | `b7ed8fc0bde0` | finance_markets | - | oracle_verification |
| 2026-10-05 | `20261002T214604175763Z_oracle_verification_sports_football` | `b7ed8fc0bde0` | sports_football | - | oracle_verification |
| 2026-10-05 | `20261002T215412Z_boundary` | `b7ed8fc0bde0` | - | - | boundary |
| 2026-10-05 | `20261002T215643Z_all_mutants_detection` | `b7ed8fc0bde0` | - | - | mutation |
| 2026-10-05 | `20261002T222337Z_all_reference_d7s_sweep` | `b7ed8fc0bde0` | - | - | d7s_sweep |
| 2026-10-05 | `20261002T215413059424Z_all_native_semantics` | `b7ed8fc0bde0` | - | - | native_semantics |
| 2026-10-05 | `20261002T223330Z_query_family` | `b7ed8fc0bde0` | - | - | query_family |
| 2026-10-05 | `20261002T235755Z_disorder_sweep` | `b7ed8fc0bde0` | - | - | disorder_sweep |
| 2026-10-05 | `20261003T004154Z_parallelism_sweep` | `b7ed8fc0bde0` | - | - | parallelism_sweep |
| 2026-10-05 | `20261003T005903Z_formulation_cost` | `b7ed8fc0bde0` | - | - | formulation_cost |
