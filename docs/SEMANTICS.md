# Semantics: the equivalence contract

This document is the equivalence contract. It fixes the schema of every
normalized event, the grouping key and event-time bucket of every aggregate,
the deduplication, ordering, tie-break and restatement rules, and the
comparison tolerance; every component below implements exactly these rules:

- stream normalization (`src/schema/`, `scripts/ingest/`);
- the online aggregation in the reference backend and in every engine
  adapter (`src/engine/`, `src/adapters/`);
- the two offline oracles (`src/oracle/pandas_oracle.py`,
  `src/oracle/duckdb_oracle.py`);
- replay validation and the diagnostics D1 to D8.

The contract is domain-neutral: one logical schema serves all five workloads
(`finance_markets`, `transportation_taxi`, `sports_football`,
`datacenter_drives`, `environment_airquality`).

The field-name constants are in `src/schema/record.py`; the per-domain
source-to-schema mappings are in `config/domains/<domain>.yaml`. Anything
these rules do not cover is out of scope.

## UTC policy

- All persisted timestamps use UTC.
- All bucketing uses UTC.
- Any source timestamp without an explicit timezone is interpreted in the
  source contract (recorded in the domain YAML and the ingest manifest), then
  converted and persisted as UTC before aggregation.
- `ingest_time_utc` records when the row entered this system. It is never used
  for bucket assignment.
- `event_time_utc` is the observation time and is the only timestamp used for
  hour/day bucket assignment.

## Normalized row schema

Each accepted normalized row must contain these fields
(`src/schema/record.py::NORMALIZED_COLUMNS`).

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `event_time_utc` | ISO 8601 string | yes | Observation timestamp in UTC |
| `ingest_time_utc` | ISO 8601 string | no | System ingest timestamp in UTC |
| `processing_order` | integer | yes | Row's position in the replay being executed; fixture construction and row selection reassign it after reordering |
| `entity` | string | yes | Aggregation subject: station / symbol / player / pickup zone / drive serial |
| `metric` | string | yes | Measured quantity, such as pm25, trade_price, speed, fare, or temperature |
| `value` | float | yes | Scalar measurement value |
| `topic` | string or null | yes | Source stream identifier when available |
| `dedupe_id` | string | yes | Source-identity key used by the dedupe policy |
| `source_record_type` | string | yes | Original source record type; only `data` is admitted |

Rows failing
`src/schema/record.py::validate_row` are rejected at the ingest boundary before
any state mutation (boundary-validation diagnostic).

## Per-domain metric admission

Only the metrics listed in `config/domains/<domain>.yaml` are admitted; every
other source field is rejected at normalization time. The admitted entity and
metrics per domain:

| Domain | entity | metrics |
| --- | --- | --- |
| finance_markets | symbol | trade_price, trade_size |
| transportation_taxi | pickup_zone | fare, trip_distance, duration |
| sports_football | player | pos_x, pos_y, speed, distance |
| datacenter_drives | drive_serial | temperature, power_on_hours, realloc_sectors |
| environment_airquality | station | pm25, ozone |

## Hour/day bucket definition

- `hour_bucket = floor(event_time_utc to the start of the UTC hour)`
- `day_bucket = calendar date of event_time_utc in UTC`

Examples:

- `2025-12-17T05:25:31Z -> hour_bucket=2025-12-17T05:00:00Z`
- `2025-12-17T05:25:31Z -> day_bucket=2025-12-17`

Rows are always attributed to buckets by `event_time_utc`, even if they arrive
late.

## Dedupe policy

Dedupe uses stable source-event identity (`src/schema/dedupe.py`):

`dedupe_id = SHA-256(source_namespace | source_event_id | metric)`

The namespace includes the domain and entity. `source_event_id` is a native
source identifier when one exists, such as Binance `trade_id`, and otherwise a
deterministic source-row or frame identifier. Value and event time are not
identity fields, so distinct source events with equal content remain distinct.

Operational rule:

- a repeated `dedupe_id` is a redelivery and is ignored for exact aggregates
- duplicate counters may still be recorded for diagnostics, but duplicates do
  not change exact aggregate state

## Out-of-order policy

- rows may arrive in any ingest order
- bucket membership is always determined by `event_time_utc`
- exact aggregate state must be updated for the affected bucket even when the
  row is late relative to wall clock
- `processing_order` records replay arrival position for provenance,
  diagnostics, and arrival-ordered controls. It does not determine event-time
  ties or valid D7 restatement resolution

The contract itself is replay-first and correctness-first: no watermarks or
lateness cutoffs. D4 measures inclusion of delayed rows in final unbounded
state. It does not claim watermark, trigger, or streaming-window conformance.
Historical archive storage order is not treated as measured arrival order for
D4. Its control is sorted by `(event_time_utc, dedupe_id)`, and the fixture is a
bounded relocation of selected rows from that control. The configured allowance
is domain-specific and scaled to the source interval. Every reported workload
size must have zero control rows beyond the allowance and at least one injected
target beyond it.

## Definition of `latest`

There are two official `latest` meanings.

### Bucket-local latest

For a bucket key `(hour_bucket, entity, metric)` or `(day_bucket, entity,
metric)`, `latest_value` is the `value` from the row with the maximum
`event_time_utc` in that bucket.

Tie rule: if two rows share the same `event_time_utc`, the winner has the
lexicographically larger stable `dedupe_id`. The rule is invariant to replay
arrival order.

### Unbucketed latest

For read APIs such as `get_latest`, the official latest entity/metric value is
the row with the maximum `event_time_utc` over the requested scope, same tie
rule.

This definition makes `latest` mean "most recent observation time," not merely
"last message received off the wire."

## Exact aggregate contract

Exact state is maintained per key:

- `(hour_bucket, entity, metric)`
- `(day_bucket, entity, metric)`

Supported exact fields:

- `count`
- `sum`
- `min`
- `max`
- `mean = sum / count` when `count > 0`
- `latest_value`
- `latest_event_time`

The comparator (`src/oracle/comparator.py`) requires exact value equality on
integer and string fields and ε-bounded equality on finite floating-point
fields, for every key in the union of the two outputs.

## Restatements (D7)

A *restatement* is a later row that points to and corrects one source
observation by its `dedupe_id`, through its `supersedes` field. Resolution
replaces that observation's value with the restatement's value and consumes
the restatement row; the corrected observation keeps its original identity and
event time, so `count` counts base observations only. Fixtures allow only
well-formed corrections: each pointer names a base observation, each base has
at most one restatement, and no base row carries a pointer, so resolution does
not depend on the order in which corrections arrive. Source-identity
deduplication runs before resolution, so a redelivered restatement cannot apply
twice.

## Final notes

- The normalized log is the source of truth for replay and for the oracle
  comparison.
- Every correctness claim traces back to a rule in this file.
