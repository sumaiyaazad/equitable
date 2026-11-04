# Datasets: sources, licenses and checksums

One entry per domain. The checksum and row count come from
`data/normalized/<domain>/manifest.json`, which the ingest script writes.
Every base log is a deterministic uniform
selection of 1,000,000 events, evenly spaced after sorting the pinned source
window by `(event_time_utc, dedupe_id)`, so the log is totally ordered by
event time with a stable identity tie-break. The manifest records the selection
rule and the checksums of the input files, so anyone who runs the same script
obtains the same log byte for byte. It also records the commit of the ingest
code in the development repository; that commit does not resolve in the
public tree, and the checksums are what verify a rebuilt log.

| Domain | Source | License | Fetch script | Ingested (UTC) | sha256 of the normalized log | Rows |
|---|---|---|---|---|---|---|
| finance_markets | Binance spot monthly trades, 2024-06, 6 symbols (data.binance.vision); stands in for IEX DEEP | Binance public data terms (free) | `scripts/ingest/ingest_finance_markets.py` | 2026-10-02 | `fd0a6e54...8ec9e16f` | 1,000,000 |
| transportation_taxi | NYC TLC yellow-taxi trip records, 2024-06 (d37ci6vzurychx.cloudfront.net) | NYC Open Data | `scripts/ingest/ingest_transportation_taxi.py` | 2026-10-02 | `a4490253...be302f94` | 1,000,000 |
| sports_football | Metrica Sports sample tracking data, game 1 (github.com/metrica-sports/sample-data); stands in for DEBS GC 2013 | no license file | `scripts/ingest/ingest_sports_football.py` | 2026-10-02 | `5d909a92...7ed81225` | 1,000,000 |
| datacenter_drives | Backblaze Drive Stats, Q2 2024 archive, month 2024-06 (backblaze.com) | free with attribution | `scripts/ingest/ingest_datacenter_drives.py` | 2026-10-02 | `5b1d3e24...c1e96a55` | 1,000,000 |
| environment_airquality | EPA AirNow hourly archive, 2024-06-01 to 2024-07-01 (files.airnowtech.org) | public (US EPA) | `scripts/ingest/ingest_environment_airquality.py` | 2026-10-02 | `3a837b49...eb877398` | 1,000,000 |

The table shows the first and last eight hexadecimal characters of each
checksum; the full checksums are in the manifests.

No raw input is redistributed. Each ingest script fetches its source from
the publisher named above, under that publisher's own terms, and the
repository ships only the manifests and checksums that prove a fetched copy
is the one the paper measured.

Key cardinality and metrics, from the manifests: finance 6 entities
(trade_price, trade_size); taxi 252 (fare, trip_distance, duration);
football 29 (pos_x, pos_y, speed, distance); drives 209,566 (temperature,
power_on_hours, realloc_sectors); air quality 2,096 (pm25, ozone).

## Substitutions

Two sources stand in for the archives of the original lineage, and the
substitution is recorded here and in the domain configuration. The finance
workload uses the Binance spot trade tape in place of IEX DEEP, which needs
an account; Binance publishes free, reproducible URLs. The football workload
uses Metrica Sports sample tracking data in place of the DEBS Grand Challenge
2013 sensor archive, which has no stable public mirror. Both keep the domain
and the aggregate shapes of the original. The paper names only the sources it
used (Table VI).

The Metrica repository publishes no license file; it asks for only
responsible use and acknowledgement. As for every domain, the ingest script
fetches the data and the recorded checksum proves that a fetched copy is the
identical log.

## Layout and rebuild

```
data/raw/<domain>/          raw downloaded source files           (not tracked)
data/normalized/<domain>/   log.parquet (not tracked) + manifest.json (tracked)
data/fixtures/<domain>/     diagnostic fixtures (not tracked) + manifest.json (tracked)
data/oracle_cache/          cached oracle tables                  (not tracked)
```

Every domain is produced the same way: the ingest script downloads the raw
source into `data/raw/<domain>/`, normalizes it to the shared relational
schema (`event_time_utc`, `entity`, `metric`, `value`, `dedupe_id`,
`processing_order`, `topic`, `source_record_type`; see [docs/SEMANTICS.md](SEMANTICS.md)) at
`data/normalized/<domain>/log.parquet`, and writes the manifest. The parquet logs are not tracked, because the ingest scripts regenerate them;
the small manifests are tracked. The
per-domain field mappings are in `config/domains/<domain>.yaml`.

All five domains are rebuilt and verified against their manifests by

```bash
./reproduce_all.sh data
```

The quick path, `./reproduce_all.sh quick`, builds the taxi domain
alone and checks its checksum the same way.
