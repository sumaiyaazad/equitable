#!/usr/bin/env python3
"""
run_boundary.py -- Boundary-validation diagnostic (Tab 9).

Property under test: malformed / out-of-contract rows injected at the ingest
boundary must be REJECTED before any state mutation, so exact aggregates are
byte-identical to the clean-log oracle. This is a schema-layer guarantee
(src/schema/record.py::validate_row + engine normalization), so it runs entirely
in-process against the reference engine and the pandas oracle -- no containers.

For each domain it:
  1. loads a prefix of the normalized log (clean baseline),
  2. builds baseline oracle aggregates,
  3. injects N malformed rows (config perturbation.malformed_rows), each breaking
     exactly one contract rule (missing required field / non-numeric value /
     unparseable timestamp),
  4. rebuilds aggregates over clean+malformed and checks:
       rows_malformed == N            (all injected rows rejected)
       hourly/daily aggregates equal baseline   (no state mutation),
  5. cross-checks the ExactEngine rejects the same N rows.

Inputs  : data/normalized/<domain>/log.parquet, config/default.yaml
Outputs : runs/<UTC>_boundary/{summary.json, boundary.csv}
Runtime : ~1-2 min for 5 domains at the default prefix
Run as  : python scripts/run/run_boundary.py [--max-rows 50000] [--domain all]
Promote : python scripts/results/promote_results.py runs/<run_id> --block boundary
"""
from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import pyarrow.parquet as pq  # noqa: E402

from src.engine.exact_engine import ExactEngine  # noqa: E402
from src.oracle.pandas_oracle import build_oracle_tables_from_rows  # noqa: E402
from src.provenance import run_provenance  # noqa: E402


def _load_prefix(path: Path, max_rows: int) -> List[Dict[str, Any]]:
    """Read the first `max_rows` rows (0 = all). The normalized log is stored in
    arrival order with processing_order renumbered 0..N-1, so the file prefix is
    exactly the processing_order prefix -- no need to load the full 1M rows."""
    pf = pq.ParquetFile(str(path))
    if max_rows and max_rows > 0:
        batches = []
        got = 0
        for batch in pf.iter_batches(batch_size=min(max_rows, 65536)):
            batches.append(batch)
            got += batch.num_rows
            if got >= max_rows:
                break
        import pyarrow as pa
        frame = pa.Table.from_batches(batches).to_pandas()
        frame = frame.iloc[:max_rows]
    else:
        frame = pf.read().to_pandas()
    return frame.to_dict(orient="records")

DOMAINS = ["datacenter_drives", "environment_airquality", "finance_markets",
           "sports_football", "transportation_taxi"]


def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _malformed_rows(template: Dict[str, Any], n: int) -> List[Dict[str, Any]]:
    """N rows, each a copy of a real row broken in one distinct contract-violating
    way. Unique dedupe_ids so a rejection is attributable to malformation, not
    de-duplication."""
    breakers = [
        ("missing_event_time",  lambda r: r.update(event_time_utc=None)),
        ("unparseable_time",    lambda r: r.update(event_time_utc="garbage-timestamp")),
        ("empty_entity",         lambda r: r.update(entity="")),
        ("nonnumeric_value",     lambda r: r.update(value="not_a_number")),
        ("invalid_source_type",  lambda r: r.update(source_record_type="control")),
        ("missing_metric",       lambda r: r.update(metric=None)),
    ]
    if n < len(breakers):
        raise ValueError(
            f"malformed_rows={n} is fewer than the {len(breakers)} violation "
            "classes, so at least one would never be injected and the control "
            "would pass without testing it")
    if n % len(breakers):
        raise ValueError(
            f"malformed_rows={n} is not a multiple of the {len(breakers)} "
            "violation classes, so they would be injected unequally and a "
            "per-class rejection rate would not be comparable")
    out: List[Dict[str, Any]] = []
    for i in range(n):
        row = copy.deepcopy(template)
        row["dedupe_id"] = f"__boundary_malformed_{i}__"
        row["processing_order"] = (template.get("processing_order") or 0) + 10_000_000 + i
        name, mutate = breakers[i % len(breakers)]
        mutate(row)
        row["_break"] = name
        out.append(row)
    return out


def _agg_signature(tables: Dict[str, Any]) -> str:
    """Order-independent content hash of the hourly+daily aggregate tables."""
    h = hashlib.sha256()
    for key in ("hourly", "daily"):
        frame: pd.DataFrame = tables[key]
        if frame.empty:
            h.update(f"{key}:empty".encode())
            continue
        cols = sorted(frame.columns)
        sort_cols = [c for c in ("bucket", "entity", "metric") if c in frame.columns]
        ordered = frame[cols].sort_values(sort_cols).reset_index(drop=True)
        h.update(f"{key}:".encode())
        h.update(pd.util.hash_pandas_object(ordered, index=False).values.tobytes())
    return h.hexdigest()


def _run_domain(domain: str, max_rows: int, n_malformed: int) -> Dict[str, Any]:
    log = ROOT / "data" / "normalized" / domain / "log.parquet"
    if not log.exists():
        return {"domain": domain, "error": f"missing log: {log}"}

    rows = _load_prefix(log, max_rows)
    rows.sort(key=lambda r: (r.get("processing_order") is None,
                             r.get("processing_order", 0)))

    base = build_oracle_tables_from_rows(rows, include_quantiles=False)
    base_sig = _agg_signature(base)

    malformed = _malformed_rows(rows[len(rows) // 2], n_malformed)
    classes = sorted({row["_break"] for row in malformed})
    combined = list(rows) + malformed
    after = build_oracle_tables_from_rows(combined, include_quantiles=False)
    after_sig = _agg_signature(after)

    # Cross-check the online engine rejects exactly the malformed rows too.
    engine = ExactEngine()
    engine.reset()
    engine.ingest_rows(rows)
    counts = engine.ingest_rows(malformed)  # {accepted, rejected}

    rejected_oracle = after["summary"]["rows_malformed"]
    unchanged = (after_sig == base_sig
                 and after["summary"]["rows_accepted"] == base["summary"]["rows_accepted"])
    passed = (rejected_oracle == n_malformed
              and counts["accepted"] == 0
              and counts["rejected"] == n_malformed
              and unchanged)

    return {
        "domain": domain,
        "rows": len(rows),
        "injected": n_malformed,
        "rejected": int(rejected_oracle),
        "engine_rejected": int(counts["rejected"]),
        "engine_accepted_malformed": int(counts["accepted"]),
        "aggregates_unchanged": bool(unchanged),
        "violation_classes": classes,
        "violation_class_count": len(classes),
        "injected_per_class": n_malformed // len(classes),
        "pass": bool(passed),
        "input_sha256": _sha256(log),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Boundary-validation diagnostic (Tab 9).")
    ap.add_argument("--domain", default="all")
    ap.add_argument("--max-rows", type=int, default=50000,
                    help="prefix of each log to test (0 = full log). Boundary "
                         "rejection is size-independent; a prefix keeps it fast.")
    args = ap.parse_args()

    cfg = yaml.safe_load((ROOT / "config" / "default.yaml").read_text()) or {}
    n_malformed = int((cfg.get("perturbation") or {}).get("malformed_rows", 60))

    domains = DOMAINS if args.domain == "all" else [args.domain]
    started = datetime.now(timezone.utc)
    results = []
    for domain in domains:
        print(f"==== boundary : {domain} ====")
        res = _run_domain(domain, args.max_rows, n_malformed)
        if "error" in res:
            print(f"  SKIP: {res['error']}")
        else:
            print(f"  injected={res['injected']} rejected={res['rejected']} "
                  f"engine_rejected={res['engine_rejected']} "
                  f"aggregates_unchanged={res['aggregates_unchanged']} "
                  f"VERDICT: {'PASS' if res['pass'] else 'FAIL'}")
        results.append(res)
    finished = datetime.now(timezone.utc)

    ok = [r for r in results if "error" not in r]
    all_pass = bool(ok) and all(r["pass"] for r in ok)

    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    out_dir = ROOT / "runs" / f"{stamp}_boundary"
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "kind": "boundary",
        "diagnostic": "boundary",
        **run_provenance(ROOT),
        "started_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_utc": finished.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "malformed_rows": n_malformed,
        "max_rows": args.max_rows,
        "domains": [r["domain"] for r in ok],
        "rows": ok,
        "pass": all_pass,
        "artifact_valid": bool(ok) and len(ok) == len(results),
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    fields = ["domain", "rows", "injected", "rejected", "engine_rejected",
              "aggregates_unchanged", "pass"]
    with (out_dir / "boundary.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in ok:
            w.writerow({k: r.get(k) for k in fields})

    print(f"\n{'PASS' if all_pass else 'FAIL'}: boundary validation "
          f"({len(ok)}/{len(results)} domains, all_pass={all_pass})")
    print(f"output: {out_dir / 'summary.json'}")
    print(f"output: {out_dir / 'boundary.csv'}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
