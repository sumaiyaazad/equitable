"""Shared helpers for domain ingest scripts.

Every domain ingester (finance_markets, transportation_taxi, sports_football, datacenter_drives, environment_airquality) parses its
own raw source into records, then calls ``write_normalized_log`` here to emit
the identical neutral schema + a provenance manifest. This is what makes the
normalized-log artifact uniform across domains.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Union

import numpy as np
import pandas as pd

from src.schema import NORMALIZED_COLUMNS, EVENT_TIME, PROCESSING_ORDER, INGEST_TIME, DEDUPE_ID
from src.provenance import git_commit as provenance_git_commit, git_dirty as provenance_git_dirty


def _git_commit() -> str:
    return provenance_git_commit()


def _sha256_file(path: Path, buf: int = 1 << 20) -> tuple[str, int]:
    """Streaming sha256 of a file (any size). Returns (hexdigest, byte_count)."""
    h = hashlib.sha256()
    n = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(buf)
            if not chunk:
                break
            h.update(chunk)
            n += len(chunk)
    return h.hexdigest(), n


def _hash_inputs(paths: Sequence[str]) -> Dict[str, Any]:
    """Hash the raw input file(s) an ingester read, for end-to-end reproducibility
    (the manifest records the input SHA-256). Returns a combined, order-independent
    `input_sha256` over all files plus a count; includes the per-file list when
    the set is small enough to keep the manifest readable."""
    files = []
    for p in paths:
        pp = Path(p)
        if pp.exists():
            sha, nbytes = _sha256_file(pp)
            files.append({"name": pp.name, "sha256": sha, "bytes": nbytes})
    combined = (
        hashlib.sha256("\n".join(sorted(f"{f['name']}:{f['sha256']}" for f in files)).encode()).hexdigest()
        if files else None
    )
    out: Dict[str, Any] = {"input_sha256": combined, "input_file_count": len(files)}
    if 0 < len(files) <= 50:                 # keep per-file detail for small sets only
        out["input_files"] = files
    return out


def write_normalized_log(
    rows: Union[Sequence[Dict[str, Any]], pd.DataFrame],
    domain: str,
    *,
    source: str,
    extra_manifest: Optional[Dict[str, Any]] = None,
    out_root: str = "data/normalized",
    max_rows: Optional[int] = None,
    select: str = "uniform",
    input_paths: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Write data/normalized/<domain>/log.parquet in canonical column order,
    plus manifest.json (sha256, row count, git commit, source, entities/metrics).

    If ``max_rows`` is set and the normalized log exceeds it, keep a deterministic
    slice: order by the TOTAL order (event_time_utc, dedupe_id), then keep evenly
    spaced rows across the full window (``select='uniform'``, default), the latest
    rows (``tail``), or the earliest rows (``head``). Kept rows are written
    in the total order (event_time_utc, dedupe_id) with processing_order
    renumbered 0..N-1. This is the domain-independent way to pin every workload
    to the same size without silently narrowing its declared time range. The
    exact rule is recorded in the manifest.

    Why event-time order rather than source order: none of the five source
    orders is an arrival process. Finance keeps trades in hash-sample order
    (random within a symbol), drives read the daily files newest first,
    football is player-major, air quality is station-major, and only taxi
    carries a genuine file order. Source order is an artifact of how each
    archive is laid out; keeping it would make the base log disordered before
    any diagnostic touched it and would let the arrival-ordered formulation
    diverge on cells whose intervention cannot move the latest row (D1, D3, D5,
    D8 on the disordered domains). The benchmark's disorder comes from exactly
    one place, the controlled D2 and D4 interventions, and the paper states
    that the base log is event-time ordered.
    Returns the manifest dict."""
    if select not in ("uniform", "tail", "head"):
        raise ValueError(f"select must be 'uniform', 'tail', or 'head', got {select!r}")
    df = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))

    # ensure every canonical column exists, in canonical order
    for col in NORMALIZED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[NORMALIZED_COLUMNS]

    # deterministic base-size cap: last/first max_rows by (event_time, order)
    rows_before_cap = int(len(df))
    selection = None
    if max_rows and rows_before_cap > max_rows:
        ordered = df.sort_values([EVENT_TIME, DEDUPE_ID], kind="mergesort")
        if select == "uniform":
            positions = np.linspace(0, rows_before_cap - 1, num=max_rows, dtype=np.int64)
            kept = ordered.iloc[positions]
            selection = (
                f"uniform {max_rows} evenly spaced by (event_time_utc, dedupe_id)")
        else:
            kept = ordered.tail(max_rows) if select == "tail" else ordered.head(max_rows)
            selection = f"{select} {max_rows} by (event_time_utc, dedupe_id)"
        df = kept[NORMALIZED_COLUMNS]

    # Replay order is the total order (event_time_utc, dedupe_id), capped or
    # not; see the docstring. processing_order is renumbered to match.
    df = df.sort_values([EVENT_TIME, DEDUPE_ID], kind="mergesort").reset_index(drop=True)
    df[PROCESSING_ORDER] = range(len(df))
    df = df[NORMALIZED_COLUMNS]

    # Determinism: ingest_time_utc is wall-clock at generation time, which would
    # make the log non-reproducible (identical input -> different bytes every run).
    # For a static historical archive there is no real ingest time -- the true
    # ingest timestamp is assigned at REPLAY, not baked into the artifact. Null it
    # so (input, window, code) fully determine the output bytes. The engine treats
    # a null ingest_time as absent (health fields stay None); aggregation is unaffected.
    df = df.assign(**{INGEST_TIME: None})

    out_dir = Path(out_root) / domain
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "log.parquet"
    df.to_parquet(log_path, index=False)

    sha = hashlib.sha256(log_path.read_bytes()).hexdigest()
    manifest: Dict[str, Any] = {
        "domain": domain,
        "source": source,
        "rows": int(len(df)),
        "entities": int(df["entity"].nunique()),
        "metrics": sorted([m for m in df["metric"].dropna().unique().tolist()]),
        "sha256": sha,
        "columns": NORMALIZED_COLUMNS,
        "contract_version": 2,
        "dedupe_identity": "source_observation",
        "git_commit": _git_commit(),
        "git_dirty": _git_dirty(),
        "written_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "log_path": str(log_path),
        "rows_before_cap": rows_before_cap,
        "selection": selection,
    }
    if extra_manifest:
        manifest.update(extra_manifest)
    if input_paths:
        manifest.update(_hash_inputs(input_paths))

    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest

def _git_dirty() -> bool:
    return provenance_git_dirty()
