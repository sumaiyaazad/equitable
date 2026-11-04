"""Efficient reads and manifest-backed identities for benchmark Parquet files."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def _valid_sha256(value: object) -> Optional[str]:
    text = str(value or "").lower()
    if len(text) == 64 and all(char in "0123456789abcdef" for char in text):
        return text
    return None


def artifact_sha256(path: Path) -> str:
    """Read the producer manifest identity, falling back to a streamed hash."""
    path = path.resolve()
    manifest_path = path.parent / "manifest.json"
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text())
            if path.name == "log.parquet":
                value = _valid_sha256(manifest.get("sha256"))
            else:
                entry = next(
                    (item for item in manifest.get("fixtures", [])
                     if item.get("name") == path.name),
                    None,
                )
                value = _valid_sha256((entry or {}).get("sha256"))
            if value is not None:
                return value
        except (OSError, ValueError, TypeError):
            pass

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_parquet_prefix(path: Path, requested_rows: int) -> pd.DataFrame:
    """Read only the requested leading rows when a full read is unnecessary."""
    parquet = pq.ParquetFile(path)
    total_rows = int(parquet.metadata.num_rows)
    if requested_rows <= 0 or requested_rows >= total_rows:
        return pd.read_parquet(path)

    batches = []
    rows = 0
    for batch in parquet.iter_batches(batch_size=requested_rows):
        batches.append(batch)
        rows += batch.num_rows
        if rows >= requested_rows:
            break
    return pa.Table.from_batches(batches).slice(0, requested_rows).to_pandas()
