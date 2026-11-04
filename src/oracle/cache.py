"""Provenance-keyed cache for independently verified oracle tables."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd

from src.oracle.comparator import compare_bucket_tables
from src.oracle.duckdb_oracle import (
    build_oracle_tables_from_rows as build_duckdb_tables,
)
from src.oracle.pandas_oracle import (
    build_oracle_tables_from_rows as build_pandas_tables,
)
from src.provenance import run_provenance

CACHE_VERSION = 1


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _cache_spec(
    root: Path,
    source_sha256: str,
    selection: Dict[str, Any],
    restatement_aware: bool,
    co_oracle: bool,
) -> Dict[str, Any]:
    provenance = run_provenance(root)
    return {
        "cache_version": CACHE_VERSION,
        "source_sha256": source_sha256,
        "selection": selection,
        "restatement_aware": restatement_aware,
        "co_oracle": co_oracle,
        "git_commit": provenance["git_commit"],
        "benchmark_config_sha256": provenance["benchmark_config_sha256"],
        "dependency_lock_sha256": provenance["dependency_lock_sha256"],
    }


def _key(spec: Dict[str, Any]) -> str:
    payload = json.dumps(
        spec, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _load(cache_dir: Path, spec: Dict[str, Any]) -> Tuple[
        Dict[str, pd.DataFrame], Dict[str, Any] | None, Dict[str, Any]]:
    metadata = json.loads((cache_dir / "metadata.json").read_text())
    if metadata.get("spec") != spec:
        raise ValueError(f"oracle cache specification mismatch: {cache_dir}")
    pandas_tables = {
        "hourly": pd.read_parquet(cache_dir / "hourly.parquet"),
        "daily": pd.read_parquet(cache_dir / "daily.parquet"),
    }
    timing = {
        "oracle_build_s": float(metadata["oracle_build_s"]),
        "oracle_cache_hit": True,
        "oracle_cache_key": cache_dir.name,
        "oracle_cache_path": str(cache_dir),
        "oracle_cache_files_sha256": metadata["files_sha256"],
    }
    return pandas_tables, metadata.get("co_oracle_result"), timing


def _write(
    cache_root: Path,
    cache_dir: Path,
    key: str,
    spec: Dict[str, Any],
    pandas_tables: Dict[str, pd.DataFrame],
    co_result: Dict[str, Any] | None,
    build_s: float,
) -> Dict[str, Any]:
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{key}.", dir=cache_root))
    try:
        hourly_path = temp_dir / "hourly.parquet"
        daily_path = temp_dir / "daily.parquet"
        pandas_tables["hourly"].to_parquet(hourly_path, index=False)
        pandas_tables["daily"].to_parquet(daily_path, index=False)
        metadata = {
            "spec": spec,
            "oracle_build_s": build_s,
            "co_oracle_result": co_result,
            "files_sha256": {
                "hourly.parquet": _sha256(hourly_path),
                "daily.parquet": _sha256(daily_path),
            },
        }
        (temp_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n")
        os.rename(temp_dir, cache_dir)
        return metadata
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)


def store_oracle_tables(
    pandas_tables: Dict[str, pd.DataFrame],
    co_result: Dict[str, Any],
    *,
    root: Path,
    source_sha256: str,
    selection: Dict[str, Any],
    restatement_aware: bool,
    build_s: float,
) -> Dict[str, Any]:
    """Store oracle tables already built by a full verification pass."""
    spec = _cache_spec(
        root, source_sha256, selection, restatement_aware, True)
    key = _key(spec)
    cache_root = root / "data" / "oracle_cache"
    cache_dir = cache_root / key
    cache_root.mkdir(parents=True, exist_ok=True)
    with (cache_root / f"{key}.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if (cache_dir / "metadata.json").is_file():
            return _load(cache_dir, spec)[2]
        metadata = _write(
            cache_root, cache_dir, key, spec, pandas_tables, co_result,
            build_s)
    return {
        "oracle_build_s": build_s,
        "oracle_cache_hit": False,
        "oracle_cache_key": key,
        "oracle_cache_path": str(cache_dir),
        "oracle_cache_files_sha256": metadata["files_sha256"],
    }


def build_or_load_oracle(
    df: pd.DataFrame,
    *,
    root: Path,
    source_sha256: str,
    selection: Dict[str, Any],
    restatement_aware: bool,
    co_oracle: bool,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any] | None, Dict[str, Any]]:
    """Build each expected table once, then reuse its immutable cache artifact."""
    spec = _cache_spec(
        root, source_sha256, selection, restatement_aware, co_oracle)
    key = _key(spec)
    cache_root = root / "data" / "oracle_cache"
    cache_dir = cache_root / key
    cache_root.mkdir(parents=True, exist_ok=True)

    lock_path = cache_root / f"{key}.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if (cache_dir / "metadata.json").is_file():
            return _load(cache_dir, spec)

        started = time.perf_counter()
        oracle_df = df.copy()
        oracle_df["event_time_utc"] = pd.to_datetime(
            oracle_df["event_time_utc"], utc=True,
        ).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        rows = oracle_df.to_dict("records")
        pandas_tables = build_pandas_tables(
            rows, restatement_aware=restatement_aware,
            include_quantiles=False)

        co_result = None
        if co_oracle:
            duckdb_tables = build_duckdb_tables(
                rows, restatement_aware=restatement_aware)
            hourly = compare_bucket_tables(
                pandas_tables["hourly"], duckdb_tables["hourly"])
            daily = compare_bucket_tables(
                pandas_tables["daily"], duckdb_tables["daily"])
            co_result = {
                "hourly_equivalent": hourly["equivalent"],
                "daily_equivalent": daily["equivalent"],
                "hourly_mismatched_keys": hourly["mismatched_keys"],
                "daily_mismatched_keys": daily["mismatched_keys"],
                "agree": bool(hourly["equivalent"] and daily["equivalent"]),
            }

        build_s = time.perf_counter() - started
        metadata = _write(
            cache_root, cache_dir, key, spec, pandas_tables, co_result,
            build_s)

    timing = {
        "oracle_build_s": build_s,
        "oracle_cache_hit": False,
        "oracle_cache_key": key,
        "oracle_cache_path": str(cache_dir),
        "oracle_cache_files_sha256": metadata["files_sha256"],
    }
    return pandas_tables, co_result, timing


def build_or_load_hopping_oracle(
    df: pd.DataFrame,
    *,
    root: Path,
    source_sha256: str,
    selection: Dict[str, Any],
    co_oracle: bool,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any] | None, Dict[str, Any]]:
    """Build or reuse D8's cross-checked hopping-window tables.

    The ordinary cache specification describes the tumbling oracle. The same
    input prefix also has a hopping oracle, so this specification names the
    oracle kind and window geometry and the two can never share a key.
    """
    from src.oracle.hopping import (GRAIN_GEOMETRY, build_hopping_table,
                                    build_hopping_table_sql)

    spec = _cache_spec(root, source_sha256, selection, False, co_oracle)
    spec["oracle_kind"] = "hopping"
    spec["grain_geometry_hours"] = {
        grain: list(GRAIN_GEOMETRY[grain]) for grain in sorted(GRAIN_GEOMETRY)
    }
    key = _key(spec)
    cache_root = root / "data" / "oracle_cache"
    cache_dir = cache_root / key
    cache_root.mkdir(parents=True, exist_ok=True)

    with (cache_root / f"{key}.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if (cache_dir / "metadata.json").is_file():
            return _load(cache_dir, spec)

        started = time.perf_counter()
        pandas_tables = {
            grain: build_hopping_table(df, grain=grain)
            for grain in ("hourly", "daily")
        }
        co_result = None
        if co_oracle:
            comparisons = {
                grain: compare_bucket_tables(
                    pandas_tables[grain],
                    build_hopping_table_sql(df, grain=grain),
                )
                for grain in ("hourly", "daily")
            }
            co_result = {
                "hourly_equivalent": comparisons["hourly"]["equivalent"],
                "daily_equivalent": comparisons["daily"]["equivalent"],
                "hourly_mismatched_keys": comparisons["hourly"]["mismatched_keys"],
                "daily_mismatched_keys": comparisons["daily"]["mismatched_keys"],
                "agree": bool(
                    comparisons["hourly"]["equivalent"]
                    and comparisons["daily"]["equivalent"]
                ),
            }

        build_s = time.perf_counter() - started
        metadata = _write(
            cache_root, cache_dir, key, spec, pandas_tables, co_result, build_s)

    timing = {
        "oracle_build_s": build_s,
        "oracle_cache_hit": False,
        "oracle_cache_key": key,
        "oracle_cache_path": str(cache_dir),
        "oracle_cache_files_sha256": metadata["files_sha256"],
    }
    return pandas_tables, co_result, timing
