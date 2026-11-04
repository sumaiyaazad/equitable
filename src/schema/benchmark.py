"""Shared configuration and row selection for fixed benchmark runs."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Union

import numpy as np
import yaml


def load_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def effective_perturbation(
    root: Path,
    domain: str,
    defaults: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return global perturbation defaults plus one domain's overrides."""
    base = defaults
    if base is None:
        base = load_yaml(root / "config" / "default.yaml").get("perturbation", {})
    domain_config = load_yaml(root / "config" / "domains" / f"{domain}.yaml")
    return {**base, **(domain_config.get("perturbation") or {})}


def row_positions(
    total_rows: int,
    requested_rows: int,
    mode: str = "prefix",
) -> Union[slice, np.ndarray]:
    """Return deterministic positions for one benchmark row-count setting."""
    if requested_rows <= 0 or requested_rows >= total_rows:
        return slice(None)
    if mode == "prefix":
        return slice(0, requested_rows)
    if mode == "uniform":
        return np.linspace(
            0, total_rows - 1, requested_rows, dtype=np.int64)
    raise ValueError(f"unknown benchmark row-selection mode: {mode}")
