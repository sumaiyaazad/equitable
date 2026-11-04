"""Engine adapters: one uniform interface over every system under test.

    from src.adapters import get_adapter, ADAPTER_NAMES
    eng = get_adapter("reference")          # -> ReferenceAdapter instance

Adapters are imported lazily so that pulling in one engine never requires the
others' drivers (psycopg2 / pyflink / pyspark / confluent-kafka / requests).
Only `base` (pandas-only) is imported eagerly.
"""
from __future__ import annotations

from typing import Any, Dict, Tuple

from .base import (
    EngineAdapter,
    NativeNotExpressible,
    IngestStats,
    KEY_COLUMNS,
    VIEW_COLUMNS,
    GRAINS,
    VARIANTS,
    canonical_input,
)

# name -> (module under this package, class name). Kept as strings for laziness.
_REGISTRY: Dict[str, Tuple[str, str]] = {
    "reference":   ("reference",  "ReferenceAdapter"),
    "materialize": ("materialize", "MaterializeAdapter"),
    "risingwave":  ("risingwave",  "RisingWaveAdapter"),
    "ksqldb":      ("ksqldb",      "KsqlDbAdapter"),
    # Flink and Spark are scored as checkpointed clusters (docker/streaming-engines.yml).
    "flink":       ("flink_cluster", "FlinkClusterAdapter"),
    "spark":       ("spark_cluster", "SparkClusterAdapter"),
    "feldera":     ("feldera",     "FelderaAdapter"),
}

#: canonical ordering used across smoke output and (later) the design-space table.
ADAPTER_NAMES: Tuple[str, ...] = tuple(_REGISTRY)

# Seeded-defect adapters for the detection-power control (src/adapters/mutants.py).
# Deliberately NOT in ADAPTER_NAMES: they are oracles for validating the
# diagnostics, not systems under test, and must never enter the design-space map.
_MUTANT_REGISTRY: Dict[str, Tuple[str, str]] = {
    "mutant_no_dedup":          ("mutants", "NoDedupMutant"),
    "mutant_order_sensitive":   ("mutants", "OrderSensitiveMutant"),
    "mutant_ulp_drift":         ("mutants", "UlpDriftMutant"),
    "mutant_drop_late":         ("mutants", "DropLateMutant"),
    "mutant_ignore_supersedes": ("mutants", "IgnoreSupersedesMutant"),
    "mutant_recovery_loss":     ("mutants", "RecoveryLossMutant"),
    "mutant_topk_rank":         ("mutants", "TopKRankMutant"),
}

MUTANT_NAMES: Tuple[str, ...] = tuple(_MUTANT_REGISTRY)

# Alternate DEPLOYMENTS of an engine already in _REGISTRY, kept out of
# ADAPTER_NAMES for the same reason the mutants are: they must never appear as a
# separate row in the design-space map. The in-process PyFlink and PySpark
# adapters were the scored deployments before the clusters replaced them; they
# stay available for comparison and for reproducing earlier campaigns.
_DEPLOYMENT_REGISTRY: Dict[str, Tuple[str, str]] = {
    "flink_inprocess": ("flink", "FlinkAdapter"),
    "spark_inprocess": ("spark", "SparkAdapter"),
}

DEPLOYMENT_NAMES: Tuple[str, ...] = tuple(_DEPLOYMENT_REGISTRY)


def get_adapter(name: str, **options: Any) -> EngineAdapter:
    """Instantiate the adapter registered under `name` (not yet connected)."""
    entry = (_REGISTRY.get(name) or _MUTANT_REGISTRY.get(name)
             or _DEPLOYMENT_REGISTRY.get(name))
    if entry is None:
        raise KeyError(
            f"unknown engine {name!r}; known: {ADAPTER_NAMES} "
            f"(+ mutants {MUTANT_NAMES}, deployments {DEPLOYMENT_NAMES})")
    module_name, class_name = entry
    import importlib
    module = importlib.import_module(f"{__name__}.{module_name}")
    return getattr(module, class_name)(**options)


__all__ = [
    "EngineAdapter", "NativeNotExpressible", "IngestStats", "get_adapter", "ADAPTER_NAMES",
    "MUTANT_NAMES", "DEPLOYMENT_NAMES",
    "KEY_COLUMNS", "VIEW_COLUMNS", "GRAINS", "VARIANTS", "canonical_input",
]
