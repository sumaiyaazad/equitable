"""Domain configuration: how one domain's raw records map to the neutral schema.

Every domain (finance_markets, transportation_taxi, sports_football, datacenter_drives, environment_airquality) provides one
DomainConfig. The same generic normalizer (``normalize.py``) then produces the
identical neutral schema for all of them, which is what makes the pipeline
domain-independent.

Two raw layouts are supported:
  * "wide": one raw record carries several measured quantities in separate
    columns (e.g. an air-quality reading with pm25 and ozone columns). Each
    listed column explodes into its own neutral row.
  * "long": one raw record already has a metric-name column and a value column
    (e.g. a trade with metric='trade_price', value=101.2).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class DomainConfig:
    name: str
    entity_field: str                       # raw column -> entity
    event_time_field: str                   # raw column -> event_time_utc
    source_id_field: Optional[str] = None   # stable source id; else deterministic input row
    layout: str = "wide"                    # "wide" | "long"

    # wide layout: {canonical_metric_name: raw_value_column}
    metric_fields: Dict[str, str] = field(default_factory=dict)

    # long layout:
    metric_field: Optional[str] = None      # raw column holding the metric name
    value_field: Optional[str] = None       # raw column holding the value
    metric_map: Dict[str, str] = field(default_factory=dict)  # raw metric name -> canonical

    topic_field: Optional[str] = None       # raw column -> topic (optional)
    topic_const: Optional[str] = None       # constant topic (optional)

    def __post_init__(self) -> None:
        if self.layout not in ("wide", "long"):
            raise ValueError(f"{self.name}: layout must be 'wide' or 'long', got {self.layout!r}")
        if self.layout == "wide" and not self.metric_fields:
            raise ValueError(f"{self.name}: wide layout requires metric_fields")
        if self.layout == "long" and not (self.metric_field and self.value_field):
            raise ValueError(f"{self.name}: long layout requires metric_field and value_field")

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DomainConfig":
        norm = d.get("normalize", d)  # allow either a bare dict or a {'normalize': {...}} block
        return cls(
            name=d.get("domain") or d.get("name") or norm.get("name"),
            entity_field=norm["entity_field"],
            event_time_field=norm["event_time_field"],
            source_id_field=norm.get("source_id_field"),
            layout=norm.get("layout", "wide"),
            metric_fields=norm.get("metric_fields", {}) or {},
            metric_field=norm.get("metric_field"),
            value_field=norm.get("value_field"),
            metric_map=norm.get("metric_map", {}) or {},
            topic_field=norm.get("topic_field"),
            topic_const=norm.get("topic_const"),
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> "DomainConfig":
        try:
            import yaml
        except ImportError as exc:  # pragma: no cover
            raise ImportError("PyYAML is required to read domain configs (pip install pyyaml)") from exc
        with open(path, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
        if "normalize" not in data:
            raise ValueError(
                f"{path}: missing a 'normalize:' block mapping raw source fields to the "
                f"neutral schema. This block is added per domain in Step 2 (ingest)."
            )
        return cls.from_dict(data)
