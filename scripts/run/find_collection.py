#!/usr/bin/env python3
"""Find the newest collector artifact for one exact invocation."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", required=True)
    parser.add_argument("--invocation-id", required=True)
    parser.add_argument("--domain")
    parser.add_argument(
        "--required-engines", default="",
        help="Comma-separated required_engines set the artifact must match.")
    args = parser.parse_args()
    required_engines = {
        engine.strip() for engine in args.required_engines.split(",")
        if engine.strip()
    }
    matches = []
    for path in (ROOT / "runs").glob("*/summary.json"):
        try:
            summary = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if (summary.get("kind") == args.kind
                and summary.get("invocation_id") == args.invocation_id
                and (args.domain is None
                     or summary.get("domain") == args.domain)
                and (
                    not required_engines
                    or set(summary.get("required_engines") or [])
                        == required_engines
                )
                and summary.get("artifact_valid") is True):
            matches.append(path.parent)
    if not matches:
        print(f"no valid {args.kind} for {args.invocation_id}")
        return 1
    print(max(matches, key=lambda path: path.name))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
