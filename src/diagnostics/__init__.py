"""Diagnostics D1-D7. Each module drives a system-under-test adapter
over a base log or a perturbation fixture and compares its materialized view to
the oracle. D6 (top-k, d6.py) and D7 (restatement, d7.py) carry their own oracle
semantics; D1-D4, D6, and D7 are driven by scripts/run/run_diagnostic.py. D5 uses the real crash/restart runner, while the D7S sensitivity sweep validates the bounded-deletion sketch control."""
