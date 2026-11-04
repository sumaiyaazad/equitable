#!/usr/bin/env bash
# Rebuild the five normalized logs and their fixtures from the raw archives
# already under data/raw/, after backing up the current copies.
#
#   bash scripts/ingest/regenerate_data.sh
#
# Run it once after the ingest change that replays every base log in
# (event_time_utc, dedupe_id) order, and before launching a campaign: the
# tracked manifests under data/normalized/ and data/fixtures/ change, and they
# must be committed first, or a running campaign would see a modified tree.
#
# The parameters are the ones reproduce_all.sh pins; keep them in step.
set -euo pipefail
ROOT="$(git -C "$(dirname -- "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
cd "$ROOT"
PY="$ROOT/.venv/bin/python"

backup="$HOME/vsa-data-backup-$(date -u +%Y%m%dT%H%M%SZ)"
echo "backing up data/normalized and data/fixtures to $backup"
mkdir -p "$backup"
cp -a data/normalized data/fixtures "$backup"/

echo "ingesting the five domains"
"$PY" scripts/ingest/ingest_environment_airquality.py --from-raw \
  --start 2024-06-01 --end 2024-07-01
"$PY" scripts/ingest/ingest_transportation_taxi.py --from-raw \
  --start 2024-06 --end 2024-06
"$PY" scripts/ingest/ingest_datacenter_drives.py \
  --from-zip data/raw/datacenter_drives/data_Q2_2024.zip --month 2024-06
"$PY" scripts/ingest/ingest_finance_markets.py --month 2024-06 \
  --symbols BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT ADAUSDT
"$PY" scripts/ingest/ingest_sports_football.py --game 1

echo "building the perturbation fixtures"
"$PY" scripts/perturb/build_fixtures.py

echo "verifying"
"$PY" scripts/verify/verify_data_artifacts.py --fixtures
echo "DONE: data regenerated and verified; the old copy is in $backup"
echo "next: git add -A && git commit, then ./reproduce_all.sh"
