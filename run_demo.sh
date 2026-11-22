#!/usr/bin/env bash
# Replays the full demo from a clean warehouse:
#   batch 1 full build -> batch 2 augmented build -> two full rebuilds (idempotency proof).
set -euo pipefail
cd "$(dirname "$0")"
PY="${PYTHON:-python}"

"$PY" data_generate.py --root .
rm -f data/processed/warehouse.duckdb

echo "1/4  Full build, batch 1 (baseline)"
"$PY" build.py --root . --mode full --promo-max-batch 1

echo "2/4  Augmented build, batch 2, purchase dates 2018-01-03 to 2018-01-05"
"$PY" build.py --root . --mode augmented --promo-max-batch 2 --date-start 2018-01-03 --date-end 2018-01-05

echo "3/4  Full rebuild with batch 2"
"$PY" build.py --root . --mode full --promo-max-batch 2

echo "4/4  Same full rebuild again (checksums must match run 3)"
"$PY" build.py --root . --mode full --promo-max-batch 2
