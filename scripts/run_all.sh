#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Full benchmark sweep on the CURRENT cluster (one architecture).
# Run this on the head node, with the cluster up and TPC-H data in S3.
# The architecture is auto-detected by config.py from the head node's CPU
# (m7i head == x86_64, m8g head == aarch64), so the SAME command runs on both
# clusters; results land in results/results.csv and mirror to S3.
#
# Usage:
#   scripts/run_all.sh sf100
#   scripts/run_all.sh --repeat 5 sf10 sf100 sf600
#   scripts/run_all.sh --engines ray sf600          # ray-only
#   scripts/run_all.sh --engines rayspark sf100      # spark-only
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/.."   # repo root

ENGINES="ray,rayspark"
REPEAT="${BENCH_REPEAT:-3}"
PY="${BENCH_PYTHON:-python3.11}"    # AL2023: deps live in python3.11, not the default python3 (3.9)
SFS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --engines) ENGINES="$2"; shift 2 ;;
    --repeat)  REPEAT="$2";  shift 2 ;;
    -h|--help) sed -n '2,18p' "$0"; exit 0 ;;
    sf*|[0-9]*) SFS+=("$1"); shift ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ ${#SFS[@]} -eq 0 ]]; then
  echo "usage: $0 [--engines ray,rayspark] [--repeat N] <scale_factor...>" >&2
  exit 1
fi

has_engine() { [[ ",$ENGINES," == *",$1,"* ]]; }

echo "=================================================================="
echo " sweep: scale_factors=[${SFS[*]}] engines=$ENGINES repeat=$REPEAT"
echo "=================================================================="

for sf in "${SFS[@]}"; do
  if has_engine ray; then
    echo ">>> [ray-only] sf=$sf"
    "$PY" -m benchmarks.ray_only.run --scale-factor "$sf" --repeat "$REPEAT" || \
      echo "!!! ray-only sf=$sf had failures (continuing)"
  fi
  if has_engine rayspark; then
    echo ">>> [ray+spark] sf=$sf"
    "$PY" -m benchmarks.ray_spark.run --scale-factor "$sf" --repeat "$REPEAT" || \
      echo "!!! ray+spark sf=$sf had failures (continuing)"
  fi
done

echo "=================================================================="
echo " sweep complete -> results/results.csv (and S3 if BENCH_RESULTS_PREFIX set)"
echo " After running BOTH clusters, build the comparison with:"
echo "     python scripts/report.py --from-s3 \$BENCH_RESULTS_PREFIX"
echo "=================================================================="
