"""Run Ray+Spark (RayDP) workloads against a TPC-H scale factor.

On the control node, with the cluster up and data in S3::

    python -m benchmarks.ray_spark.run --scale-factor sf100
    python -m benchmarks.ray_spark.run --sf sf600 --workload q5,q9,hybrid_etl

Spark runs as executors on the Ray cluster (one Spark session reused across all
selected workloads). Each workload is wrapped in a BenchmarkRun.
"""
from __future__ import annotations

import argparse
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)

import ray  # noqa: E402

from benchmarks.ray_spark.spark_session import (  # noqa: E402
    init_spark, register_tpch_views, stop_spark)
from benchmarks.ray_spark.tpch_queries import QUERIES  # noqa: E402
from benchmarks.ray_spark.workloads import hybrid_etl  # noqa: E402
from common.config import SCALE_FACTORS, TPCH_TABLES, get_config  # noqa: E402
from common.runner import BenchmarkRun  # noqa: E402

ALL_WORKLOADS = list(QUERIES) + ["hybrid_etl"]


def normalize_sf(arg: str) -> str:
    if arg in SCALE_FACTORS or arg.startswith("sf"):
        return arg
    return f"sf{int(arg)}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Ray+Spark (RayDP) TPC-H benchmark runner")
    ap.add_argument("--scale-factor", "--sf", dest="sf", required=True)
    ap.add_argument("--workload", default="all",
                    help="'all' or comma list: " + ",".join(ALL_WORKLOADS))
    ap.add_argument("--no-monitor", action="store_true")
    ap.add_argument("--repeat", type=int, default=1,
                    help="iterations per workload (report takes best/median)")
    args = ap.parse_args()

    sf = normalize_sf(args.sf)
    names = ALL_WORKLOADS if args.workload == "all" else [w.strip() for w in args.workload.split(",")]
    unknown = [n for n in names if n not in ALL_WORKLOADS]
    if unknown:
        ap.error(f"unknown workload(s): {unknown}. choose from {ALL_WORKLOADS}")

    cfg = get_config()
    # working_dir ships the repo to all workers (so the hybrid workload module is
    # importable); env_vars pins the interpreter for RayDP's Spark executors.
    ray.init(address="auto", ignore_reinit_error=True,
             runtime_env={
                 "working_dir": REPO_ROOT,
                 "excludes": [".git", "results", "**/__pycache__", "**/*.parquet"],
                 "env_vars": {
                     "PYSPARK_PYTHON": sys.executable,
                     "PYSPARK_DRIVER_PYTHON": sys.executable,
                 },
             })

    spark = init_spark(cfg)
    failures = 0
    try:
        register_tpch_views(spark, cfg, sf, TPCH_TABLES)
        # consistent throughput denominator across all SparkSQL workloads
        lineitem_rows = spark.sql("SELECT count(*) FROM lineitem").collect()[0][0]
        print(f"[ray_spark] arch={cfg.arch} sf={sf} lineitem_rows={lineitem_rows:,} workloads={names}")

        for name in names:
            for it in range(args.repeat):
                try:
                    with BenchmarkRun(engine="rayspark", workload=name, scale_factor=sf,
                                      config=cfg, monitor=not args.no_monitor) as run:
                        if name == "hybrid_etl":
                            n_in, n_out = hybrid_etl(spark, cfg, sf)
                        else:
                            rows = spark.sql(QUERIES[name]).collect()
                            n_in, n_out = lineitem_rows, len(rows)
                        run.set_metrics(input_rows=n_in, output_rows=n_out,
                                        notes=f"iter={it}")
                except Exception as exc:  # noqa: BLE001
                    failures += 1
                    print(f"[ray_spark] workload '{name}' iter={it} FAILED: {exc}")
    finally:
        stop_spark()

    print(f"[ray_spark] complete ({args.repeat}x each): {len(names)} workloads, {failures} failed runs")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
