"""Run one or more Ray-only workloads against a given TPC-H scale factor.

On the control node, with the cluster up and data in S3::

    # all workloads at SF100
    python -m benchmarks.ray_only.run --scale-factor sf100

    # a subset at SF600
    python -m benchmarks.ray_only.run --sf sf600 --workload q1,sort,broadcast_join

Each workload is wrapped in a BenchmarkRun (times it, samples cluster resources,
computes cost/throughput) and the result row is appended to results/results.csv
and mirrored to S3.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import ray  # noqa: E402

from benchmarks.ray_only.workloads import WORKLOADS  # noqa: E402
from common.config import SCALE_FACTORS, get_config  # noqa: E402
from common.runner import BenchmarkRun  # noqa: E402


def normalize_sf(arg: str) -> str:
    if arg in SCALE_FACTORS:
        return arg
    if arg.startswith("sf"):
        return arg
    return f"sf{int(arg)}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Ray-only TPC-H-style benchmark runner")
    ap.add_argument("--scale-factor", "--sf", dest="sf", required=True,
                    help="sf10 | sf100 | sf600 | <integer>")
    ap.add_argument("--workload", default="all",
                    help="'all' or comma list: " + ",".join(WORKLOADS))
    ap.add_argument("--no-monitor", action="store_true",
                    help="disable per-node resource sampling")
    ap.add_argument("--repeat", type=int, default=1,
                    help="iterations per workload (report takes best/median)")
    args = ap.parse_args()

    sf = normalize_sf(args.sf)
    if args.workload == "all":
        names = list(WORKLOADS)
    else:
        names = [w.strip() for w in args.workload.split(",")]
    unknown = [n for n in names if n not in WORKLOADS]
    if unknown:
        ap.error(f"unknown workload(s): {unknown}. choose from {list(WORKLOADS)}")

    cfg = get_config()
    ray.init(address="auto", ignore_reinit_error=True)
    print(f"[ray_only] arch={cfg.arch} sf={sf} workloads={names}")

    failures = 0
    for name in names:
        fn = WORKLOADS[name]
        for it in range(args.repeat):
            try:
                with BenchmarkRun(engine="ray", workload=name, scale_factor=sf,
                                  config=cfg, monitor=not args.no_monitor) as run:
                    n_in, n_out = fn(cfg, sf)
                    run.set_metrics(input_rows=n_in, output_rows=n_out,
                                    notes=f"iter={it}")
            except Exception as exc:  # noqa: BLE001 — record + continue to next workload
                failures += 1
                print(f"[ray_only] workload '{name}' iter={it} FAILED: {exc}")

    print(f"[ray_only] complete ({args.repeat}x each): {len(names)} workloads, {failures} failed runs")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
