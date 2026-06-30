"""Build the m7i-vs-m8g comparison report from benchmark result rows.

Reads per-run JSON results (the source of truth written by common/metrics.py),
optionally pulling them from S3 first, aggregates iterations to the BEST and
median wall time per (engine, workload, scale_factor, arch), then pairs the two
architectures to produce speedup, cost, and price-performance.

Key metrics (m8g = Graviton4, m7i = Intel):
  speedup        = m7i_best_wall / m8g_best_wall      (>1  -> m8g is faster)
  cost_savings%  = (m7i_cost - m8g_cost) / m7i_cost   (>0  -> m8g is cheaper)
  price_perf     = m7i_cost_per_run / m8g_cost_per_run (>1 -> m8g better $/work)

Usage:
  python scripts/report.py                              # local results/raw/*.json
  python scripts/report.py --from-s3 s3://bucket/results
  python scripts/report.py --results-dir results --out results/comparison
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import tempfile

import pandas as pd


def _load_local(results_dir: str) -> list:
    rows = []
    for f in glob.glob(os.path.join(results_dir, "raw", "*.json")):
        try:
            rows.append(json.load(open(f)))
        except Exception as exc:  # noqa: BLE001
            print(f"[report] skip {f}: {exc}")
    return rows


def _download_s3_raw(s3_prefix: str, dest: str) -> int:
    import boto3

    bucket, _, key_prefix = s3_prefix[len("s3://"):].partition("/")
    raw_prefix = f"{key_prefix.rstrip('/')}/raw/"
    s3 = boto3.client("s3")
    os.makedirs(os.path.join(dest, "raw"), exist_ok=True)
    n = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=raw_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                s3.download_file(bucket, key, os.path.join(dest, "raw", os.path.basename(key)))
                n += 1
    print(f"[report] downloaded {n} result JSONs from {s3_prefix}")
    return n


def build_report(rows: list, out_base: str) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("[report] no result rows found")
    df = df[df["status"] == "ok"].copy()
    if df.empty:
        raise SystemExit("[report] no successful runs to report")

    # best (min) + median wall across iterations, per group+arch
    grp = (df.groupby(["engine", "workload", "scale_factor", "arch"])
             .agg(best_wall=("wall_seconds", "min"),
                  median_wall=("wall_seconds", "median"),
                  hourly=("cluster_hourly_cost_usd", "first"),
                  worker_instance=("worker_instance", "first"),
                  runs=("wall_seconds", "count"))
             .reset_index())
    grp["best_cost"] = grp["hourly"] * grp["best_wall"] / 3600.0

    records = []
    for (engine, wl, sf), sub in grp.groupby(["engine", "workload", "scale_factor"]):
        by_arch = {r["arch"]: r for _, r in sub.iterrows()}
        rec = {"engine": engine, "workload": wl, "scale_factor": sf}
        for arch in ("m7i", "m8g"):
            if arch in by_arch:
                rec[f"{arch}_best_s"] = round(by_arch[arch]["best_wall"], 2)
                rec[f"{arch}_median_s"] = round(by_arch[arch]["median_wall"], 2)
                rec[f"{arch}_cost_usd"] = round(by_arch[arch]["best_cost"], 4)
        if "m7i" in by_arch and "m8g" in by_arch:
            m7, m8 = by_arch["m7i"], by_arch["m8g"]
            c7 = m7["best_cost"]
            c8 = m8["best_cost"]
            rec["speedup_m8g"] = round(m7["best_wall"] / m8["best_wall"], 3)
            rec["cost_savings_pct"] = round((c7 - c8) / c7 * 100, 1) if c7 else None
            rec["price_perf_m8g"] = round(c7 / c8, 3) if c8 else None
        records.append(rec)

    report = pd.DataFrame(records).sort_values(["engine", "scale_factor", "workload"])
    csv_path = f"{out_base}.csv"
    md_path = f"{out_base}.md"
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    report.to_csv(csv_path, index=False)
    _write_markdown(report, md_path)
    print(f"[report] wrote {csv_path} and {md_path}")
    return report


def _write_markdown(report: pd.DataFrame, path: str) -> None:
    lines = ["# m7i vs m8g benchmark comparison", "",
             "speedup_m8g > 1 means Graviton (m8g) is faster; "
             "price_perf_m8g > 1 means m8g delivers more work per dollar.", ""]
    try:
        lines.append(report.to_markdown(index=False))
    except Exception:  # tabulate not installed -> fall back to CSV-style block
        lines.append("```")
        lines.append(report.to_csv(index=False))
        lines.append("```")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="m7i vs m8g benchmark report")
    ap.add_argument("--results-dir", default="results",
                    help="local dir holding raw/*.json (default: results)")
    ap.add_argument("--from-s3", default="",
                    help="s3://bucket/prefix to pull raw/*.json before reporting")
    ap.add_argument("--out", default="results/comparison",
                    help="output path base (writes .csv and .md)")
    args = ap.parse_args()

    rows = _load_local(args.results_dir)
    if args.from_s3:
        tmp = tempfile.mkdtemp(prefix="bench-report-")
        _download_s3_raw(args.from_s3, tmp)
        rows += _load_local(tmp)

    report = build_report(rows, args.out)
    print()
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(report.to_string(index=False))


if __name__ == "__main__":
    main()
