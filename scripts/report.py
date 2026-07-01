"""Build the cross-architecture comparison report from benchmark result rows.

Reads per-run JSON results (the source of truth written by common/metrics.py),
optionally pulling them from S3 first, aggregates iterations to the BEST and
median wall time per (engine, workload, scale_factor, arch), then compares every
architecture against a baseline (default m7i). Scales to any number of archs
(m7i, m8i, m8g, m9g, ...).

Per (engine, workload, scale_factor, arch), relative to the baseline arch:
  speedup_vs_<base>   = base_best_wall / arch_best_wall   (>1 -> arch is faster)
  priceperf_vs_<base> = base_cost_per_run / arch_cost     (>1 -> more work per $)
The baseline's own rows have speedup=1.0 and priceperf=1.0.

Usage:
  python scripts/report.py                                  # local results/raw/*.json
  python scripts/report.py --from-s3 s3://bucket/results
  python scripts/report.py --baseline m7i --out results/comparison
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


def build_report(rows: list, out_base: str, baseline: str = "m7i") -> pd.DataFrame:
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

    archs = sorted(grp["arch"].unique())
    if baseline not in archs:
        print(f"[report] baseline '{baseline}' not in results {archs}; "
              "speedup/priceperf columns will be blank")

    # Long format: one row per (engine, workload, scale_factor, arch), each with
    # its own numbers plus the ratios vs the baseline arch. Scales to any # archs.
    out = []
    for (engine, wl, sf), sub in grp.groupby(["engine", "workload", "scale_factor"]):
        by_arch = {r["arch"]: r for _, r in sub.iterrows()}
        base = by_arch.get(baseline)
        for arch in sorted(by_arch):
            r = by_arch[arch]
            row = {
                "engine": engine, "workload": wl, "scale_factor": sf, "arch": arch,
                "worker_instance": r["worker_instance"],
                "best_s": round(r["best_wall"], 2),
                "median_s": round(r["median_wall"], 2),
                "cost_usd": round(r["best_cost"], 4),
                "runs": int(r["runs"]),
            }
            if base is not None and r["best_wall"] > 0:
                row[f"speedup_vs_{baseline}"] = round(base["best_wall"] / r["best_wall"], 3)
                row[f"priceperf_vs_{baseline}"] = (
                    round(base["best_cost"] / r["best_cost"], 3) if r["best_cost"] else None)
            out.append(row)

    report = (pd.DataFrame(out)
              .sort_values(["engine", "scale_factor", "workload", "arch"])
              .reset_index(drop=True))
    csv_path = f"{out_base}.csv"
    md_path = f"{out_base}.md"
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    report.to_csv(csv_path, index=False)
    _write_markdown(report, md_path, baseline)
    print(f"[report] wrote {csv_path} and {md_path}  (archs: {', '.join(archs)}, baseline: {baseline})")
    return report


def _write_markdown(report: pd.DataFrame, path: str, baseline: str) -> None:
    lines = [f"# Cross-architecture benchmark comparison (baseline: {baseline})", "",
             f"`speedup_vs_{baseline}` > 1 means the arch is faster than {baseline}; "
             f"`priceperf_vs_{baseline}` > 1 means it delivers more work per dollar. "
             f"The {baseline} rows are 1.0 by definition.", ""]
    try:
        lines.append(report.to_markdown(index=False))
    except Exception:  # tabulate not installed -> fall back to CSV-style block
        lines.append("```")
        lines.append(report.to_csv(index=False))
        lines.append("```")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="cross-architecture benchmark report")
    ap.add_argument("--results-dir", default="results",
                    help="local dir holding raw/*.json (default: results)")
    ap.add_argument("--from-s3", default="",
                    help="s3://bucket/prefix to pull raw/*.json before reporting")
    ap.add_argument("--baseline", default="m7i",
                    help="architecture to compare others against (default: m7i)")
    ap.add_argument("--out", default="results/comparison",
                    help="output path base (writes .csv and .md)")
    args = ap.parse_args()

    rows = _load_local(args.results_dir)
    if args.from_s3:
        tmp = tempfile.mkdtemp(prefix="bench-report-")
        _download_s3_raw(args.from_s3, tmp)
        rows += _load_local(tmp)

    report = build_report(rows, args.out, args.baseline)
    print()
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(report.to_string(index=False))


if __name__ == "__main__":
    main()
