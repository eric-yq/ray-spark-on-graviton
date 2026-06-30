"""Distributed TPC-H parquet generator (runs ON the Ray cluster).

Why on the cluster: at SF600 the dataset is ~600 GB — far larger than the head
node's disk. By fanning generation out as Ray tasks across the 3 workers
(each with a 600 GB volume), every task generates one ~250 MB parquet *part*,
uploads it straight to S3, then deletes the local file. Peak local disk stays
tiny and all 48 cores are used.

The data is deterministic, so generate ONCE (on whichever cluster is up) and
both the m7i and m8g runs read the same S3 copy.

Run (from repo root, on the control node, with the cluster up)::

    python -m data.generators.gen_tpch --scale-factor sf100
    python -m data.generators.gen_tpch --scale-factor sf600 --tables lineitem,orders

Output layout in S3:  <data_prefix>/<sf>/<table>/<table>.<part>.parquet
"""
from __future__ import annotations

import argparse
import glob
import os
import subprocess
import sys
import time

# Allow `python data/generators/gen_tpch.py` as well as `-m`.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import ray  # noqa: E402

from common.config import SCALE_FACTORS, TPCH_TABLES, get_config  # noqa: E402

# Relative number of parts per table (multiplied by scale factor). Tuned so each
# parquet file lands around ~250 MB, giving good read parallelism across 48 cores.
PART_WEIGHTS = {
    "lineitem": 1.0,
    "orders": 0.25,
    "partsupp": 0.2,
    "customer": 0.05,
    "part": 0.04,
    "supplier": 0.01,
    "nation": 0.0,   # tiny — always 1 part
    "region": 0.0,   # tiny — always 1 part
}


def parts_for(table: str, sf: int, scale: float = 1.0) -> int:
    return max(1, round(sf * PART_WEIGHTS.get(table, 0.05) * scale))


def _split_s3(uri: str):
    assert uri.startswith("s3://"), uri
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key


@ray.remote(num_cpus=1)
def gen_part(table: str, sf: int, parts: int, part: int,
             s3_table_dir: str, local_root: str, region: str) -> dict:
    """Generate one partition, upload to S3, delete local. Returns stats."""
    import boto3

    os.makedirs(local_root, exist_ok=True)
    cmd = [
        "tpchgen-cli", "parquet",
        "-s", str(sf), "-T", table,
        "--parts", str(parts), "--part", str(part),
        "-o", local_root, "-n", "1", "--quiet", "--no-progress",
    ]
    subprocess.run(cmd, check=True)

    produced = glob.glob(os.path.join(local_root, table, f"{table}.{part}.*"))
    if not produced:
        raise FileNotFoundError(f"tpchgen produced no file for {table} part {part}")
    local_file = produced[0]
    size = os.path.getsize(local_file)

    bucket, key_prefix = _split_s3(s3_table_dir.rstrip("/") + "/")
    key = key_prefix + os.path.basename(local_file)
    boto3.client("s3", region_name=region).upload_file(local_file, bucket, key)

    os.remove(local_file)
    return {"table": table, "part": part, "bytes": size, "s3": f"s3://{bucket}/{key}"}


def resolve_sf(arg: str):
    """Accept 'sf100' (preset), any 'sfN', or a raw integer; return (numeric_sf, label)."""
    if arg in SCALE_FACTORS:
        return SCALE_FACTORS[arg], arg
    digits = arg[2:] if arg.lower().startswith("sf") else arg
    sf = int(digits)
    return sf, f"sf{sf}"


def main() -> None:
    ap = argparse.ArgumentParser(description="Distributed TPC-H parquet generator")
    ap.add_argument("--scale-factor", required=True,
                    help="sf10 | sf100 | sf600 | <integer>")
    ap.add_argument("--tables", default="all",
                    help="comma list (default: all 8 TPC-H tables)")
    ap.add_argument("--parts-scale", type=float, default=1.0,
                    help="multiplier on parts-per-table (more = smaller files)")
    ap.add_argument("--local-root", default=None,
                    help="node-local scratch dir for generation (default: <scratch>/tmp/gen)")
    args = ap.parse_args()

    cfg = get_config()
    sf, label = resolve_sf(args.scale_factor)
    tables = TPCH_TABLES if args.tables == "all" else tuple(t.strip() for t in args.tables.split(","))
    local_root = args.local_root or os.path.join(
        os.environ.get("BENCH_SCRATCH", "/opt/bench/scratch"), "tmp", "gen")

    ray.init(address="auto")

    plan = {t: parts_for(t, sf, args.parts_scale) for t in tables}
    total_parts = sum(plan.values())
    print(f"[gen] {label} (SF={sf}) -> {cfg.data_prefix}/{label}")
    print(f"[gen] parts per table: {plan}  (total {total_parts} tasks)")

    refs = []
    for table in tables:
        parts = plan[table]
        s3_dir = cfg.table_path(label, table)
        for part in range(1, parts + 1):
            refs.append(gen_part.remote(table, sf, parts, part, s3_dir, local_root, cfg.region))

    done, total_bytes, t0 = 0, 0, time.time()
    pending = refs
    while pending:
        ready, pending = ray.wait(pending, num_returns=min(32, len(pending)), timeout=30)
        for r in ray.get(ready):
            done += 1
            total_bytes += r["bytes"]
        elapsed = time.time() - t0
        print(f"[gen] {done}/{total_parts} parts | "
              f"{total_bytes/1e9:.1f} GB | {elapsed:.0f}s", flush=True)

    print(f"[gen] DONE {label}: {done} files, {total_bytes/1e9:.1f} GB to "
          f"{cfg.data_prefix}/{label} in {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
