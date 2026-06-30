"""Benchmark result model + local/S3 persistence.

A flat :class:`BenchmarkResult` is written as one row to ``results/results.csv``
(append, stable header) and as a per-run JSON under ``results/raw/``. When an S3
results prefix is configured both are uploaded so they survive ``ray down``.
"""
from __future__ import annotations

import csv
import dataclasses
import json
import os
import socket
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional

# Stable column order for the aggregate CSV.
CSV_FIELDS = [
    "run_id", "timestamp", "arch", "engine", "workload", "scale_factor",
    "status", "wall_seconds", "input_rows", "output_rows", "throughput_rows_per_s",
    "head_instance", "worker_instance", "num_workers",
    "cluster_hourly_cost_usd", "run_cost_usd",
    "cpu_pct_avg", "cpu_pct_max", "mem_used_gb_total_max",
    "disk_read_mb_total", "disk_write_mb_total",
    "net_recv_mb_total", "net_sent_mb_total",
    "driver_host", "error", "notes",
]


@dataclass
class BenchmarkResult:
    arch: str
    engine: str                 # "ray" | "rayspark"
    workload: str
    scale_factor: str
    status: str = "ok"          # "ok" | "error"
    wall_seconds: float = 0.0
    input_rows: int = 0
    output_rows: int = 0
    throughput_rows_per_s: float = 0.0
    head_instance: str = ""
    worker_instance: str = ""
    num_workers: int = 0
    cluster_hourly_cost_usd: float = 0.0
    run_cost_usd: float = 0.0
    cpu_pct_avg: float = 0.0
    cpu_pct_max: float = 0.0
    mem_used_gb_total_max: float = 0.0
    disk_read_mb_total: float = 0.0
    disk_write_mb_total: float = 0.0
    net_recv_mb_total: float = 0.0
    net_sent_mb_total: float = 0.0
    driver_host: str = field(default_factory=socket.gethostname)
    error: str = ""
    notes: str = ""
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    resources: Dict = field(default_factory=dict)   # full per-node detail (JSON only)

    def to_csv_row(self) -> Dict:
        d = asdict(self)
        return {k: d.get(k, "") for k in CSV_FIELDS}


def _append_csv(path: str, result: BenchmarkResult) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    new_file = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow(result.to_csv_row())


def _write_json(path: str, result: BenchmarkResult) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(dataclasses.asdict(result), f, indent=2, default=str)


def _upload_s3(local_path: str, s3_uri: str) -> Optional[str]:
    """Best-effort upload. Returns the s3 uri on success, else None."""
    try:
        import boto3
    except Exception:
        return None
    if not s3_uri.startswith("s3://"):
        return None
    try:
        bucket, _, key = s3_uri[len("s3://"):].partition("/")
        boto3.client("s3").upload_file(local_path, bucket, key)
        return s3_uri
    except Exception as exc:  # noqa: BLE001 — persistence must never fail a run
        print(f"[metrics] S3 upload skipped ({exc})")
        return None


def persist(result: BenchmarkResult, results_dir: str = "results",
            s3_results_prefix: str = "") -> None:
    """Append to CSV + write per-run JSON locally, then mirror both to S3."""
    csv_path = os.path.join(results_dir, "results.csv")
    json_path = os.path.join(results_dir, "raw", f"{result.run_id}.json")
    _append_csv(csv_path, result)
    _write_json(json_path, result)
    print(f"[metrics] wrote {csv_path} and {json_path}")

    if s3_results_prefix.startswith("s3://"):
        base = s3_results_prefix.rstrip("/")
        _upload_s3(json_path, f"{base}/raw/{result.run_id}.json")
        # keep a per-run copy of the CSV too (atomic, no concurrent-append clobber)
        _upload_s3(csv_path, f"{base}/results-{result.run_id}.csv")
