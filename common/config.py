"""Central benchmark configuration.

Everything that must stay constant across the m7i and m8g runs lives here so the
two clusters differ ONLY in CPU architecture. Values are overridable via env
vars (see ``BenchConfig.from_env``) so the same code runs on either cluster
without edits.
"""
from __future__ import annotations

import os
import platform
from dataclasses import dataclass, field
from typing import Dict

# ---------------------------------------------------------------------------
# Fixed topology  (1 head + 3 workers, identical sizing per arch)
# ---------------------------------------------------------------------------
NUM_WORKERS = 3

ARCH_INSTANCES: Dict[str, Dict[str, str]] = {
    "m7i": {"head": "m7i.2xlarge", "worker": "m7i.4xlarge"},   # Intel (Sapphire Rapids)
    "m8i": {"head": "m8i.2xlarge", "worker": "m8i.4xlarge"},   # Intel (newer gen)
    "m8g": {"head": "m8g.2xlarge", "worker": "m8g.4xlarge"},   # Graviton4
    "m9g": {"head": "m9g.2xlarge", "worker": "m9g.4xlarge"},   # Graviton (newer gen)
}

# CPU architecture per family — used to pick the right AMI (x86_64 vs arm64).
ARCH_FAMILY: Dict[str, str] = {
    "m7i": "x86_64", "m8i": "x86_64",
    "m8g": "arm64",  "m9g": "arm64",
}

# ---------------------------------------------------------------------------
# Scale factors.  ~1 GB per SF unit of raw TPC-H => sf600 ~= 600 GB.
#   sf10  : smoke test (fits in memory)
#   sf100 : mostly in-memory -> isolates CPU
#   sf600 : spill-heavy -> realistic mixed CPU+IO
# ---------------------------------------------------------------------------
SCALE_FACTORS: Dict[str, int] = {"sf10": 10, "sf100": 100, "sf600": 600}

TPCH_TABLES = (
    "lineitem", "orders", "customer", "part", "partsupp",
    "supplier", "nation", "region",
)

# ---------------------------------------------------------------------------
# Scratch paths — on the single large gp3 root volume (see node_setup.sh).
# ---------------------------------------------------------------------------
SCRATCH_DIR = os.environ.get("BENCH_SCRATCH", "/opt/bench/scratch")
RAY_TEMP_DIR = f"{SCRATCH_DIR}/ray"
SPARK_LOCAL_DIR = f"{SCRATCH_DIR}/spark"

# ---------------------------------------------------------------------------
# On-demand Linux pricing, us-east-1, USD/hr.
# User-provided figures (2026-07). VERIFY for your region and override via env
# BENCH_PRICE_<INSTANCE> (e.g. BENCH_PRICE_M9G_4XLARGE=0.78) or the Pricing API.
# ---------------------------------------------------------------------------
DEFAULT_PRICES_USD_HR: Dict[str, float] = {
    "m7i.2xlarge": 0.4032,   "m7i.4xlarge": 0.8064,
    "m8i.2xlarge": 0.42336,  "m8i.4xlarge": 0.84672,
    "m8g.2xlarge": 0.35904,  "m8g.4xlarge": 0.71808,
    "m9g.2xlarge": 0.39136,  "m9g.4xlarge": 0.78272,
}


def _imds_instance_family(timeout: float = 0.3) -> str:
    """Return the EC2 instance family (e.g. 'm8i') via IMDSv2, or '' if unavailable."""
    import urllib.request
    try:
        tok_req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token", method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
        token = urllib.request.urlopen(tok_req, timeout=timeout).read().decode()
        it_req = urllib.request.Request(
            "http://169.254.169.254/latest/meta-data/instance-type",
            headers={"X-aws-ec2-metadata-token": token})
        instance_type = urllib.request.urlopen(it_req, timeout=timeout).read().decode()
        return instance_type.split(".", 1)[0]      # 'm8i.2xlarge' -> 'm8i'
    except Exception:
        return ""


def _detect_arch() -> str:
    """Auto-detect the arch tag (overridden by BENCH_ARCH).

    m7i/m8i are both x86_64 and m8g/m9g are both arm64, so the CPU arch alone is
    ambiguous. Prefer the exact instance family from EC2 metadata; fall back to a
    CPU-arch default only if metadata is unavailable (e.g. running off-EC2).
    """
    fam = _imds_instance_family()
    if fam in ARCH_INSTANCES:
        return fam
    return "m8g" if platform.machine().lower() in ("aarch64", "arm64") else "m7i"


@dataclass
class BenchConfig:
    arch: str                       # "m7i" | "m8i" | "m8g" | "m9g"
    region: str
    s3_bucket: str
    data_prefix: str                # s3://<bucket>/<path> root holding <sf>/<table>/
    results_prefix: str             # s3://<bucket>/<path> for uploaded results
    results_dir: str                # local dir for CSV/JSON
    num_workers: int = NUM_WORKERS
    prices: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_PRICES_USD_HR))

    # -- instances ----------------------------------------------------------
    @property
    def head_instance(self) -> str:
        return ARCH_INSTANCES[self.arch]["head"]

    @property
    def worker_instance(self) -> str:
        return ARCH_INSTANCES[self.arch]["worker"]

    # -- pricing ------------------------------------------------------------
    def price(self, instance: str) -> float:
        env = os.environ.get(f"BENCH_PRICE_{instance.replace('.', '_').upper()}")
        if env:
            return float(env)
        return self.prices.get(instance, 0.0)

    def cluster_hourly_cost(self) -> float:
        """Whole-cluster on-demand $/hr (1 head + N workers)."""
        return self.price(self.head_instance) + self.num_workers * self.price(self.worker_instance)

    # -- data paths ---------------------------------------------------------
    def table_path(self, scale_factor: str, table: str) -> str:
        return f"{self.data_prefix.rstrip('/')}/{scale_factor}/{table}/"

    # -- construction -------------------------------------------------------
    @classmethod
    def from_env(cls) -> "BenchConfig":
        bucket = os.environ.get("BENCH_S3_BUCKET", "")
        default_data = f"s3://{bucket}/tpch" if bucket else "s3://CHANGEME/tpch"
        default_results = f"s3://{bucket}/results" if bucket else "s3://CHANGEME/results"
        return cls(
            arch=os.environ.get("BENCH_ARCH", _detect_arch()),
            region=os.environ.get("BENCH_REGION", "us-east-1"),
            s3_bucket=bucket,
            data_prefix=os.environ.get("BENCH_DATA_PREFIX", default_data),
            results_prefix=os.environ.get("BENCH_RESULTS_PREFIX", default_results),
            results_dir=os.environ.get("BENCH_RESULTS_DIR", "results"),
        )


def get_config() -> BenchConfig:
    return BenchConfig.from_env()
