"""RayDP Spark-on-Ray session helpers.

``init_spark`` launches Spark executors as Ray actors (so Spark runs *on* the
Ray cluster — the hybrid topology). Executor sizing defaults to one executor per
worker node and is fully env-overridable so the same code runs on the real
cluster or a tiny local test.

S3 access uses the S3A connector with the EC2 instance role; the required jars
are pre-staged into pyspark/jars by node_setup.sh.
"""
from __future__ import annotations

import os
import shutil
import sys

import raydp

from common.config import SPARK_LOCAL_DIR, TPCH_TABLES, BenchConfig


def _java_home() -> str:
    jh = os.environ.get("JAVA_HOME")
    if jh:
        return jh
    exe = shutil.which("javac") or shutil.which("java")
    return os.path.dirname(os.path.dirname(os.path.realpath(exe))) if exe else ""


def to_s3a(path: str) -> str:
    """Spark reads S3 via the s3a:// scheme; pass-through for local paths."""
    return "s3a://" + path[len("s3://"):] if path.startswith("s3://") else path


def init_spark(cfg: BenchConfig, app_name: str = "ray-spark-bench"):
    jh = _java_home()
    if jh:
        os.environ["JAVA_HOME"] = jh

    num_executors = int(os.environ.get("BENCH_SPARK_EXECUTORS", cfg.num_workers))
    cores = int(os.environ.get("BENCH_SPARK_EXECUTOR_CORES", "15"))
    memory = os.environ.get("BENCH_SPARK_EXECUTOR_MEMORY", "40GB")
    shuffle_parts = os.environ.get("BENCH_SPARK_SHUFFLE_PARTITIONS",
                                   str(max(num_executors * cores * 2, 8)))

    configs = {
        "spark.local.dir": SPARK_LOCAL_DIR,
        "spark.sql.shuffle.partitions": shuffle_parts,
        "spark.sql.adaptive.enabled": "true",
        "spark.driver.memory": os.environ.get("BENCH_SPARK_DRIVER_MEMORY", "4g"),
        # Pin the Python interpreter for Spark's Arrow/UDF workers (e.g. the
        # Ray Data -> Spark hybrid path) to the one that actually has pyspark +
        # pyarrow. Without this Spark forks `python3` off PATH, which may be the
        # wrong interpreter. The path is identical across nodes on the cluster.
        "spark.pyspark.python": sys.executable,
        "spark.executorEnv.PYSPARK_PYTHON": sys.executable,
        # --- S3A: read parquet straight from S3 with the instance role ---
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider":
            "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        "spark.hadoop.fs.s3a.endpoint.region": cfg.region,
    }
    print(f"[spark] init: {num_executors} executors x {cores} cores x {memory} "
          f"(shuffle_partitions={shuffle_parts}, JAVA_HOME={jh})")
    return raydp.init_spark(app_name=app_name, num_executors=num_executors,
                            executor_cores=cores, executor_memory=memory, configs=configs)


def register_tpch_views(spark, cfg: BenchConfig, sf_label: str, tables=TPCH_TABLES) -> None:
    """Register each TPC-H parquet directory as a temp view named after the table."""
    for t in tables:
        spark.read.parquet(to_s3a(cfg.table_path(sf_label, t))).createOrReplaceTempView(t)
    print(f"[spark] registered views: {', '.join(tables)}")


def stop_spark() -> None:
    try:
        raydp.stop_spark()
    except Exception as exc:  # noqa: BLE001
        print(f"[spark] stop_spark warning: {exc}")
