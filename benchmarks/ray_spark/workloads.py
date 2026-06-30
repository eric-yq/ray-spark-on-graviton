"""Hybrid Ray Data -> Spark workload.

Demonstrates the genuine hybrid path: Ray Data does the heavy per-row scan +
arithmetic (PyArrow, zero-copy), then hands the result to Spark **in memory**
via ``Dataset.to_spark`` (RayDP), where SparkSQL does the final grouped
aggregation. No intermediate storage round-trip.
"""
from __future__ import annotations

import datetime as _dt
from typing import Tuple

import pyarrow as pa
import pyarrow.compute as pc
import ray

from common.config import BenchConfig


def hybrid_etl(spark, cfg: BenchConfig, sf_label: str) -> Tuple[int, int]:
    cols = ["l_shipdate", "l_quantity", "l_extendedprice", "l_discount", "l_shipmode"]
    src = cfg.table_path(sf_label, "lineitem")
    n_in = ray.data.read_parquet(src, columns=["l_shipmode"]).count()  # metadata-fast

    lo, hi = _dt.date(1994, 1, 1), _dt.date(1995, 1, 1)

    def _feat(t: pa.Table) -> pa.Table:
        t = t.filter(pc.and_(pc.greater_equal(t["l_shipdate"], pa.scalar(lo)),
                             pc.less(t["l_shipdate"], pa.scalar(hi))))
        ext = pc.cast(t["l_extendedprice"], pa.float64())
        disc = pc.cast(t["l_discount"], pa.float64())
        revenue = pc.multiply(ext, pc.subtract(pa.scalar(1.0), disc))
        return pa.table({
            "l_shipmode": t["l_shipmode"],
            "revenue": revenue,
            "qty": pc.cast(t["l_quantity"], pa.float64()),
        })

    feat = ray.data.read_parquet(src, columns=cols).map_batches(_feat, batch_format="pyarrow")

    # In-memory handoff Ray Data -> Spark (uses RayDP under the hood).
    sdf = feat.to_spark(spark)
    sdf.createOrReplaceTempView("li_feat")
    rows = spark.sql(
        "SELECT l_shipmode, count(*) AS n, sum(revenue) AS revenue, avg(qty) AS avg_qty "
        "FROM li_feat GROUP BY l_shipmode ORDER BY l_shipmode"
    ).collect()
    return n_in, len(rows)
