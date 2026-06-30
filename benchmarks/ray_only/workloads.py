"""Ray-only big-data workloads (Ray Data + Ray Core), built on the stable
Ray 2.44 API surface (``map_batches`` / ``groupby`` / ``aggregate`` / ``sort``).

Design notes
------------
* Ray 2.44 has **no** native ``Dataset.join`` (added only in later releases), so
  the join here is a **broadcast (map-side) join**: the small build side is
  placed in the object store once and probed locally per block with PyArrow.
  Large-shuffle cost is covered separately by ``sort`` and ``groupby_orderkey``.
* TPC-H money columns are ``decimal128(15,2)``; Ray's numeric aggregators expect
  float, so the relevant columns are cast to ``float64`` in a PyArrow
  ``map_batches`` projection before aggregation (fine for benchmark purposes).
* Each workload returns ``(input_rows, output_rows)`` for the metrics row.

All transforms use ``batch_format="pyarrow"`` to stay in Arrow and avoid pandas
conversion overhead — the same code path on both architectures.
"""
from __future__ import annotations

import datetime as _dt
from typing import Callable, Dict, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import ray
from ray.data.aggregate import Count, Mean, Sum

from common.config import BenchConfig


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _read(cfg: BenchConfig, sf_label: str, table: str, columns=None):
    """Lazily read a TPC-H table from S3 with optional column pushdown."""
    return ray.data.read_parquet(cfg.table_path(sf_label, table), columns=columns)


def _f64(col: pa.ChunkedArray) -> pa.ChunkedArray:
    return pc.cast(col, pa.float64())


# ---------------------------------------------------------------------------
# 1. read_sum — parquet read + decode throughput, light CPU
# ---------------------------------------------------------------------------
def read_sum(cfg: BenchConfig, sf: str) -> Tuple[int, int]:
    ds = _read(cfg, sf, "lineitem", columns=["l_extendedprice"])
    n_in = ds.count()

    def _partial(t: pa.Table) -> pa.Table:
        s = pc.sum(_f64(t["l_extendedprice"])).as_py() or 0.0
        return pa.table({"partial": pa.array([s], pa.float64())})

    partials = ds.map_batches(_partial, batch_format="pyarrow").to_pandas()
    _total = float(partials["partial"].sum())   # force execution + realize value
    return n_in, 1


# ---------------------------------------------------------------------------
# 2. filter_project — selective scan + arithmetic, embarrassingly parallel
# ---------------------------------------------------------------------------
def filter_project(cfg: BenchConfig, sf: str) -> Tuple[int, int]:
    cols = ["l_shipdate", "l_extendedprice", "l_discount"]
    ds = _read(cfg, sf, "lineitem", columns=cols)
    n_in = ds.count()
    lo, hi = _dt.date(1994, 1, 1), _dt.date(1995, 1, 1)

    def _fp(t: pa.Table) -> pa.Table:
        mask = pc.and_(pc.greater_equal(t["l_shipdate"], pa.scalar(lo)),
                       pc.less(t["l_shipdate"], pa.scalar(hi)))
        t = t.filter(mask)
        dp = pc.multiply(_f64(t["l_extendedprice"]),
                         pc.subtract(pa.scalar(1.0), _f64(t["l_discount"])))
        return pa.table({"disc_price": dp})

    n_out = ds.map_batches(_fp, batch_format="pyarrow").count()
    return n_in, n_out


# ---------------------------------------------------------------------------
# 3. q1 — TPC-H Q1: filter + low-cardinality groupby with many aggregates
# ---------------------------------------------------------------------------
def q1(cfg: BenchConfig, sf: str) -> Tuple[int, int]:
    cols = ["l_shipdate", "l_returnflag", "l_linestatus",
            "l_quantity", "l_extendedprice", "l_discount", "l_tax"]
    ds = _read(cfg, sf, "lineitem", columns=cols)
    n_in = ds.count()
    cutoff = _dt.date(1998, 9, 2)

    def _prep(t: pa.Table) -> pa.Table:
        t = t.filter(pc.less_equal(t["l_shipdate"], pa.scalar(cutoff)))
        ext, disc, tax = _f64(t["l_extendedprice"]), _f64(t["l_discount"]), _f64(t["l_tax"])
        disc_price = pc.multiply(ext, pc.subtract(pa.scalar(1.0), disc))
        charge = pc.multiply(disc_price, pc.add(pa.scalar(1.0), tax))
        return pa.table({
            "l_returnflag": t["l_returnflag"],
            "l_linestatus": t["l_linestatus"],
            "qty": _f64(t["l_quantity"]),
            "ext": ext, "disc": disc,
            "disc_price": disc_price, "charge": charge,
        })

    res = (ds.map_batches(_prep, batch_format="pyarrow")
             .groupby(["l_returnflag", "l_linestatus"])
             .aggregate(
                 Sum("qty", alias_name="sum_qty"),
                 Sum("ext", alias_name="sum_base_price"),
                 Sum("disc_price", alias_name="sum_disc_price"),
                 Sum("charge", alias_name="sum_charge"),
                 Mean("qty", alias_name="avg_qty"),
                 Mean("ext", alias_name="avg_price"),
                 Mean("disc", alias_name="avg_disc"),
                 Count(alias_name="count_order"),
             ))
    groups = res.take_all()
    return n_in, len(groups)


# ---------------------------------------------------------------------------
# 4. sort — global sort of lineitem (full shuffle, heaviest spill)
# ---------------------------------------------------------------------------
def sort(cfg: BenchConfig, sf: str) -> Tuple[int, int]:
    ds = _read(cfg, sf, "lineitem", columns=["l_orderkey", "l_shipdate"])
    n_in = ds.count()
    sorted_ds = ds.sort(["l_shipdate", "l_orderkey"]).materialize()
    return n_in, sorted_ds.count()


# ---------------------------------------------------------------------------
# 5. groupby_orderkey — high-cardinality shuffle (join-scale repartition cost)
# ---------------------------------------------------------------------------
def groupby_orderkey(cfg: BenchConfig, sf: str) -> Tuple[int, int]:
    ds = _read(cfg, sf, "lineitem", columns=["l_orderkey", "l_extendedprice"])
    n_in = ds.count()

    def _cast(t: pa.Table) -> pa.Table:
        return pa.table({"l_orderkey": t["l_orderkey"], "ext": _f64(t["l_extendedprice"])})

    res = (ds.map_batches(_cast, batch_format="pyarrow")
             .groupby("l_orderkey")
             .sum("ext")
             .materialize())
    return n_in, res.count()


# ---------------------------------------------------------------------------
# 6. broadcast_join — map-side join lineitem ⨝ supplier, then per-nation sum
# ---------------------------------------------------------------------------
def broadcast_join(cfg: BenchConfig, sf: str) -> Tuple[int, int]:
    sup_pd = _read(cfg, sf, "supplier", columns=["s_suppkey", "s_nationkey"]).to_pandas()
    sup_ref = ray.put(pa.Table.from_pandas(sup_pd, preserve_index=False))

    li = _read(cfg, sf, "lineitem", columns=["l_suppkey", "l_extendedprice"])
    n_in = li.count()

    def _join(t: pa.Table, sup_ref) -> pa.Table:
        sup = ray.get(sup_ref)
        j = t.join(sup, keys="l_suppkey", right_keys="s_suppkey", join_type="inner")
        return pa.table({"s_nationkey": j["s_nationkey"], "ext": _f64(j["l_extendedprice"])})

    res = (li.map_batches(_join, batch_format="pyarrow", fn_kwargs={"sup_ref": sup_ref})
             .groupby("s_nationkey")
             .sum("ext")
             .materialize())
    return n_in, res.count()


WORKLOADS: Dict[str, Callable[[BenchConfig, str], Tuple[int, int]]] = {
    "read_sum": read_sum,
    "filter_project": filter_project,
    "q1": q1,
    "sort": sort,
    "groupby_orderkey": groupby_orderkey,
    "broadcast_join": broadcast_join,
}
