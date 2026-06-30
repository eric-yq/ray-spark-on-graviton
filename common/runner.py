"""``BenchmarkRun`` — one context manager that times a workload, samples cluster
resources, computes cost / throughput, and persists the result row.

Usage inside any workload::

    from common.runner import BenchmarkRun

    with BenchmarkRun(engine="ray", workload="q1_groupby", scale_factor="sf100") as run:
        rows_in, rows_out = do_the_work()
        run.set_metrics(input_rows=rows_in, output_rows=rows_out)
"""
from __future__ import annotations

import time
from typing import Optional

from common.config import BenchConfig, get_config
from common.metrics import BenchmarkResult, persist
from common.resource_monitor import ClusterMonitor


class BenchmarkRun:
    def __init__(self, engine: str, workload: str, scale_factor: str,
                 config: Optional[BenchConfig] = None,
                 monitor: bool = True, monitor_interval: float = 2.0):
        self.cfg = config or get_config()
        self.engine = engine
        self.workload = workload
        self.scale_factor = scale_factor
        self.enable_monitor = monitor
        self.monitor_interval = monitor_interval
        self._monitor: Optional[ClusterMonitor] = None
        self._t0 = 0.0
        self.input_rows = 0
        self.output_rows = 0
        self.notes = ""
        self.result: Optional[BenchmarkResult] = None

    def set_metrics(self, input_rows: Optional[int] = None,
                    output_rows: Optional[int] = None,
                    notes: Optional[str] = None) -> None:
        if input_rows is not None:
            self.input_rows = int(input_rows)
        if output_rows is not None:
            self.output_rows = int(output_rows)
        if notes is not None:
            self.notes = notes

    def __enter__(self) -> "BenchmarkRun":
        if self.enable_monitor:
            try:
                self._monitor = ClusterMonitor(self.monitor_interval)
                n = self._monitor.start()
                print(f"[run] resource monitor active on {n} node(s)")
            except Exception as exc:  # noqa: BLE001
                print(f"[run] resource monitor disabled ({exc})")
                self._monitor = None
        self._t0 = time.time()
        print(f"[run] START engine={self.engine} workload={self.workload} "
              f"sf={self.scale_factor} arch={self.cfg.arch}")
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        wall = time.time() - self._t0

        summary = {}
        if self._monitor is not None:
            try:
                summary = self._monitor.stop()
            except Exception as e:  # noqa: BLE001
                print(f"[run] monitor stop failed ({e})")

        status = "ok" if exc_type is None else "error"
        err = "" if exc_type is None else f"{exc_type.__name__}: {exc}"
        throughput = (self.input_rows / wall) if (wall > 0 and self.input_rows) else 0.0
        run_cost = self.cfg.cluster_hourly_cost() * wall / 3600.0

        self.result = BenchmarkResult(
            arch=self.cfg.arch, engine=self.engine, workload=self.workload,
            scale_factor=self.scale_factor, status=status, wall_seconds=round(wall, 3),
            input_rows=self.input_rows, output_rows=self.output_rows,
            throughput_rows_per_s=round(throughput, 2),
            head_instance=self.cfg.head_instance, worker_instance=self.cfg.worker_instance,
            num_workers=self.cfg.num_workers,
            cluster_hourly_cost_usd=round(self.cfg.cluster_hourly_cost(), 5),
            run_cost_usd=round(run_cost, 5),
            cpu_pct_avg=round(summary.get("cpu_pct_avg", 0.0), 2),
            cpu_pct_max=round(summary.get("cpu_pct_max", 0.0), 2),
            mem_used_gb_total_max=round(summary.get("mem_used_gb_total_max", 0.0), 2),
            disk_read_mb_total=round(summary.get("disk_read_mb_total", 0.0), 1),
            disk_write_mb_total=round(summary.get("disk_write_mb_total", 0.0), 1),
            net_recv_mb_total=round(summary.get("net_recv_mb_total", 0.0), 1),
            net_sent_mb_total=round(summary.get("net_sent_mb_total", 0.0), 1),
            error=err, notes=self.notes, resources=summary,
        )
        persist(self.result, self.cfg.results_dir, self.cfg.results_prefix)

        print(f"[run] DONE status={status} wall={wall:.1f}s "
              f"throughput={throughput:,.0f} rows/s run_cost=${run_cost:.4f}")
        if exc_type is not None:
            print(f"[run] ERROR {err}")
        return False  # never suppress exceptions
