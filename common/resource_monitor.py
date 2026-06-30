"""Cluster-wide resource sampling using one Ray actor pinned to each node.

The driver starts a :class:`ClusterMonitor` around a workload; it places a
``NodeMonitor`` actor on every alive node (via node-affinity scheduling) that
samples CPU / memory / disk / network with ``psutil`` in a background thread.
On stop it returns a per-node summary plus a cluster aggregate that gets folded
into the benchmark result row.

No external monitoring stack required — keeps the comparison self-contained.
"""
from __future__ import annotations

import threading
import time
from typing import Dict, List

import psutil
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@ray.remote(num_cpus=0)
class NodeMonitor:
    """Samples local node resource usage on a background thread."""

    def __init__(self, interval: float = 2.0):
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._cpu_samples: List[float] = []
        self._mem_used_gb: List[float] = []
        self._ncpu = psutil.cpu_count(logical=True) or 1
        try:
            self._node = ray.util.get_node_ip_address()
        except Exception:
            self._node = "unknown"
        psutil.cpu_percent(interval=None)        # prime the counter
        self._disk0 = psutil.disk_io_counters()
        self._net0 = psutil.net_io_counters()
        self._t0 = time.time()

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._cpu_samples.append(psutil.cpu_percent(interval=None))
            self._mem_used_gb.append(psutil.virtual_memory().used / 1e9)
            self._stop.wait(self.interval)

    def start(self) -> str:
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self._node

    def stop(self) -> Dict:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        disk1 = psutil.disk_io_counters()
        net1 = psutil.net_io_counters()
        cpu = self._cpu_samples or [0.0]
        mem = self._mem_used_gb or [0.0]
        return {
            "node": self._node,
            "ncpu": self._ncpu,
            "samples": len(self._cpu_samples),
            "duration_s": time.time() - self._t0,
            "cpu_pct_avg": sum(cpu) / len(cpu),
            "cpu_pct_max": max(cpu),
            "mem_used_gb_avg": sum(mem) / len(mem),
            "mem_used_gb_max": max(mem),
            "disk_read_mb": (disk1.read_bytes - self._disk0.read_bytes) / 1e6,
            "disk_write_mb": (disk1.write_bytes - self._disk0.write_bytes) / 1e6,
            "net_recv_mb": (net1.bytes_recv - self._net0.bytes_recv) / 1e6,
            "net_sent_mb": (net1.bytes_sent - self._net0.bytes_sent) / 1e6,
        }


class ClusterMonitor:
    """Fan-out resource sampling across all alive Ray nodes."""

    def __init__(self, interval: float = 2.0):
        self.interval = interval
        self._monitors: List = []

    def start(self) -> int:
        nodes = [n for n in ray.nodes() if n.get("Alive")]
        for n in nodes:
            strat = NodeAffinitySchedulingStrategy(n["NodeID"], soft=False)
            mon = NodeMonitor.options(scheduling_strategy=strat).remote(self.interval)
            ray.get(mon.start.remote())
            self._monitors.append(mon)
        return len(self._monitors)

    def stop(self) -> Dict:
        if not self._monitors:
            return {}
        per_node = ray.get([m.stop.remote() for m in self._monitors])
        for m in self._monitors:
            ray.kill(m)
        self._monitors = []
        return self._aggregate(per_node)

    @staticmethod
    def _aggregate(per_node: List[Dict]) -> Dict:
        if not per_node:
            return {}
        n = len(per_node)
        return {
            "nodes": n,
            # cluster CPU% = mean of per-node averages (each already 0..100 of that node)
            "cpu_pct_avg": sum(d["cpu_pct_avg"] for d in per_node) / n,
            "cpu_pct_max": max(d["cpu_pct_max"] for d in per_node),
            "mem_used_gb_total_avg": sum(d["mem_used_gb_avg"] for d in per_node),
            "mem_used_gb_total_max": sum(d["mem_used_gb_max"] for d in per_node),
            "disk_read_mb_total": sum(d["disk_read_mb"] for d in per_node),
            "disk_write_mb_total": sum(d["disk_write_mb"] for d in per_node),
            "net_recv_mb_total": sum(d["net_recv_mb"] for d in per_node),
            "net_sent_mb_total": sum(d["net_sent_mb"] for d in per_node),
            "per_node": per_node,
        }
