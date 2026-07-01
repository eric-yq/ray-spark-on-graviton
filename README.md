# ray-spark-on-graviton

*English | [中文](README.zh-CN.md)*

A reproducible big-data benchmark for comparing AWS **Intel (m7i, m8i)** vs
**Graviton (m8g, m9g)** instances on two engines:

1. **Ray-only** — Ray Data / Ray Core workloads (scan, filter, group-by, sort, broadcast join).
2. **Ray + Spark hybrid** — SparkSQL running *on* the Ray cluster via [RayDP](https://github.com/oap-project/raydp),
   plus a genuine in-memory `Ray Data -> Spark` handoff.

Each architecture runs an identical cluster: **1 scheduler/head + 3 task/worker nodes**,
provisioned with the native Ray cluster launcher (`ray up`). Clusters differ **only** in
instance family — every software version, instance size, EBS spec, and workload is held
constant so the comparison isolates the hardware.

---

## Topology

Per role, all architectures use the same size class:

| Role   | Count | size    | vCPU / RAM   | Disk                          |
|--------|-------|---------|--------------|-------------------------------|
| head   | 1     | 2xlarge | 8 / 32 GiB   | 150 GB gp3                    |
| worker | 3     | 4xlarge | 16 / 64 GiB  | **600 GB gp3, 8000 IOPS, 500 MB/s** |

Worker cluster total: 48 vCPU / 192 GiB. The single 600 GB gp3 root volume on each
worker holds the OS plus all scratch (Ray object spill + Spark shuffle) under
`/opt/bench/scratch`.

### Supported architectures

| family | CPU                | head        | worker      | cluster on-demand $/hr* |
|--------|--------------------|-------------|-------------|-------------------------|
| m7i    | Intel, x86_64      | m7i.2xlarge | m7i.4xlarge | 2.8224 |
| m8i    | Intel, x86_64      | m8i.2xlarge | m8i.4xlarge | 2.9635 |
| m8g    | Graviton, arm64    | m8g.2xlarge | m8g.4xlarge | 2.5133 |
| m9g    | Graviton, arm64    | m9g.2xlarge | m9g.4xlarge | 2.7395 |

\* 1 head + 3 workers, us-east-1 Linux on-demand (user-provided 2026-07 figures;
override per instance via `BENCH_PRICE_<INSTANCE>`). m7i is the default report baseline.

## Software (identical on every architecture)

| Component | Version | Notes |
|-----------|---------|-------|
| OS        | Amazon Linux 2023 | standard AMI, default kernel; both x86_64 + arm64 |
| Python    | 3.11    | installed via dnf (`python3.11`); AL2023's default `python3` is 3.9 |
| Ray       | 2.44.1  | `ray[default,data]` |
| RayDP     | 1.6.5   | Spark-on-Ray (requires ray>=2.37, pyspark<=3.5.7) |
| PySpark   | 3.5.7   | pip bundle ships Spark + Hadoop 3.3.4 client |
| PyArrow   | 17.0.0  | |
| JDK       | Amazon Corretto 17 | from AL2023 repos (`java-17-amazon-corretto-devel`), both arches |
| TPC-H gen | tpchgen-cli 3.x | pure-Rust, ~20x faster than dbgen, multi-arch wheels |

---

## Data: TPC-H at three scales

Generated as Parquet directly to S3, identical bytes for both architectures
(TPC-H is deterministic — generate once, both clusters read the same copy).

| Scale  | Approx size | Purpose |
|--------|-------------|---------|
| `sf10` | ~10 GB      | smoke test |
| `sf100`| ~100 GB     | fits largely in 192 GiB RAM -> isolates **CPU** |
| `sf600`| ~600 GB     | spill-heavy -> realistic mixed **CPU + IO** |

> Note on `sf600`: at 600 GB vs 192 GiB cluster RAM, shuffle/sort workloads spill
> heavily to EBS, so disk throughput becomes part of what you measure. That is
> realistic, but if you want a purer CPU signal, lean on the `sf100` numbers.

S3 layout: `s3://<bucket>/tpch/<sf>/<table>/<table>.<part>.parquet`

---

## Prerequisites

1. **An AWS account** and a region (default `us-east-1`).
2. **A control machine** with Ray installed and AWS credentials configured. This can
   be your laptop or a small EC2 instance — it only orchestrates; it is *not* part of
   the benchmark cluster. Install the matching client:
   ```bash
   git clone https://github.com/eric-yq/ray-spark-on-graviton.git
   yum install -y python3.11 python3.11-pip
   python3.11 -m pip install -r requirements.txt # or at least: ray[default]==2.44.1 boto3
   aws configure                                 # creds with EC2 + PassRole permissions
   ```
3. **An S3 bucket** for data + results.
4. **IAM role + instance profile** for the cluster nodes — **auto-created** by
   `scripts/setup_iam.sh` (idempotent; run on the control machine). No manual
   IAM console steps.

### IAM setup

**(a) Instance profile `ray-bench-node`** — attached to head + workers. Don't
create it by hand; run the idempotent helper on the control machine — it checks
and creates only what's missing:

```bash
BENCH_S3_BUCKET=your-bucket scripts/setup_iam.sh
```

It ensures a role + same-named instance profile with the policy below. The HEAD's
autoscaler launches the workers, so EC2 + `iam:PassRole` are needed alongside S3:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "S3Data", "Effect": "Allow",
     "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
     "Resource": ["arn:aws:s3:::YOUR_BUCKET", "arn:aws:s3:::YOUR_BUCKET/*"]},
    {"Sid": "Ec2LaunchWorkers", "Effect": "Allow",
     "Action": ["ec2:RunInstances", "ec2:TerminateInstances",
                "ec2:CreateTags", "ec2:Describe*"],
     "Resource": "*"},
    {"Sid": "PassNodeRole", "Effect": "Allow",
     "Action": "iam:PassRole",
     "Resource": "arn:aws:iam::ACCOUNT_ID:role/ray-bench-node"}
  ]
}
```
(That JSON is exactly what `setup_iam.sh` applies — shown here for reference.)

> Least privilege: only the **head** actually needs the EC2 + PassRole statements
> (its autoscaler launches workers). For a stricter setup, split into
> `ray-bench-head` (S3 + EC2 + PassRole) and `ray-bench-worker` (S3 only), and set
> the two `IamInstanceProfile.Name` fields in the YAMLs accordingly.

**(b) Control-node credentials** — your IAM user/role needs EC2 permissions to launch
and manage the cluster (RunInstances, TerminateInstances, Describe*, CreateTags,
CreateSecurityGroup, etc.) plus `iam:PassRole` for `ray-bench-node`. To let
`setup_iam.sh` bootstrap the role, it also needs IAM management perms
(iam:GetRole/CreateRole/PutRolePolicy, iam:GetInstanceProfile/CreateInstanceProfile/AddRoleToInstanceProfile).
See the [Ray AWS launcher IAM reference](https://docs.ray.io/en/latest/cluster/vms/getting-started.html).

---

## Quickstart

All commands run from the repo root on your **control machine** unless noted.

### 1. No YAML editing needed

The tracked `cluster-m7i.yaml` / `cluster-m8g.yaml` are **templates — leave them
unedited** so `git pull` never conflicts. `scripts/launch.sh` resolves the AMI and
renders a gitignored `cluster-<arch>.local.yaml` at launch time. Region/AZ come from
`BENCH_REGION` / `BENCH_AZ` (default `us-east-1` / `us-east-1a`); the node instance
profile is `ray-bench-node` (created in step 2).

> Already edited a tracked YAML by hand? Revert it so pulls stay clean:
> `git checkout -- infra/ray-cluster/cluster-m7i.yaml infra/ray-cluster/cluster-m8g.yaml`
> then use `scripts/launch.sh` (below) — no manual ImageId editing.

### 2. Set environment (data + results location)

```bash
export BENCH_S3_BUCKET=your-bucket
export BENCH_REGION=us-east-1
export BENCH_DATA_PREFIX=s3://your-bucket/tpch
export BENCH_RESULTS_PREFIX=s3://your-bucket/results
```

Create the cluster IAM once (idempotent — checks and creates only what's missing,
using the control machine's credentials):

```bash
scripts/setup_iam.sh
```

### 3. Launch a cluster

```bash
scripts/launch.sh m7i                                              # resolve AMI, render, ray up
ray rsync-up infra/ray-cluster/cluster-m7i.local.yaml ./ '~/ray-spark-on-graviton/'  # sync repo to head
ray attach   infra/ray-cluster/cluster-m7i.local.yaml             # SSH into the head node
```
Use the rendered `*.local.yaml` (not the template) for every ray command after launch.
`launch.sh` accepts **`m7i` | `m8i` | `m8g` | `m9g`** (resolves the right x86_64/arm64 AMI
automatically). Run the full flow once per family you want to compare.

### 4. On the head node: generate data (once) and run the sweep

```bash
cd ~/ray-spark-on-graviton
# re-export the BENCH_* vars here too (or put them in ~/.bashrc on the head)

# Generate TPC-H data to S3 (do this ONCE; both clusters reuse it).
# NOTE: use python3.11 — that's where the deps live on AL2023.
python3.11 -m data.generators.gen_tpch --scale-factor sf100
python3.11 -m data.generators.gen_tpch --scale-factor sf600

# Run the full sweep (ray-only + ray+spark), 3 iterations each.
# run_all.sh already defaults to python3.11 (override with BENCH_PYTHON).
scripts/run_all.sh --repeat 3 sf10 sf100 sf600
```

Architecture is auto-detected from the head CPU, so results are tagged `m7i`.
Results append to `results/results.csv` and upload to `s3://.../results/`.

### 5. Repeat on the other architecture

```bash
exit                                               # leave the m7i head
ray down infra/ray-cluster/cluster-m7i.local.yaml  # terminate m7i cluster
scripts/launch.sh m8g
ray rsync-up infra/ray-cluster/cluster-m8g.local.yaml ./ '~/ray-spark-on-graviton/'
ray attach   infra/ray-cluster/cluster-m8g.local.yaml
# on head:
cd ~/ray-spark-on-graviton
scripts/run_all.sh --repeat 3 sf10 sf100 sf600     # data already in S3; no regen needed
```

### 6. Build the comparison report

From your control machine (pulls every cluster's results from S3):

```bash
python scripts/report.py --from-s3 s3://your-bucket/results   # baseline defaults to m7i
python scripts/report.py --from-s3 s3://your-bucket/results --baseline m8g
```
Produces `results/comparison.csv` and `.md` with one row per (workload, arch); each
non-baseline arch is scored against the baseline (default m7i):

| column | meaning |
|--------|---------|
| `best_s` / `median_s`  | best / median wall-clock across `--repeat` iterations |
| `cost_usd`             | on-demand cost of that best run (cluster $/hr × best_s) |
| `speedup_vs_m7i`       | `m7i_best / arch_best` — **> 1 means faster than m7i** |
| `priceperf_vs_m7i`     | `m7i_cost / arch_cost` — **> 1 means more work per dollar** |

(Baseline rows are 1.0 by definition. Use `--baseline` to compare against a different family.)

### 7. Tear down

```bash
ray down infra/ray-cluster/cluster-m8g.local.yaml
```

---

## Workloads

### Ray-only (`benchmarks/ray_only`)
| name | what it stresses |
|------|------------------|
| `read_sum`         | parquet read + decode throughput (light CPU) |
| `filter_project`   | selective scan + arithmetic (parallel, no shuffle) |
| `q1`               | TPC-H Q1: low-cardinality group-by + many aggregates |
| `sort`             | global sort of lineitem (full shuffle, heaviest spill) |
| `groupby_orderkey` | high-cardinality group-by (join-scale shuffle) |
| `broadcast_join`   | map-side join lineitem ⨝ supplier, then per-nation sum |

> Ray Data 2.44 has no native `Dataset.join`, so the Ray-only join is a broadcast
> (map-side) join; large hash-join shuffle cost is represented by `sort` and
> `groupby_orderkey`. Full shuffle joins are exercised on the Spark side.

### Ray + Spark via RayDP (`benchmarks/ray_spark`)
| name | what it stresses |
|------|------------------|
| `q1`          | low-cardinality group + many aggregates |
| `q6`          | selective filter + single aggregate (scan-bound) |
| `q5`          | 6-table star join + group |
| `q9`          | 6-table join + derived arithmetic + group (heaviest) |
| `join_orders` | lineitem ⨝ orders, group by priority (large hash join) |
| `hybrid_etl`  | **Ray Data** featurizes lineitem -> `to_spark` -> **SparkSQL** aggregation |

Run a subset directly:
```bash
python3.11 -m benchmarks.ray_only.run  --sf sf100 --workload q1,sort --repeat 3
python3.11 -m benchmarks.ray_spark.run --sf sf100 --workload q5,q9,hybrid_etl --repeat 3
```

---

## Methodology / fairness

- Identical instance sizes, EBS spec, single AZ + placement, AMI family, and pinned
  software versions across both architectures.
- Spark executor sizing is constant (default: 1 executor/worker × 15 cores × 40 GB).
- Each workload runs `--repeat N` times; the report uses the **best** (min) wall time
  to reduce noise, and also records the median.
- Cost is computed from on-demand Linux pricing (us-east-1 defaults baked into
  `common/config.py`; override via `BENCH_PRICE_*` or refresh from the Pricing API).
- The result row also captures per-node CPU / memory / disk / network sampled by a
  lightweight Ray-actor monitor (one actor per node), so you can see *why* a number
  looks the way it does (e.g. disk-bound at sf600).

## Project layout

```
infra/ray-cluster/   ray up YAMLs (m7i/m8g), node_setup.sh, ray_systemd.sh
scripts/             setup_iam.sh, launch.sh, resolve_ami.sh, run_all.sh, report.py
common/              config, metrics, resource_monitor, runner
data/generators/     gen_tpch.py (distributed TPC-H -> S3)
benchmarks/ray_only/ Ray Data/Core workloads + runner
benchmarks/ray_spark/RayDP SparkSQL + hybrid workloads + runner
results/             results.csv, raw/<run_id>.json, comparison.{csv,md}
```

## Configuration (environment variables)

| var | default | purpose |
|-----|---------|---------|
| `BENCH_S3_BUCKET`        | —              | bucket for data + results |
| `BENCH_DATA_PREFIX`      | `s3://<bucket>/tpch`    | TPC-H root |
| `BENCH_RESULTS_PREFIX`   | `s3://<bucket>/results` | result upload root (empty = local only) |
| `BENCH_RESULTS_DIR`      | `results`      | local results dir |
| `BENCH_REGION`           | `us-east-1`    | AWS region |
| `BENCH_ARCH`             | auto (EC2 metadata) | force arch tag: `m7i`/`m8i`/`m8g`/`m9g` |
| `BENCH_SCRATCH`          | `/opt/bench/scratch` | Ray/Spark spill root |
| `BENCH_SPARK_EXECUTORS`  | `3`            | Spark executors (= workers) |
| `BENCH_SPARK_EXECUTOR_CORES` | `15`       | cores per executor |
| `BENCH_SPARK_EXECUTOR_MEMORY`| `40GB`     | memory per executor |
| `BENCH_PRICE_<INSTANCE>` | built-in       | e.g. `BENCH_PRICE_M8G_4XLARGE=0.71` |

## Reboot resilience (security-baseline reboots)

Some accounts apply a security baseline to freshly launched EC2 instances and
**reboot the OS**. Plain `ray start` processes don't survive a reboot, so the
cluster is built to recover on its own:

- **Ray runs under systemd** (`bench-ray.service`, `Restart=always`, enabled on
  boot), installed by `ray_systemd.sh` from the start commands. After a reboot,
  systemd restarts the head/worker Ray process; workers reconnect via the head's
  private IP (unchanged across a reboot). `JAVA_HOME` / `PYSPARK_PYTHON` are baked
  into the unit so RayDP's Spark executors still work post-reboot.
- **Node setup resumes on boot** (`bench-setup.service`, a oneshot enabled early in
  `node_setup.sh`, guarded by the `/opt/bench/.node_setup_done` sentinel). If a
  reboot interrupts the dnf/pip install, the next boot re-runs `node_setup.sh`
  until it completes — the script is idempotent (jars download atomically, already
  installed packages are skipped).

Verify after bring-up (`ray attach <yaml>` into the head):
```bash
systemctl status bench-setup.service bench-ray.service
ray status
```

Recovery / caveats:
- If `ray up` errors because a node rebooted **mid-provision** (before the Ray
  service was installed), wait for the baseline reboot to settle and re-run
  `ray up` — setup is idempotent and finishes quickly.
- If a *worker* reboot during initial bring-up is slow enough that the head
  autoscaler relaunches it (node churn), either ask for these ephemeral benchmark
  instances to be excluded from the auto-reboot policy, or pre-bake a golden AMI
  with the baseline already applied.

## Troubleshooting

- **Spark `ModuleNotFoundError: No module named 'pyspark'/'ray'` in a python worker**:
  the executor's Python must have ray + pyspark + pyarrow + raydp. On AL2023 the
  default `python3` is 3.9 (no deps), so the `bench-ray` systemd unit pins
  `PYSPARK_PYTHON=/usr/bin/python3.11` and node_setup installs all deps into
  python3.11. Always invoke the driver as `python3.11` (run_all.sh does).
- **Spark can't read S3** (`No FileSystem for scheme s3a`): node_setup stages
  `hadoop-aws:3.3.4` + `aws-java-sdk-bundle` into `pyspark/jars`; ensure setup_commands
  completed. Spark reads use the `s3a://` scheme and the EC2 instance role.
- **`ray up` permission errors**: check control-node EC2 permissions + `iam:PassRole`.
- **Slow / disk-bound at sf600**: expected (spill). Compare against `sf100` for a
  CPU-focused view, or raise the gp3 throughput in the worker `BlockDeviceMappings`.
