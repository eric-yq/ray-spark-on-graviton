# ray-spark-on-graviton

*[English](README.md) | 中文*

一套可复现的大数据 benchmark,用于对比 **AWS m7i(Intel Sapphire Rapids, x86_64)** 与
**m8g(AWS Graviton4, aarch64)** 在两种引擎上的性能:

1. **纯 Ray** —— Ray Data / Ray Core 负载(扫描、过滤、分组聚合、排序、broadcast join)。
2. **Ray + Spark 混合** —— 通过 [RayDP](https://github.com/oap-project/raydp) 让 SparkSQL
   *跑在* Ray 集群之上,外加一个真正的内存级 `Ray Data -> Spark` 交接。

两套架构跑完全对称的集群:**1 个调度/head + 3 个任务/worker 节点**,用 Ray 原生 cluster
launcher(`ray up`)拉起。两套集群**只有 CPU 架构不同** —— 软件版本、实例规格、EBS 配置、
负载全部保持一致,从而让对比只反映硬件差异。

---

## 拓扑

| 角色   | 数量 | m7i           | m8g           | vCPU / 内存  | 磁盘                          |
|--------|------|---------------|---------------|--------------|-------------------------------|
| head   | 1    | m7i.2xlarge   | m8g.2xlarge   | 8 / 32 GiB   | 150 GB gp3                    |
| worker | 3    | m7i.4xlarge   | m8g.4xlarge   | 16 / 64 GiB  | **600 GB gp3, 8000 IOPS, 500 MB/s** |

worker 侧合计 48 vCPU / 192 GiB。每个 worker 的单块 600 GB gp3 根卷承载操作系统以及全部
scratch(Ray object spill + Spark shuffle),统一放在 `/opt/bench/scratch` 下。

## 软件(两套架构完全一致)

| 组件      | 版本    | 说明 |
|-----------|---------|------|
| 操作系统  | Amazon Linux 2023 | 标准 AMI、默认内核;x86_64 + arm64 都有 |
| Python    | 3.11    | 用 dnf 安装(`python3.11`);AL2023 默认 `python3` 是 3.9 |
| Ray       | 2.44.1  | `ray[default,data]` |
| RayDP     | 1.6.5   | Spark-on-Ray(要求 ray>=2.37、pyspark<=3.5.7) |
| PySpark   | 3.5.7   | pip 包自带 Spark + Hadoop 3.3.4 客户端 |
| PyArrow   | 17.0.0  | |
| JDK       | Amazon Corretto 17 | 来自 AL2023 仓库(`java-17-amazon-corretto-devel`),两架构通用 |
| TPC-H 生成器 | tpchgen-cli 3.x | 纯 Rust,比 dbgen 快约 20x,多架构 wheel |

---

## 数据:三档 TPC-H 规模

直接以 Parquet 生成到 S3,两套架构读同一份(TPC-H 是确定性的 —— 只生成一次,两套集群复用)。

| 规模   | 约大小  | 用途 |
|--------|---------|------|
| `sf10` | ~10 GB  | 冒烟测试 |
| `sf100`| ~100 GB | 基本能进 192 GiB 内存 -> 聚焦 **CPU** |
| `sf600`| ~600 GB | 大量 spill -> 真实的 **CPU + IO** 混合 |

> 关于 `sf600`:600 GB 对上 192 GiB 集群内存,shuffle/sort 类负载会大量 spill 到 EBS,
> 因此磁盘吞吐会成为测量的一部分。这很真实,但如果想要更纯的 CPU 信号,请以 `sf100` 的数据为准。

S3 目录结构:`s3://<bucket>/tpch/<sf>/<table>/<table>.<part>.parquet`

---

## 前置条件

1. **一个 AWS 账号**和一个区域(默认 `us-east-1`)。
2. **一台控制机**,装好 Ray 并配置好 AWS 凭证。可以是你的笔记本或一台小 EC2 —— 它只负责
   编排,**不属于** benchmark 集群。安装匹配的客户端:
   ```bash
   python3 -m pip install -r requirements.txt   # 或至少:ray[default]==2.44.1 boto3
   aws configure                                 # 凭证需具备 EC2 + PassRole 权限
   ```
3. **一个 S3 桶**,用于存数据和结果。
4. **一个名为 `ray-bench-node` 的 EC2 实例 profile**,供集群节点使用(见下)。

### IAM 配置

**(a) 实例 profile `ray-bench-node`** —— 挂在 head + worker 上,授予 S3 访问:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"],
     "Resource": ["arn:aws:s3:::YOUR_BUCKET", "arn:aws:s3:::YOUR_BUCKET/*"]},
    {"Effect": "Allow", "Action": ["s3:PutObject"],
     "Resource": ["arn:aws:s3:::YOUR_BUCKET/results/*", "arn:aws:s3:::YOUR_BUCKET/tpch/*"]}
  ]
}
```
创建角色 `ray-bench-node`(信任主体:ec2.amazonaws.com),附上该策略,并创建同名实例 profile。

**(b) 控制机凭证** —— 你的 IAM 用户/角色需要具备启动和管理集群的 EC2 权限(RunInstances、
TerminateInstances、Describe*、CreateTags、CreateSecurityGroup 等),外加对 `ray-bench-node`
的 `iam:PassRole`。参见
[Ray AWS launcher IAM 文档](https://docs.ray.io/en/latest/cluster/vms/getting-started.html)。

---

## 快速上手

除特别说明外,所有命令都在**控制机**上的仓库根目录执行。

### 1. 配置集群 YAML

为每个架构解析 Amazon Linux 2023 AMI,填入 `ImageId` 字段:

```bash
scripts/resolve_ami.sh us-east-1 x86_64    # -> 填到 infra/ray-cluster/cluster-m7i.yaml 的 ImageId
scripts/resolve_ami.sh us-east-1 arm64     # -> 填到 infra/ray-cluster/cluster-m8g.yaml 的 ImageId
```
同时确认两个 YAML 里的 `region`、`availability_zone`、`IamInstanceProfile.Name`。

### 2. 设置环境变量(数据 + 结果位置)

```bash
export BENCH_S3_BUCKET=your-bucket
export BENCH_REGION=us-east-1
export BENCH_DATA_PREFIX=s3://your-bucket/tpch
export BENCH_RESULTS_PREFIX=s3://your-bucket/results
```

### 3. 拉起一套集群

```bash
ray up infra/ray-cluster/cluster-m7i.yaml          # 几分钟(会装依赖)
ray rsync-up infra/ray-cluster/cluster-m7i.yaml ./ '~/ray-spark-on-graviton/'   # 把本仓库同步到 head
ray attach infra/ray-cluster/cluster-m7i.yaml      # SSH 进 head 节点
```

### 4. 在 head 上:生成数据(一次)并跑全套

```bash
cd ~/ray-spark-on-graviton
# 在 head 上同样 export 这些 BENCH_* 变量(或写进 head 的 ~/.bashrc)

# 生成 TPC-H 数据到 S3(只做一次;两套集群复用)。
# 注意:用 python3.11 —— AL2023 上依赖装在这里。
python3.11 -m data.generators.gen_tpch --scale-factor sf100
python3.11 -m data.generators.gen_tpch --scale-factor sf600

# 跑全套(纯 Ray + Ray+Spark),每个负载 3 次。
# run_all.sh 默认就用 python3.11(可用 BENCH_PYTHON 覆盖)。
scripts/run_all.sh --repeat 3 sf10 sf100 sf600
```

架构会从 head 的 CPU 自动识别,所以结果会被打上 `m7i` 标签。结果追加到 `results/results.csv`
并上传到 `s3://.../results/`。

### 5. 在另一套架构上重复

```bash
exit                                               # 退出 m7i head
ray down infra/ray-cluster/cluster-m7i.yaml        # 销毁 m7i 集群
ray up infra/ray-cluster/cluster-m8g.yaml
ray rsync-up infra/ray-cluster/cluster-m8g.yaml ./ '~/ray-spark-on-graviton/'
ray attach infra/ray-cluster/cluster-m8g.yaml
# 在 head 上:
cd ~/ray-spark-on-graviton
scripts/run_all.sh --repeat 3 sf10 sf100 sf600     # 数据已在 S3,无需重新生成
```

### 6. 生成对比报告

在控制机上执行(从 S3 拉取两套集群的结果):

```bash
python scripts/report.py --from-s3 s3://your-bucket/results
```
产出 `results/comparison.csv` 和 `results/comparison.md`,每个负载包含:

| 指标 | 含义 |
|------|------|
| `speedup_m8g`     | `m7i_best_wall / m8g_best_wall` —— **> 1 表示 m8g 更快** |
| `cost_savings_pct`| 取最优墙钟时的 `(m7i_cost - m8g_cost) / m7i_cost` —— **> 0 表示 m8g 更便宜** |
| `price_perf_m8g`  | `m7i_cost_per_run / m8g_cost_per_run` —— **> 1 表示 m8g 性价比更高** |

### 7. 销毁集群

```bash
ray down infra/ray-cluster/cluster-m8g.yaml
```

---

## 负载

### 纯 Ray(`benchmarks/ray_only`)
| 名称 | 压测点 |
|------|--------|
| `read_sum`         | parquet 读取 + 解码吞吐(轻 CPU) |
| `filter_project`   | 选择性扫描 + 算术(并行,无 shuffle) |
| `q1`               | TPC-H Q1:低基数分组 + 多个聚合 |
| `sort`             | lineitem 全局排序(全 shuffle,spill 最重) |
| `groupby_orderkey` | 高基数分组(join 级别的 shuffle) |
| `broadcast_join`   | map 端 join lineitem ⨝ supplier,再按 nation 求和 |

> Ray Data 2.44 没有原生 `Dataset.join`,所以纯 Ray 的 join 用 broadcast(map 端)join;
> 大表 hash-join 的 shuffle 成本由 `sort` 和 `groupby_orderkey` 体现。完整的 shuffle join
> 在 Spark 侧覆盖。

### Ray + Spark(经 RayDP,`benchmarks/ray_spark`)
| 名称 | 压测点 |
|------|--------|
| `q1`          | 低基数分组 + 多聚合 |
| `q6`          | 选择性过滤 + 单聚合(扫描密集) |
| `q5`          | 6 表星型 join + 分组 |
| `q9`          | 6 表 join + 派生算术 + 分组(最重) |
| `join_orders` | lineitem ⨝ orders,按 priority 分组(大 hash join) |
| `hybrid_etl`  | **Ray Data** 对 lineitem 做特征化 -> `to_spark` -> **SparkSQL** 聚合 |

直接跑某个子集:
```bash
python3.11 -m benchmarks.ray_only.run  --sf sf100 --workload q1,sort --repeat 3
python3.11 -m benchmarks.ray_spark.run --sf sf100 --workload q5,q9,hybrid_etl --repeat 3
```

---

## 方法论 / 公平性

- 两套架构的实例规格、EBS 配置、单 AZ + 放置组、AMI 系列、锁定的软件版本完全一致。
- Spark executor 规格固定(默认:每 worker 1 个 executor × 15 核 × 40 GB)。
- 每个负载跑 `--repeat N` 次;报告取**最优**(最小)墙钟时间以降低噪声,同时记录中位数。
- 成本按按需 Linux 价格计算(us-east-1 默认值内置在 `common/config.py`;可用 `BENCH_PRICE_*`
  覆盖或从 Pricing API 刷新)。
- 结果行还会记录由一个轻量 Ray-actor 监控器(每节点一个 actor)采样的各节点 CPU / 内存 /
  磁盘 / 网络,便于解释某个数字*为什么*这样(比如 sf600 时是否磁盘瓶颈)。

## 项目结构

```
infra/ray-cluster/   ray up 配置(m7i/m8g)、node_setup.sh
scripts/             resolve_ami.sh、run_all.sh、report.py
common/              config、metrics、resource_monitor、runner
data/generators/     gen_tpch.py(分布式 TPC-H -> S3)
benchmarks/ray_only/ Ray Data/Core 负载 + 运行器
benchmarks/ray_spark/RayDP SparkSQL + 混合负载 + 运行器
results/             results.csv、raw/<run_id>.json、comparison.{csv,md}
```

## 配置(环境变量)

| 变量 | 默认 | 用途 |
|------|------|------|
| `BENCH_S3_BUCKET`        | —              | 数据 + 结果的桶 |
| `BENCH_DATA_PREFIX`      | `s3://<bucket>/tpch`    | TPC-H 根目录 |
| `BENCH_RESULTS_PREFIX`   | `s3://<bucket>/results` | 结果上传根目录(留空 = 仅本地) |
| `BENCH_RESULTS_DIR`      | `results`      | 本地结果目录 |
| `BENCH_REGION`           | `us-east-1`    | AWS 区域 |
| `BENCH_ARCH`             | 自动(CPU)    | `m7i` 或 `m8g` 标签覆盖 |
| `BENCH_SCRATCH`          | `/opt/bench/scratch` | Ray/Spark spill 根目录 |
| `BENCH_SPARK_EXECUTORS`  | `3`            | Spark executor 数(= worker 数) |
| `BENCH_SPARK_EXECUTOR_CORES` | `15`       | 每 executor 核数 |
| `BENCH_SPARK_EXECUTOR_MEMORY`| `40GB`     | 每 executor 内存 |
| `BENCH_PRICE_<INSTANCE>` | 内置           | 例如 `BENCH_PRICE_M8G_4XLARGE=0.71` |

## 排错

- **Spark 的 python worker 报 `ModuleNotFoundError: No module named 'pyspark'/'ray'`**:
  executor 的 Python 必须能 import ray + pyspark + pyarrow + raydp。AL2023 默认 `python3`
  是 3.9(没有这些依赖),所以集群 YAML 在 `ray start` 上固定了
  `PYSPARK_PYTHON=/usr/bin/python3.11`,node_setup 也把所有依赖装进 python3.11。
  driver 一律用 `python3.11` 启动(run_all.sh 已默认如此)。
- **Spark 无法读 S3**(`No FileSystem for scheme s3a`):node_setup 会把
  `hadoop-aws:3.3.4` + `aws-java-sdk-bundle` 放进 `pyspark/jars`;确认 setup_commands 已完成。
  Spark 读取使用 `s3a://` scheme 和 EC2 实例角色。
- **`ray up` 权限报错**:检查控制机的 EC2 权限和 `iam:PassRole`。
- **sf600 很慢 / 磁盘瓶颈**:属预期(spill)。要看更聚焦 CPU 的视角请对比 `sf100`,
  或调高 worker `BlockDeviceMappings` 里的 gp3 吞吐。
