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
   git clone https://github.com/eric-yq/ray-spark-on-graviton.git
   yum install -y python3.11 python3.11-pip
   python3.11 -m pip install -r requirements.txt   # 或至少:ray[default]==2.44.1 boto3
   aws configure                                 # 凭证需具备 EC2 + PassRole 权限
   ```
3. **一个 S3 桶**,用于存数据和结果。
4. **集群节点的 IAM 角色 + 实例 profile** —— 由 `scripts/setup_iam.sh` **自动创建**
   (幂等;在控制机上跑)。不需要手动到 IAM 控制台建。

### IAM 配置

**(a) 实例 profile `ray-bench-node`** —— 挂在 head + worker 上。**不用手动建**,在控制机上
跑这个幂等脚本即可(存在就跳过,缺啥建啥):

```bash
BENCH_S3_BUCKET=your-bucket scripts/setup_iam.sh
```

它会确保有一个角色 + 同名实例 profile,策略如下。**worker 是 head 的 autoscaler 创建的**,
所以除了 S3 还需要 EC2 + `iam:PassRole`:

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
(上面这段就是 `setup_iam.sh` 实际写入的策略,这里只是给你参考。)

> 最小权限:其实只有 **head** 需要 EC2 + PassRole 这两段(它的 autoscaler 拉 worker)。
> 想更严格就拆成 `ray-bench-head`(S3 + EC2 + PassRole)和 `ray-bench-worker`(仅 S3),
> 并把两个 YAML 里的 `IamInstanceProfile.Name` 分别改成对应名字。

**(b) 控制机凭证** —— 你的 IAM 用户/角色需要具备启动和管理集群的 EC2 权限(RunInstances、
TerminateInstances、Describe*、CreateTags、CreateSecurityGroup 等),外加对 `ray-bench-node`
的 `iam:PassRole`。另外为了让 `setup_iam.sh` 能自动建角色,还需要 IAM 管理权限
(iam:GetRole/CreateRole/PutRolePolicy、iam:GetInstanceProfile/CreateInstanceProfile/AddRoleToInstanceProfile)。
参见 [Ray AWS launcher IAM 文档](https://docs.ray.io/en/latest/cluster/vms/getting-started.html)。

---

## 快速上手

除特别说明外,所有命令都在**控制机**上的仓库根目录执行。

### 1. 不需要改 YAML

被跟踪的 `cluster-m7i.yaml` / `cluster-m8g.yaml` 是**模板,不要改**,这样 `git pull` 永远不冲突。
`scripts/launch.sh` 会在拉起时自动解析 AMI,并渲染出一份 gitignore 的 `cluster-<arch>.local.yaml`。
region/AZ 取自 `BENCH_REGION` / `BENCH_AZ`(默认 `us-east-1` / `us-east-1a`);节点实例 profile 是
`ray-bench-node`(第 2 步创建)。

> 已经手改过被跟踪的 YAML 了?先还原,保证 pull 干净:
> `git checkout -- infra/ray-cluster/cluster-m7i.yaml infra/ray-cluster/cluster-m8g.yaml`
> 之后用下面的 `scripts/launch.sh` —— 不用再手填 ImageId。

### 2. 设置环境变量(数据 + 结果位置)

```bash
export BENCH_S3_BUCKET=your-bucket
export BENCH_REGION=us-east-1
export BENCH_DATA_PREFIX=s3://your-bucket/tpch
export BENCH_RESULTS_PREFIX=s3://your-bucket/results
```

一次性创建集群所需 IAM(幂等 —— 用控制机的凭证检查并只补缺失的):

```bash
scripts/setup_iam.sh
```

### 3. 拉起一套集群

```bash
scripts/launch.sh m7i                                              # 解析 AMI、渲染、ray up
ray rsync-up infra/ray-cluster/cluster-m7i.local.yaml ./ '~/ray-spark-on-graviton/'  # 把仓库同步到 head
ray attach   infra/ray-cluster/cluster-m7i.local.yaml             # SSH 进 head
```
拉起之后所有 ray 命令都用渲染出来的 `*.local.yaml`(不是模板)。

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
ray down infra/ray-cluster/cluster-m7i.local.yaml  # 销毁 m7i 集群
scripts/launch.sh m8g
ray rsync-up infra/ray-cluster/cluster-m8g.local.yaml ./ '~/ray-spark-on-graviton/'
ray attach   infra/ray-cluster/cluster-m8g.local.yaml
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
ray down infra/ray-cluster/cluster-m8g.local.yaml
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
infra/ray-cluster/   ray up 配置(m7i/m8g)、node_setup.sh、ray_systemd.sh
scripts/             setup_iam.sh、launch.sh、resolve_ami.sh、run_all.sh、report.py
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

## reboot 自愈(安全基线 reboot)

有些账号会对新建 EC2 实例跑安全基线并 **reboot OS**。普通 `ray start` 进程扛不过 reboot,
所以集群被设计成能自动恢复:

- **Ray 跑在 systemd 下**(`bench-ray.service`,`Restart=always`,开机自启),由 start 命令
  里的 `ray_systemd.sh` 安装。reboot 后 systemd 把 head/worker 的 Ray 进程拉回来;worker 用
  head 的私有 IP 重连(reboot 不变 IP)。`JAVA_HOME` / `PYSPARK_PYTHON` 写进了 unit,所以
  reboot 后 RayDP 的 Spark executor 仍然正常。
- **节点 setup 开机续跑**(`bench-setup.service`,oneshot,在 `node_setup.sh` 最前面就启用,
  用 `/opt/bench/.node_setup_done` 哨兵守门)。如果 reboot 打断了 dnf/pip,下次开机会自动
  重跑 `node_setup.sh` 直到完成 —— 脚本是幂等的(jar 原子下载、已装的包跳过)。

起好集群后验证(`ray attach <yaml>` 进 head):
```bash
systemctl status bench-setup.service bench-ray.service
ray status
```

恢复 / 注意事项:
- 如果 `ray up` 因为节点在**配置中途**(Ray 服务还没装上时)reboot 而报错,等基线 reboot
  稳定后**重跑 `ray up`** 即可 —— setup 幂等,很快补齐。
- 如果某个 **worker** 初次拉起时 reboot 太慢,导致 head 的 autoscaler 把它当失败重新拉
  (出现节点反复重建),要么申请把这些临时 benchmark 实例排除出自动 reboot 策略,要么改用
  预烘焙好基线的 golden AMI。

## 排错

- **Spark 的 python worker 报 `ModuleNotFoundError: No module named 'pyspark'/'ray'`**:
  executor 的 Python 必须能 import ray + pyspark + pyarrow + raydp。AL2023 默认 `python3`
  是 3.9(没有这些依赖),所以 `bench-ray` 的 systemd unit 固定了
  `PYSPARK_PYTHON=/usr/bin/python3.11`,node_setup 也把所有依赖装进 python3.11。
  driver 一律用 `python3.11` 启动(run_all.sh 已默认如此)。
- **Spark 无法读 S3**(`No FileSystem for scheme s3a`):node_setup 会把
  `hadoop-aws:3.3.4` + `aws-java-sdk-bundle` 放进 `pyspark/jars`;确认 setup_commands 已完成。
  Spark 读取使用 `s3a://` scheme 和 EC2 实例角色。
- **`ray up` 权限报错**:检查控制机的 EC2 权限和 `iam:PassRole`。
- **sf600 很慢 / 磁盘瓶颈**:属预期(spill)。要看更聚焦 CPU 的视角请对比 `sf100`,
  或调高 worker `BlockDeviceMappings` 里的 gp3 吞吐。
