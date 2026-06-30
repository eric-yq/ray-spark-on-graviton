#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Node bootstrap for the Ray benchmark cluster.
# Base image: Ubuntu 22.04 LTS (x86_64 on m7i, aarch64 on m8g).
# Runs IDENTICALLY on both architectures — apt + pip resolve the right arch.
#
# Responsibilities:
#   1. system packages + Amazon Corretto 17 JDK
#   2. scratch directories on the single large gp3 root volume
#   3. pinned Python deps (Ray / RayDP / Spark / Arrow) from requirements.txt
# Invoked by the Ray cluster launcher via `setup_commands`.
# ---------------------------------------------------------------------------
set -euxo pipefail

export DEBIAN_FRONTEND=noninteractive
BENCH_HOME=/home/ubuntu/bench
SCRATCH=/opt/bench/scratch

# --- 1. system packages -----------------------------------------------------
sudo apt-get update -y
sudo apt-get install -y --no-install-recommends \
  python3.10 python3.10-venv python3-pip \
  build-essential unzip wget curl ca-certificates gnupg

# --- 2. Amazon Corretto 17 (apt repo serves both amd64 + arm64) -------------
if ! dpkg -s java-17-amazon-corretto-jdk >/dev/null 2>&1; then
  wget -qO- https://apt.corretto.aws/corretto.key \
    | sudo gpg --dearmor -o /usr/share/keyrings/corretto.gpg
  echo "deb [signed-by=/usr/share/keyrings/corretto.gpg] https://apt.corretto.aws stable main" \
    | sudo tee /etc/apt/sources.list.d/corretto.list
  sudo apt-get update -y
  sudo apt-get install -y java-17-amazon-corretto-jdk
fi

# Resolve JAVA_HOME robustly across arch and persist for interactive shells.
JAVA_HOME_RESOLVED="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
echo "export JAVA_HOME=${JAVA_HOME_RESOLVED}" | sudo tee /etc/profile.d/javahome.sh >/dev/null

# --- 3. scratch dirs on the single 600GB gp3 root volume --------------------
# Single-disk design: root volume IS the big gp3 (8000 IOPS / 500 MB/s on workers).
# Ray object spill + Spark shuffle both target these dirs.
sudo mkdir -p "$SCRATCH/ray" "$SCRATCH/spark" "$SCRATCH/tmp"
sudo chown -R ubuntu:ubuntu /opt/bench

# --- 4. pinned Python deps --------------------------------------------------
sudo python3.10 -m pip install --upgrade pip
sudo python3.10 -m pip install -r "${BENCH_HOME}/requirements.txt"

# --- 5. Spark S3A connector jars (so SparkSQL can read s3a:// directly) ------
# PySpark 3.5.x bundles Hadoop 3.3.4 client libs -> pair with hadoop-aws 3.3.4
# and the matching aws-java-sdk-bundle. Placed in pyspark/jars so every RayDP
# executor (same install on each node) picks them up — no runtime Ivy download.
PYSPARK_JARS="$(python3.10 -c 'import os,pyspark;print(os.path.join(os.path.dirname(pyspark.__file__),"jars"))')"
MVN=https://repo1.maven.org/maven2
sudo wget -q -P "$PYSPARK_JARS" "$MVN/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
sudo wget -q -P "$PYSPARK_JARS" "$MVN/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

echo "===================================================================="
echo "node_setup complete"
echo "  arch:  $(uname -m)"
echo "  java:  $(java -version 2>&1 | head -1)"
echo "  JAVA_HOME: ${JAVA_HOME_RESOLVED}"
echo "  ray:   $(ray --version 2>&1 || echo 'NOT FOUND')"
echo "  scratch: ${SCRATCH}  ($(df -h /opt/bench | tail -1 | awk '{print $2" total, "$4" free"}'))"
echo "===================================================================="
