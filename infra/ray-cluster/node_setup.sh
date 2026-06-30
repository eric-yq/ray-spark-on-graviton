#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Node bootstrap for the Ray benchmark cluster.
# Base image: Amazon Linux 2023 (x86_64 on m7i, aarch64 on m8g).
# Runs IDENTICALLY on both architectures — dnf + pip resolve the right arch.
#
# Note: AL2023's default `python3` is 3.9. We install Python 3.11 explicitly and
# pin EVERYTHING (driver + Spark workers) to python3.11, so the deps installed
# here are the ones Ray/Spark actually use.
#
# Responsibilities:
#   1. Python 3.11 + Amazon Corretto 17 JDK (both from AL2023 repos)
#   2. scratch directories on the single large gp3 root volume
#   3. pinned Python deps + Spark S3A connector jars
# Invoked by the Ray cluster launcher via `setup_commands`.
# ---------------------------------------------------------------------------
set -euxo pipefail

BENCH_HOME=/home/ec2-user/bench
SCRATCH=/opt/bench/scratch
PY=python3.11

# --- 0. install the boot-time RESUME service FIRST --------------------------
# Installed before the slow dnf/pip steps so the window where a reboot leaves no
# recovery path is tiny. If a security-baseline reboot interrupts this script,
# systemd re-runs it on the next boot (guarded by the completion sentinel at the
# end) until setup finishes — no manual intervention needed.
sudo tee /etc/systemd/system/bench-setup.service >/dev/null <<EOF
[Unit]
Description=Resume benchmark node setup until complete
After=network-online.target
Wants=network-online.target
ConditionPathExists=!/opt/bench/.node_setup_done
[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/bin/env bash ${BENCH_HOME}/node_setup.sh
[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable bench-setup.service    # enable for future boots; do NOT start now

# --- 1. system packages (dnf) ----------------------------------------------
# Corretto 17 + Python 3.11 are both in the default AL2023 repos.
sudo dnf install -y \
  "${PY}" "${PY}-pip" "${PY}-devel" \
  java-17-amazon-corretto-devel \
  gcc tar gzip

# Resolve JAVA_HOME (Corretto -> /usr/lib/jvm/java-17-amazon-corretto.<arch>)
# and persist for interactive shells.
JAVA_HOME_RESOLVED="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
echo "export JAVA_HOME=${JAVA_HOME_RESOLVED}" | sudo tee /etc/profile.d/javahome.sh >/dev/null

# --- 2. scratch dirs on the single 600GB gp3 root volume --------------------
# Single-disk design: root volume IS the big gp3 (8000 IOPS / 500 MB/s on workers).
# Ray object spill + Spark shuffle both target these dirs.
sudo mkdir -p "$SCRATCH/ray" "$SCRATCH/spark" "$SCRATCH/tmp"
sudo chown -R ec2-user:ec2-user /opt/bench

# --- 3. pinned Python deps (into python3.11) --------------------------------
sudo "$PY" -m pip install --upgrade pip
sudo "$PY" -m pip install -r "${BENCH_HOME}/requirements.txt"

# --- 4. Spark S3A connector jars (idempotent + atomic; safe after a reboot) -
# PySpark 3.5.x bundles Hadoop 3.3.4 client libs -> pair with hadoop-aws 3.3.4
# and the matching aws-java-sdk-bundle. Placed in pyspark/jars so every RayDP
# executor (same install on each node) picks them up — no runtime Ivy download.
PYSPARK_JARS="$("$PY" -c 'import os,pyspark;print(os.path.join(os.path.dirname(pyspark.__file__),"jars"))')"
MVN=https://repo1.maven.org/maven2

# Download only if missing, and write atomically: a reboot mid-download leaves a
# .part file (overwritten next run), never a half-written .jar on the classpath.
fetch_jar() {
  local url="$1" dest="$2"
  [ -s "$dest" ] && return 0
  sudo curl -fsSL "$url" -o "${dest}.part"
  sudo mv -f "${dest}.part" "$dest"
}
fetch_jar "$MVN/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
          "$PYSPARK_JARS/hadoop-aws-3.3.4.jar"
fetch_jar "$MVN/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
          "$PYSPARK_JARS/aws-java-sdk-bundle-1.12.262.jar"

# Completion sentinel — lets a re-run / boot-time service know setup finished.
sudo touch /opt/bench/.node_setup_done

echo "===================================================================="
echo "node_setup complete"
echo "  arch:    $(uname -m)"
echo "  python:  $($PY --version)"
echo "  java:    $(java -version 2>&1 | head -1)"
echo "  JAVA_HOME: ${JAVA_HOME_RESOLVED}"
echo "  ray:     $($PY -c 'import ray; print(ray.__version__)' 2>&1)"
echo "  scratch: ${SCRATCH}  ($(df -h /opt/bench | tail -1 | awk '{print $2" total, "$4" free"}'))"
echo "===================================================================="
