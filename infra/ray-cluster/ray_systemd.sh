#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Install + (re)start a systemd service that runs the Ray head/worker process,
# so Ray survives the security-baseline reboot (Restart=always, enabled on boot).
#
# Invoked by the cluster YAML start commands:
#     ray_systemd.sh head
#     ray_systemd.sh worker <HEAD_IP>
#
# Why systemd: `ray start` launches daemons that do NOT come back after an OS
# reboot. This unit restarts them on boot and on crash; the worker's baked-in
# head IP stays valid because a reboot keeps the instance's private IP.
# ---------------------------------------------------------------------------
set -euo pipefail

ROLE="${1:?usage: ray_systemd.sh head|worker [head_ip]}"
HEAD_IP="${2:-}"

RAY_BIN="$(command -v ray || echo /usr/local/bin/ray)"
TEMP_DIR=/opt/bench/scratch/ray
BOOTSTRAP=/home/ec2-user/ray_bootstrap_config.yaml   # written by `ray up` on the head
JAVA_HOME_RESOLVED="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"

case "$ROLE" in
  head)
    EXEC="${RAY_BIN} start --head --port=6379 --temp-dir=${TEMP_DIR} --dashboard-host=0.0.0.0 --autoscaling-config=${BOOTSTRAP} --block"
    ;;
  worker)
    [ -n "$HEAD_IP" ] || { echo "worker role requires HEAD_IP argument" >&2; exit 1; }
    EXEC="${RAY_BIN} start --address=${HEAD_IP}:6379 --temp-dir=${TEMP_DIR} --block"
    ;;
  *) echo "unknown role '$ROLE' (use head|worker)" >&2; exit 1 ;;
esac

# JAVA_HOME + PYSPARK_PYTHON live in the unit so that after a reboot the RayDP
# Spark executors (Ray workers spawned by the raylet) inherit them correctly.
sudo tee /etc/systemd/system/bench-ray.service >/dev/null <<EOF
[Unit]
Description=Ray ${ROLE} (benchmark, reboot-resilient)
After=network-online.target bench-setup.service
Wants=network-online.target
Requires=bench-setup.service
[Service]
Type=simple
User=ec2-user
Environment=PATH=/usr/local/bin:/usr/bin:/bin
Environment=JAVA_HOME=${JAVA_HOME_RESOLVED}
Environment=PYSPARK_PYTHON=/usr/bin/python3.11
Environment=PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11
ExecStartPre=-${RAY_BIN} stop
ExecStart=${EXEC}
Restart=always
RestartSec=10
[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable bench-ray.service
sudo systemctl restart bench-ray.service     # (re)start now; survives future reboots
echo "bench-ray.service (${ROLE}) installed, enabled, started"
