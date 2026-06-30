#!/usr/bin/env bash
# Resolve the latest Amazon Linux 2023 AMI for a region + architecture from the
# public SSM parameter store. Paste the output into the cluster YAML's ImageId.
#
# (The EC2 console "quick start" list is irrelevant — ray up references the AMI
#  by ID, and these SSM parameters always point at the current AL2023 release.)
#
# Usage:
#   scripts/resolve_ami.sh <region> <arch>
#   scripts/resolve_ami.sh us-east-1 x86_64     # -> for cluster-m7i.yaml
#   scripts/resolve_ami.sh us-east-1 arm64       # -> for cluster-m8g.yaml
set -euo pipefail

REGION="${1:-us-east-1}"
ARCH="${2:-x86_64}"

case "$ARCH" in
  x86_64|amd64)  AL_ARCH=x86_64 ;;
  arm64|aarch64) AL_ARCH=arm64 ;;
  *) echo "unknown arch '$ARCH' (use x86_64 or arm64)" >&2; exit 1 ;;
esac

# Standard (non-minimal) AL2023 AMI on the default kernel.
PARAM="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-${AL_ARCH}"

echo "region=${REGION} arch=${ARCH} param=${PARAM}" >&2
aws ssm get-parameter \
  --region "$REGION" \
  --name "$PARAM" \
  --query 'Parameter.Value' \
  --output text
