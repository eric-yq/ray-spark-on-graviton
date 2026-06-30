#!/usr/bin/env bash
# Resolve the latest Canonical Ubuntu 22.04 LTS AMI for a region + architecture
# from the public SSM parameter store. Paste the output into the cluster YAML's
# ImageId field.
#
# Usage:
#   scripts/resolve_ami.sh <region> <arch>
#   scripts/resolve_ami.sh us-east-1 x86_64     # -> for cluster-m7i.yaml
#   scripts/resolve_ami.sh us-east-1 arm64       # -> for cluster-m8g.yaml
set -euo pipefail

REGION="${1:-us-east-1}"
ARCH="${2:-x86_64}"

case "$ARCH" in
  x86_64|amd64)  CANON_ARCH=amd64 ;;
  arm64|aarch64) CANON_ARCH=arm64 ;;
  *) echo "unknown arch '$ARCH' (use x86_64 or arm64)" >&2; exit 1 ;;
esac

PARAM="/aws/service/canonical/ubuntu/server/22.04/stable/current/${CANON_ARCH}/hvm/ebs-gp3/ami-id"

echo "region=${REGION} arch=${ARCH} param=${PARAM}" >&2
aws ssm get-parameter \
  --region "$REGION" \
  --name "$PARAM" \
  --query 'Parameter.Value' \
  --output text
