#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Render a gitignored cluster config from the tracked template (with the AMI
# auto-resolved + region/AZ filled from env), then `ray up` it.
#
# WHY: the tracked cluster-*.yaml files stay pristine templates, so editing
# per-environment values (ImageId, region) never happens in git-tracked files
# and `git pull` never conflicts. The rendered cluster-*.local.yaml is gitignored.
#
# Usage:
#   scripts/launch.sh m7i                 # resolve AMI, render, ray up
#   scripts/launch.sh m8g --no-restart    # extra args are passed through to ray up
#
# Env (optional):
#   BENCH_REGION (default us-east-1), BENCH_AZ (default ${REGION}a)
#
# After launch, use the RENDERED file for every other ray command, e.g.:
#   ray attach infra/ray-cluster/cluster-m7i.local.yaml
#   ray down   infra/ray-cluster/cluster-m7i.local.yaml
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/.."

ARCH_KEY="${1:?usage: scripts/launch.sh m7i|m8g [extra ray up args...]}"; shift || true
case "$ARCH_KEY" in
  m7i) AMI_ARCH=x86_64 ;;
  m8g) AMI_ARCH=arm64 ;;
  *) echo "first arg must be 'm7i' or 'm8g'" >&2; exit 1 ;;
esac

REGION="${BENCH_REGION:-us-east-1}"
AZ="${BENCH_AZ:-${REGION}a}"
TEMPLATE="infra/ray-cluster/cluster-${ARCH_KEY}.yaml"
RENDERED="infra/ray-cluster/cluster-${ARCH_KEY}.local.yaml"

[ -f "$TEMPLATE" ] || { echo "template not found: $TEMPLATE" >&2; exit 1; }

AMI="$(scripts/resolve_ami.sh "$REGION" "$AMI_ARCH")"
[ -n "$AMI" ] || { echo "failed to resolve AMI (check AWS creds / region)" >&2; exit 1; }
echo "resolved AMI: $AMI  (region=$REGION arch=$AMI_ARCH)"

# Substitute the AMI placeholder and region/AZ into the gitignored render.
sed -e "s|ami-REPLACE_X86_64|${AMI}|g" \
    -e "s|ami-REPLACE_ARM64|${AMI}|g" \
    -e "s|^  region: .*|  region: ${REGION}|" \
    -e "s|^  availability_zone: .*|  availability_zone: ${AZ}|" \
    "$TEMPLATE" > "$RENDERED"
echo "rendered -> $RENDERED"

echo "+ ray up $RENDERED $*"
ray up "$RENDERED" "$@"

echo "----------------------------------------------------------------------"
echo "cluster up. Use the rendered file for the rest:"
echo "  ray rsync-up $RENDERED ./ '~/ray-spark-on-graviton/'"
echo "  ray attach   $RENDERED"
echo "  ray down     $RENDERED"
echo "----------------------------------------------------------------------"
