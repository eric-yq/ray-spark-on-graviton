#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Idempotently ensure the IAM role + instance profile the benchmark cluster
# needs. Run ONCE on the control machine (ray-control), which already has AWS
# CLI credentials configured. Existing resources are left as-is (policy is
# refreshed); missing ones are created.
#
# The role grants:
#   - S3 read/write on your bucket (data + results)
#   - EC2 launch perms (the HEAD autoscaler launches the worker nodes)
#   - iam:PassRole (head passes this same role to the workers it launches)
#
# Usage:
#   BENCH_S3_BUCKET=my-bucket scripts/setup_iam.sh
#   scripts/setup_iam.sh my-bucket [role_name]
#
# Control-node creds must allow: iam:GetRole/CreateRole/PutRolePolicy,
#   iam:GetInstanceProfile/CreateInstanceProfile/AddRoleToInstanceProfile,
#   iam:PassRole, sts:GetCallerIdentity.
# ---------------------------------------------------------------------------
set -euo pipefail

BUCKET="${1:-${BENCH_S3_BUCKET:-}}"
ROLE="${2:-ray-bench-node}"
PROFILE="$ROLE"
POLICY_NAME="${ROLE}-policy"

if [ -z "$BUCKET" ]; then
  echo "usage: BENCH_S3_BUCKET=<bucket> $0   (or: $0 <bucket> [role_name])" >&2
  exit 1
fi

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
echo "account=$ACCOUNT_ID  role=$ROLE  profile=$PROFILE  bucket=$BUCKET"

TRUST="$(mktemp)"; PERMS="$(mktemp)"
trap 'rm -f "$TRUST" "$PERMS"' EXIT

cat > "$TRUST" <<'JSON'
{"Version":"2012-10-17","Statement":[
  {"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}
]}
JSON

cat > "$PERMS" <<JSON
{"Version":"2012-10-17","Statement":[
  {"Sid":"S3Data","Effect":"Allow",
   "Action":["s3:GetObject","s3:ListBucket","s3:PutObject"],
   "Resource":["arn:aws:s3:::${BUCKET}","arn:aws:s3:::${BUCKET}/*"]},
  {"Sid":"Ec2LaunchWorkers","Effect":"Allow",
   "Action":["ec2:RunInstances","ec2:TerminateInstances","ec2:CreateTags","ec2:Describe*"],
   "Resource":"*"},
  {"Sid":"PassNodeRole","Effect":"Allow",
   "Action":"iam:PassRole",
   "Resource":"arn:aws:iam::${ACCOUNT_ID}:role/${ROLE}"}
]}
JSON

# 1. role -------------------------------------------------------------------
if aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  echo "  role exists: $ROLE"
else
  echo "  creating role: $ROLE"
  aws iam create-role --role-name "$ROLE" \
    --assume-role-policy-document "file://$TRUST" \
    --description "Ray Spark on Graviton benchmark node role (auto-created)" >/dev/null
fi

# 2. inline permissions policy (always refreshed -> idempotent) -------------
aws iam put-role-policy --role-name "$ROLE" \
  --policy-name "$POLICY_NAME" --policy-document "file://$PERMS"
echo "  policy applied: $POLICY_NAME"

# 3. instance profile -------------------------------------------------------
PROFILE_CREATED=0
if aws iam get-instance-profile --instance-profile-name "$PROFILE" >/dev/null 2>&1; then
  echo "  instance profile exists: $PROFILE"
else
  echo "  creating instance profile: $PROFILE"
  aws iam create-instance-profile --instance-profile-name "$PROFILE" >/dev/null
  PROFILE_CREATED=1
fi

# 4. attach role to profile (a profile holds at most one role) --------------
ATTACHED="$(aws iam get-instance-profile --instance-profile-name "$PROFILE" \
  --query 'InstanceProfile.Roles[].RoleName' --output text 2>/dev/null || true)"
if printf '%s' "$ATTACHED" | grep -qw "$ROLE"; then
  echo "  role already attached to instance profile"
else
  echo "  attaching role to instance profile"
  aws iam add-role-to-instance-profile --instance-profile-name "$PROFILE" --role-name "$ROLE"
fi

if [ "$PROFILE_CREATED" -eq 1 ]; then
  echo "  newly created instance profile — waiting 15s for IAM->EC2 propagation"
  sleep 15
fi

echo "IAM ready: instance profile '$PROFILE' -> role '$ROLE'."
echo "If 'ray up' complains about an invalid instance profile, wait ~30s and retry."
