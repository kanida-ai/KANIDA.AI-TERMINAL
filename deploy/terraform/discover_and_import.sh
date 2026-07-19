#!/usr/bin/env bash
# ============================================================================
# discover_and_import.sh — re-adopt the existing Kanida.AI prod AWS stack into
# fresh Terraform state via `terraform import` (NEVER create/destroy).
# ----------------------------------------------------------------------------
# CONTEXT: the Phase-2 local state was lost. All 68 resources still exist in
# AWS. This script discovers each resource's real AWS ID (by tag / name / the
# known IDs from the last apply) and imports it into the new S3-backed state.
#
# RUN THIS IN AWS CLOUDSHELL (ap-south-1), from the terraform root dir, AFTER
# `terraform init` has adopted the S3 backend (state is EMPTY). See
# IMPORT_RUNBOOK.md for the full operator sequence.
#
# SAFETY: this script ONLY runs `terraform import` + read-only `aws` describe/
# list calls. It NEVER runs apply/destroy. It is IDEMPOTENT — every address is
# skipped if already present in `terraform state list`, so re-running is safe.
#
# Enumerated: 67 AWS resources + random_password.master (state-only) = 68.
#   NOT created in the current config (so NOT imported here):
#     * module.compute.aws_lb_listener.https[0]  — count=0 (no ACM cert wired)
#     * module.egress.aws_instance.proxy[*]       — for_each over egress_users=[]
#     * module.egress.aws_eip.proxy[*]            — for_each over egress_users=[]
#   If you later set var.acm_certificate_arn or var.egress_users, those come in
#   as NEW resources on a normal apply — they are not part of this recovery.
# ============================================================================

set -uo pipefail   # NOT -e: one failed import must not abort the whole run.

# ── Fixed facts ──────────────────────────────────────────────────────────────
export AWS_DEFAULT_REGION="ap-south-1"
readonly REGION="ap-south-1"
readonly ACCOUNT="389642461326"
readonly NAME="kanida-prod"          # name_prefix-environment
readonly TF="terraform"

# ── Known resource IDs (from the last apply outputs) ─────────────────────────
readonly VPC_ID="vpc-052b68fbc99ce0b9d"
readonly EFS_FS_ID="fs-069b9eee8cd45b790"
readonly EFS_AP_ID="fsap-0423f1c86e1e4cf7c"
readonly ALB_ARN="arn:aws:elasticloadbalancing:ap-south-1:389642461326:loadbalancer/app/kanida-prod-alb/05ca8eb36719edfe"
readonly APP_SG_ID="sg-00a155bbb2c054ede"
# Known private subnets (order is verified against tag Name below, not assumed):
readonly KNOWN_PRIV_A="subnet-0d9bb03d5128f1f83"
readonly KNOWN_PRIV_B="subnet-0ef882670447608e9"

# ── Counters ─────────────────────────────────────────────────────────────────
IMPORTED=0 SKIPPED=0 FAILED=0
FAILED_ADDRS=()
TODO_ADDRS=()

# Cache the current state list once; refresh after each successful import.
STATE_LIST="$($TF state list 2>/dev/null || true)"

log()   { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
info()  { printf '    %s\n' "$*"; }
warn()  { printf '\033[1;33m    WARN: %s\033[0m\n' "$*"; }
err()   { printf '\033[1;31m    ERROR: %s\033[0m\n' "$*"; }

# in_state <address> — is the address already in terraform state?
in_state() {
  # exact-match a line in the cached state list
  printf '%s\n' "$STATE_LIST" | grep -qxF "$1"
}

# imp <address> <id> — idempotent single import.
imp() {
  local addr="$1" id="$2"
  if [[ -z "$id" || "$id" == "None" || "$id" == "null" ]]; then
    err "no AWS id discovered for '$addr' — SKIPPING as FAILED (see TODO)."
    FAILED=$((FAILED+1)); FAILED_ADDRS+=("$addr  (id not discovered)")
    return 1
  fi
  if in_state "$addr"; then
    info "SKIP (already in state): $addr"
    SKIPPED=$((SKIPPED+1))
    return 0
  fi
  info "IMPORT: $addr  <=  $id"
  if $TF import -no-color "$addr" "$id" >/tmp/tf_import.out 2>&1; then
    info "  ok."
    IMPORTED=$((IMPORTED+1))
    STATE_LIST="$STATE_LIST"$'\n'"$addr"    # keep cache current
    return 0
  else
    err "import FAILED for $addr — see detail below:"
    sed 's/^/      | /' /tmp/tf_import.out
    FAILED=$((FAILED+1)); FAILED_ADDRS+=("$addr  (id=$id)")
    return 1
  fi
}

# ── Discovery helpers (read-only aws calls) ──────────────────────────────────
# subnet id by Name tag within the VPC
subnet_by_name() {
  aws ec2 describe-subnets \
    --filters "Name=vpc-id,Values=$VPC_ID" "Name=tag:Name,Values=$1" \
    --query 'Subnets[0].SubnetId' --output text 2>/dev/null
}
# security group id by group-name within the VPC
sg_by_name() {
  aws ec2 describe-security-groups \
    --filters "Name=vpc-id,Values=$VPC_ID" "Name=group-name,Values=$1" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null
}
# route table id by Name tag within the VPC
rt_by_name() {
  aws ec2 describe-route-tables \
    --filters "Name=vpc-id,Values=$VPC_ID" "Name=tag:Name,Values=$1" \
    --query 'RouteTables[0].RouteTableId' --output text 2>/dev/null
}
# secretsmanager ARN by friendly name
secret_arn() {
  aws secretsmanager describe-secret --secret-id "$1" \
    --query 'ARN' --output text 2>/dev/null
}

log "Preflight: verifying terraform is initialized and AWS creds work"
if ! $TF providers >/dev/null 2>&1; then
  err "'$TF providers' failed. Run 'terraform init' first (adopts the S3 backend)."
  exit 1
fi
CALLER="$(aws sts get-caller-identity --query 'Account' --output text 2>/dev/null || true)"
if [[ "$CALLER" != "$ACCOUNT" ]]; then
  err "AWS account mismatch: got '$CALLER', expected '$ACCOUNT'. Fix creds/region and re-run."
  exit 1
fi
info "OK — account $ACCOUNT, region $REGION."

# ============================================================================
# 1) module.vpc  (14)
# ============================================================================
log "module.vpc"

imp 'module.vpc.aws_vpc.this' "$VPC_ID"

IGW_ID="$(aws ec2 describe-internet-gateways \
  --filters "Name=attachment.vpc-id,Values=$VPC_ID" \
  --query 'InternetGateways[0].InternetGatewayId' --output text 2>/dev/null)"
imp 'module.vpc.aws_internet_gateway.this' "$IGW_ID"

# Subnets — resolve by Name tag so the [count.index] alignment is exact.
#   public[i]  <- kanida-prod-public-<i>   (cidr 10.20.<i>.0/24, az ap-south-1<a/b>)
#   private[i] <- kanida-prod-private-<i>  (cidr 10.20.1<i>.0/24)
PUB0="$(subnet_by_name "${NAME}-public-0")";   imp 'module.vpc.aws_subnet.public[0]'  "$PUB0"
PUB1="$(subnet_by_name "${NAME}-public-1")";   imp 'module.vpc.aws_subnet.public[1]'  "$PUB1"
PRIV0="$(subnet_by_name "${NAME}-private-0")"; imp 'module.vpc.aws_subnet.private[0]' "$PRIV0"
PRIV1="$(subnet_by_name "${NAME}-private-1")"; imp 'module.vpc.aws_subnet.private[1]' "$PRIV1"
# Sanity: the two private subnets must be the known pair (order may differ).
if [[ -n "$PRIV0$PRIV1" ]]; then
  for s in "$PRIV0" "$PRIV1"; do
    case "$s" in "$KNOWN_PRIV_A"|"$KNOWN_PRIV_B") ;; *)
      warn "private subnet '$s' is not one of the known IDs ($KNOWN_PRIV_A/$KNOWN_PRIV_B). Verify tags." ;;
    esac
  done
fi

# NAT EIP (allocation id) by Name tag.
NAT_EIP_ALLOC="$(aws ec2 describe-addresses \
  --filters "Name=tag:Name,Values=${NAME}-nat-eip" \
  --query 'Addresses[0].AllocationId' --output text 2>/dev/null)"
imp 'module.vpc.aws_eip.nat' "$NAT_EIP_ALLOC"

NAT_GW_ID="$(aws ec2 describe-nat-gateways \
  --filter "Name=vpc-id,Values=$VPC_ID" "Name=state,Values=available,pending" \
  --query 'NatGateways[0].NatGatewayId' --output text 2>/dev/null)"
imp 'module.vpc.aws_nat_gateway.this' "$NAT_GW_ID"

PUB_RT="$(rt_by_name "${NAME}-public-rt")";   imp 'module.vpc.aws_route_table.public'  "$PUB_RT"
PRIV_RT="$(rt_by_name "${NAME}-private-rt")"; imp 'module.vpc.aws_route_table.private' "$PRIV_RT"

# Route table associations import as "<subnet-id>/<rtb-id>".
imp 'module.vpc.aws_route_table_association.public[0]'  "${PUB0}/${PUB_RT}"
imp 'module.vpc.aws_route_table_association.public[1]'  "${PUB1}/${PUB_RT}"
imp 'module.vpc.aws_route_table_association.private[0]' "${PRIV0}/${PRIV_RT}"
imp 'module.vpc.aws_route_table_association.private[1]' "${PRIV1}/${PRIV_RT}"

# ============================================================================
# 2) module.security  (5 security groups; all rules are INLINE, none standalone)
# ============================================================================
log "module.security"
imp 'module.security.aws_security_group.alb'          "$(sg_by_name "${NAME}-alb-sg")"
imp 'module.security.aws_security_group.app'          "$APP_SG_ID"   # known
imp 'module.security.aws_security_group.rds'          "$(sg_by_name "${NAME}-rds-sg")"
imp 'module.security.aws_security_group.redis'        "$(sg_by_name "${NAME}-redis-sg")"
imp 'module.security.aws_security_group.egress_proxy' "$(sg_by_name "${NAME}-egress-proxy-sg")"
# NOTE: no aws_security_group_rule / aws_vpc_security_group_*_rule resources exist
# (every ingress/egress is an inline block), so there is nothing else to import here.

# ============================================================================
# 3) module.secrets  (KMS key + alias + 19 env secrets = 21)
# ============================================================================
log "module.secrets"
# KMS key id via the alias target.
KMS_KEY_ID="$(aws kms list-aliases \
  --query "Aliases[?AliasName=='alias/${NAME}'].TargetKeyId | [0]" --output text 2>/dev/null)"
imp 'module.secrets.aws_kms_key.this'  "$KMS_KEY_ID"
imp 'module.secrets.aws_kms_alias.this' "alias/${NAME}"

# 19 env secrets — for_each key == the KEY name; import by ARN (name kanida-prod/env/<KEY>).
ENV_SECRET_KEYS=(
  FALCON_VAULT_KEY POWER_JWT_SECRET POWER_ADMIN_SECRET ADMIN_SECRET
  FALCON_OPERATOR_TOKEN ANTHROPIC_API_KEY KITE_API_KEY KITE_API_SECRET
  KITE_ACCESS_TOKEN ZERODHA_USERNAME ZERODHA_PASSWORD ZERODHA_TOTP_SECRET
  POLYGON_API_KEY VAPID_PUBLIC_KEY VAPID_PRIVATE_KEY VAPID_CONTACT
  BROKER_PROXY_MAP BROKER_EGRESS_POOL RUPEEZY_LIVE_CERTIFIED
)
for k in "${ENV_SECRET_KEYS[@]}"; do
  arn="$(secret_arn "${NAME}/env/${k}")"
  imp "module.secrets.aws_secretsmanager_secret.this[\"${k}\"]" "$arn"
done

# ============================================================================
# 4) module.s3  (bucket + 3 config sub-resources)
# ============================================================================
log "module.s3"
readonly S3_BUCKET="${NAME}-artifacts-${ACCOUNT}"   # kanida-prod-artifacts-389642461326
imp 'module.s3.aws_s3_bucket.this'                                        "$S3_BUCKET"
imp 'module.s3.aws_s3_bucket_public_access_block.this'                    "$S3_BUCKET"
imp 'module.s3.aws_s3_bucket_versioning.this'                             "$S3_BUCKET"
imp 'module.s3.aws_s3_bucket_server_side_encryption_configuration.this'   "$S3_BUCKET"

# ============================================================================
# 5) module.iam  (2 roles + 2 inline role policies + 1 managed attachment)
# ============================================================================
log "module.iam"
imp 'module.iam.aws_iam_role.execution' "${NAME}-ecs-execution"
imp 'module.iam.aws_iam_role.task'      "${NAME}-ecs-task"
# aws_iam_role_policy imports as "<role-name>:<policy-name>".
imp 'module.iam.aws_iam_role_policy.execution_secrets' "${NAME}-ecs-execution:${NAME}-execution-secrets"
imp 'module.iam.aws_iam_role_policy.task'              "${NAME}-ecs-task:${NAME}-task-policy"
# aws_iam_role_policy_attachment imports as "<role-name>/<policy-arn>".
imp 'module.iam.aws_iam_role_policy_attachment.execution_managed' \
    "${NAME}-ecs-execution/arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"

# ============================================================================
# 6) module.rds  (subnet group + master secret + version + db instance)
#    + random_password.master (state-only; see caveat)
# ============================================================================
log "module.rds"
imp 'module.rds.aws_db_subnet_group.this' "${NAME}-db-subnets"

RDS_SECRET_ARN="$(secret_arn "${NAME}/rds/master_password")"
imp 'module.rds.aws_secretsmanager_secret.master' "$RDS_SECRET_ARN"

# secret_version imports as "<secret-arn>|<version-id>" (current AWSCURRENT version).
RDS_SECRET_VID="$(aws secretsmanager get-secret-value \
  --secret-id "${NAME}/rds/master_password" \
  --query 'VersionId' --output text 2>/dev/null)"
if [[ -n "$RDS_SECRET_ARN" && -n "$RDS_SECRET_VID" && "$RDS_SECRET_VID" != "None" ]]; then
  imp 'module.rds.aws_secretsmanager_secret_version.master' "${RDS_SECRET_ARN}|${RDS_SECRET_VID}"
else
  err "could not resolve RDS master_password version id — flag as TODO."
  TODO_ADDRS+=("module.rds.aws_secretsmanager_secret_version.master  ->  terraform import 'module.rds.aws_secretsmanager_secret_version.master' '<secret-arn>|<version-id>'")
  FAILED=$((FAILED+1)); FAILED_ADDRS+=("module.rds.aws_secretsmanager_secret_version.master")
fi

# db instance imports by its identifier.
imp 'module.rds.aws_db_instance.this' "${NAME}-pg"

# ── random_password.master — CAVEAT (see IMPORT_RUNBOOK.md) ──────────────────
# This is a random-PROVIDER resource, not an AWS one. Its .result feeds BOTH the
# RDS master password AND the master_password secret_version. If it is NOT in
# state, `terraform plan` will GENERATE A NEW random value and try to update the
# secret_version + RDS password. We therefore import it with the REAL stored
# value (read from the secret). NOTE: importing random_password sets `result`
# but Terraform may still show this one resource wanting replacement if the
# provider cannot reconcile length/special/override_special from the value alone.
# If so, that diff is COSMETIC to AWS *only if* you prevent regeneration — STOP
# and see the runbook's "random_password" section before applying.
log "random_password.master (state-only, feeds RDS password + secret_version)"
if in_state 'random_password.master'; then
  info "SKIP (already in state): random_password.master"; SKIPPED=$((SKIPPED+1))
else
  RDS_PW="$(aws secretsmanager get-secret-value \
    --secret-id "${NAME}/rds/master_password" \
    --query 'SecretString' --output text 2>/dev/null)"
  if [[ -n "$RDS_PW" && "$RDS_PW" != "None" ]]; then
    if $TF import -no-color 'random_password.master' "$RDS_PW" >/tmp/tf_import.out 2>&1; then
      info "  imported random_password.master from the stored secret value."
      IMPORTED=$((IMPORTED+1)); STATE_LIST="$STATE_LIST"$'\n'random_password.master
      warn "run 'terraform plan' and confirm random_password.master does NOT show 'must be replaced'."
    else
      err "random_password.master import FAILED — flag as TODO (see runbook)."
      sed 's/^/      | /' /tmp/tf_import.out
      TODO_ADDRS+=("random_password.master  ->  terraform import 'random_password.master' '<value-from-${NAME}/rds/master_password>'")
      FAILED=$((FAILED+1)); FAILED_ADDRS+=("random_password.master")
    fi
  else
    err "could not read the RDS master password value — flag as TODO."
    TODO_ADDRS+=("random_password.master  ->  terraform import 'random_password.master' '<value-from-${NAME}/rds/master_password>'")
    FAILED=$((FAILED+1)); FAILED_ADDRS+=("random_password.master")
  fi
fi

# ============================================================================
# 7) module.redis  (subnet group + replication group)
# ============================================================================
log "module.redis"
imp 'module.redis.aws_elasticache_subnet_group.this'    "${NAME}-redis-subnets"
imp 'module.redis.aws_elasticache_replication_group.this' "${NAME}-redis"

# ============================================================================
# 8) module.efs  (filesystem + sg + 2 mount targets + access point)
# ============================================================================
log "module.efs"
imp 'module.efs.aws_efs_file_system.this' "$EFS_FS_ID"
imp 'module.efs.aws_security_group.efs'   "$(sg_by_name "${NAME}-efs-sg")"

# Mount targets — one per private subnet. Import by the fsmt-id that lives IN
# the matching subnet, so [count.index] stays aligned with private[i] above.
mt_in_subnet() {
  aws efs describe-mount-targets --file-system-id "$EFS_FS_ID" \
    --query "MountTargets[?SubnetId=='$1'].MountTargetId | [0]" --output text 2>/dev/null
}
imp 'module.efs.aws_efs_mount_target.this[0]' "$(mt_in_subnet "$PRIV0")"
imp 'module.efs.aws_efs_mount_target.this[1]' "$(mt_in_subnet "$PRIV1")"

imp 'module.efs.aws_efs_access_point.db' "$EFS_AP_ID"

# ============================================================================
# 9) module.compute  (cluster + log group + task def + alb + tg + http listener + service)
# ============================================================================
log "module.compute"
imp 'module.compute.aws_ecs_cluster.this'      "${NAME}-cluster"
imp 'module.compute.aws_cloudwatch_log_group.app' "/ecs/${NAME}-app"

# Task definition — import the CURRENT active revision ARN. See runbook: a plan
# will likely register a NEW revision (create, never destroy) because the config
# jsonencode/container_image will not byte-match AWS's stored revision. Set
# var.container_image to the REAL ECR URI in tfvars to minimize that churn.
TASKDEF_ARN="$(aws ecs describe-task-definition --task-definition "${NAME}-app" \
  --query 'taskDefinition.taskDefinitionArn' --output text 2>/dev/null)"
imp 'module.compute.aws_ecs_task_definition.app' "$TASKDEF_ARN"

imp 'module.compute.aws_lb.this' "$ALB_ARN"   # known

TG_ARN="$(aws elbv2 describe-target-groups --names "${NAME}-tg" \
  --query 'TargetGroups[0].TargetGroupArn' --output text 2>/dev/null)"
imp 'module.compute.aws_lb_target_group.app' "$TG_ARN"

# HTTP listener (port 80) on the ALB — import by its ARN.
HTTP_LISTENER_ARN="$(aws elbv2 describe-listeners --load-balancer-arn "$ALB_ARN" \
  --query "Listeners[?Port==\`80\`].ListenerArn | [0]" --output text 2>/dev/null)"
imp 'module.compute.aws_lb_listener.http' "$HTTP_LISTENER_ARN"

# ECS service imports as "<cluster-name>/<service-name>".
imp 'module.compute.aws_ecs_service.app' "${NAME}-cluster/${NAME}-svc"

# ── HTTPS listener (only if an ACM cert was wired: count=1) ───────────────────
# Not created by default (var.acm_certificate_arn = null → count = 0). If you DID
# apply with a cert, uncomment and import it:
HTTPS_LISTENER_ARN="$(aws elbv2 describe-listeners --load-balancer-arn "$ALB_ARN" \
  --query "Listeners[?Port==\`443\`].ListenerArn | [0]" --output text 2>/dev/null)"
if [[ -n "$HTTPS_LISTENER_ARN" && "$HTTPS_LISTENER_ARN" != "None" ]]; then
  warn "A :443 listener EXISTS in AWS ($HTTPS_LISTENER_ARN)."
  warn "It is only in Terraform if var.acm_certificate_arn is set (address module.compute.aws_lb_listener.https[0])."
  warn "If you use HTTPS, set that var, then run:"
  warn "  terraform import 'module.compute.aws_lb_listener.https[0]' '$HTTPS_LISTENER_ARN'"
  TODO_ADDRS+=("module.compute.aws_lb_listener.https[0]  ->  terraform import 'module.compute.aws_lb_listener.https[0]' '$HTTPS_LISTENER_ARN'  (ONLY if var.acm_certificate_arn is set)")
fi

# ============================================================================
# Summary
# ============================================================================
log "IMPORT SUMMARY"
printf '    Imported: %d\n' "$IMPORTED"
printf '    Skipped (already in state): %d\n' "$SKIPPED"
printf '    Failed:   %d\n' "$FAILED"
echo
info "Now in state: $($TF state list 2>/dev/null | wc -l) resources (target: 68)."

if (( ${#FAILED_ADDRS[@]} > 0 )); then
  log "FAILED / NEEDS ATTENTION"
  for a in "${FAILED_ADDRS[@]}"; do printf '    - %s\n' "$a"; done
fi
if (( ${#TODO_ADDRS[@]} > 0 )); then
  log "MANUAL TODO (ambiguous / conditional — run by hand after checking)"
  for a in "${TODO_ADDRS[@]}"; do printf '    - %s\n' "$a"; done
fi

log "NEXT STEP"
cat <<'EONEXT'
    Run:  terraform plan
    EXPECT: 0 to destroy. A near-zero change set. Benign, create-only or in-place
    diffs that are OK:
      * module.compute.aws_ecs_task_definition.app  -> new revision (create), if
        container_image / any container field differs. Set var.container_image to
        the REAL ECR URI in terraform.tfvars to avoid it.
      * module.compute.aws_ecs_service.app          -> in-place update to point at
        the new task-def revision.
      * tag-only / description-only in-place updates.
    STOP and investigate if the plan wants to DESTROY any existing resource, or
    to REPLACE random_password.master (would churn the RDS password) — that means
    an import was missed or mis-keyed. Cross-check against IMPORT_RUNBOOK.md.
EONEXT

# Exit non-zero if anything failed, so CI / the operator notices.
(( FAILED == 0 ))
