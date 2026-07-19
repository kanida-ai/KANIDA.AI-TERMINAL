# ============================================================================
# modules/iam — Fargate execution role + task role (least privilege)
# WHY:
#   * execution_role — used by the ECS agent to PULL the image, write logs, and
#     RESOLVE the Secrets Manager values it injects into the container env.
#   * task_role — the app's OWN runtime identity: read secrets it needs, R/W the
#     artifacts bucket, decrypt with the KMS key. Scoped to exactly the ARNs
#     this stack created — no wildcards on resources.
# ============================================================================

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# ── Execution role ──────────────────────────────────────────────────────────
resource "aws_iam_role" "execution" {
  name               = "${var.name}-ecs-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

# AWS-managed policy: ECR pull + CloudWatch Logs for the ECS agent.
resource "aws_iam_role_policy_attachment" "execution_managed" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Allow the agent to read the specific secrets it injects + decrypt them.
data "aws_iam_policy_document" "execution_secrets" {
  statement {
    sid       = "ReadInjectedSecrets"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = values(var.secret_arns)
  }
  statement {
    sid       = "DecryptSecrets"
    actions   = ["kms:Decrypt"]
    resources = [var.kms_key_arn]
  }
}

resource "aws_iam_role_policy" "execution_secrets" {
  name   = "${var.name}-execution-secrets"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution_secrets.json
}

# ── Task role (app runtime identity) ────────────────────────────────────────
resource "aws_iam_role" "task" {
  name               = "${var.name}-ecs-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

data "aws_iam_policy_document" "task" {
  statement {
    sid       = "ReadRuntimeSecrets"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = values(var.secret_arns)
  }
  statement {
    sid       = "KmsDecrypt"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [var.kms_key_arn]
  }
  statement {
    sid = "ArtifactsBucketRW"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket",
    ]
    resources = [
      var.artifacts_bucket_arn,
      "${var.artifacts_bucket_arn}/*",
    ]
  }

  # ── ON-DEMAND per-user egress provisioning (autotrade egress_provisioner.py) ─
  # The app allocates a dedicated Elastic IP + a tinyproxy EC2 per user's broker
  # account at runtime (SEBI one-IP-per-account rule), then tears it down. AWS
  # does not support resource-level scoping on Allocate/Associate/Disassociate/
  # Release Address or the Describe*/CreateTags read/tag calls, so those are
  # resource "*". The mutating instance calls (RunInstances/TerminateInstances)
  # ARE scoped by the `kanida:egress-user` tag so the app can only ever churn its
  # OWN egress boxes, never other EC2 in the account.
  statement {
    sid = "EgressEipAndDescribe"
    actions = [
      "ec2:AllocateAddress",
      "ec2:ReleaseAddress",
      "ec2:AssociateAddress",
      "ec2:DisassociateAddress",
      "ec2:DescribeAddresses",
      "ec2:DescribeInstances",
      "ec2:DescribeSubnets",
      "ec2:DescribeSecurityGroups",
      "ec2:CreateTags",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "EgressRunInstances"
    actions   = ["ec2:RunInstances"]
    resources = ["*"]
  }

  # Terminate ONLY instances tagged as this app's egress boxes.
  statement {
    sid       = "EgressTerminateTaggedOnly"
    actions   = ["ec2:TerminateInstances"]
    resources = ["*"]
    condition {
      test     = "StringLike"
      variable = "ec2:ResourceTag/kanida:egress-user"
      values   = ["*"]
    }
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "${var.name}-task-policy"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}
