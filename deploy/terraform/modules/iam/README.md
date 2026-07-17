# module: iam

**Purpose.** Two least-privilege roles for Fargate:
- **execution role** — ECS agent: ECR pull, CloudWatch Logs, and read+decrypt
  the specific secrets it injects into the container env.
- **task role** — the app's runtime identity: read the runtime secrets, R/W the
  artifacts S3 bucket, decrypt with the stack KMS key.

**SPOF / requirement addressed.** No static AWS keys in the app: identity is the
task role, scoped to exactly the ARNs this stack created (no resource wildcards).

**Phase-0 status.** Authored, unverified. Both roles reference resources created
by the secrets/s3 modules, so plan them together.
