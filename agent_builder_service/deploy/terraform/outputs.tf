output "data_bucket" {
  description = "S3 bucket for Parquet market data — aws s3 sync your parquet here."
  value       = aws_s3_bucket.data.bucket
}
output "data_uri" {
  description = "Set this as AGENT_DATA_URI (already wired into the task)."
  value       = "s3://${aws_s3_bucket.data.bucket}/${var.data_prefix}"
}
output "ecr_repo_url" {
  description = "Push the agent-builder image here."
  value       = aws_ecr_repository.builder.repository_url
}
output "ecs_service_name" {
  value = aws_ecs_service.svc.name
}
output "target_group_arn" {
  value = aws_lb_target_group.svc.arn
}
output "service_security_group_id" {
  description = "Allow NFS/2049 from this SG on your EFS mount-target SG so the wallet volume mounts."
  value       = aws_security_group.svc.id
}
output "api_base" {
  description = "The builder API is served at https://<your ALB domain>/api/builder/* via the listener rule."
  value       = "https://${data.terraform_remote_state.main.outputs.alb_dns_name}/api/builder"
}
