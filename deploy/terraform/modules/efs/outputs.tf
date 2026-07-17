output "file_system_id" {
  description = "EFS filesystem id (fs-...). Feeds the ECS task-def efs_volume_configuration."
  value       = aws_efs_file_system.this.id
}

output "access_point_id" {
  description = "EFS access point id (fsap-...). The task mounts THIS (POSIX-squashed, rooted at the DB dir)."
  value       = aws_efs_access_point.db.id
}

output "security_group_id" {
  description = "EFS security group id (2049 from the app tier only)."
  value       = aws_security_group.efs.id
}
