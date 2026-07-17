output "kms_key_arn" {
  value = aws_kms_key.this.arn
}

output "secret_arns" {
  description = "Map of secret KEY NAME -> Secrets Manager ARN."
  value       = { for k, s in aws_secretsmanager_secret.this : k => s.arn }
}
