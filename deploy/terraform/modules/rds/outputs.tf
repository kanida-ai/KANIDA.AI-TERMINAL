output "endpoint" {
  value = aws_db_instance.this.endpoint
}

output "db_name" {
  value = aws_db_instance.this.db_name
}

output "master_password_secret_arn" {
  value = aws_secretsmanager_secret.master.arn
}
