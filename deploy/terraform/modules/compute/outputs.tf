output "alb_dns_name" {
  value = aws_lb.this.dns_name
}

output "cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "service_name" {
  value = aws_ecs_service.app.name
}

output "task_definition_family" {
  description = "Task-def family; reused (with a command override) by the one-time EFS DB-seed run-task."
  value       = aws_ecs_task_definition.app.family
}
