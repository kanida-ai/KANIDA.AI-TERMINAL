output "egress_ips_by_user" {
  description = "user handle -> dedicated static egress IP (register on the broker/SEBI profile)."
  value       = { for u in var.egress_users : u => aws_eip.proxy[u].public_ip }
}

output "proxy_instance_ids_by_user" {
  value = { for u in var.egress_users : u => aws_instance.proxy[u].id }
}
