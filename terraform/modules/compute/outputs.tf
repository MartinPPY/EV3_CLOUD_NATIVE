output "instance_ids" {
  description = "IDs de las instancias, por nombre"
  value       = { for name, i in aws_instance.app : name => i.id }
}

output "zookeeper_public_ip" {
  description = "IP pública (EIP) de app-zookeper"
  value       = aws_eip_association.zookeeper.public_ip
}

output "monitoring_public_ip" {
  description = "IP pública (EIP) de app-monitoring"
  value       = aws_eip_association.monitoring.public_ip
}

output "private_ips" {
  description = "IPs privadas de las instancias, por nombre"
  value       = { for name, i in aws_instance.app : name => i.private_ip }
}
