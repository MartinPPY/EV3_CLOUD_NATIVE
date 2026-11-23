output "vpc_id" {
  description = "ID de la VPC"
  value       = module.network.vpc_id
}

output "public_subnet_id" {
  description = "ID de la subred pública (NAT Gateway)"
  value       = module.network.public_subnet_id
}

output "private_subnet_ids" {
  description = "IDs de las subredes privadas"
  value       = module.network.private_subnet_ids
}

output "nat_gateway_id" {
  description = "ID del NAT Gateway"
  value       = module.network.nat_gateway_id
}

output "nat_public_ip" {
  description = "IP pública del NAT Gateway"
  value       = module.network.nat_public_ip
}

output "security_group_id" {
  description = "ID del security group sg-kafka"
  value       = module.security.sg_kafka_id
}

output "sg_app_id" {
  description = "ID del security group sg-app"
  value       = module.security.sg_app_id
}

output "sg_monitoring_id" {
  description = "ID del security group sg-monitoring"
  value       = module.security.sg_monitoring_id
}

output "instance_ids" {
  description = "IDs de las instancias EC2"
  value       = module.compute.instance_ids
}

output "instance_private_ips" {
  description = "IPs privadas de las instancias EC2"
  value       = module.compute.private_ips
}

output "zookeeper_instance_id" {
  description = "ID de app-zookeper (destino de scripts/configure-nginx.js)"
  value       = module.compute.instance_ids["app-zookeper"]
}

output "zookeeper_public_ip" {
  description = "Elastic IP de app-zookeper (debe coincidir con el registro A del DNS)"
  value       = module.compute.zookeeper_public_ip
}

output "kafka_ui_url" {
  description = "URL de Kafka UI (HTTPS tras correr scripts/configure-nginx.js; ajustar a tu dominio real)"
  value       = "https://kafka.ejemplo.com"
}

output "monitoring_public_ip" {
  description = "Elastic IP de app-monitoring (debe coincidir con el registro A del DNS de Grafana)"
  value       = module.compute.monitoring_public_ip
}

output "monitoring_url" {
  description = "URL de Grafana (HTTPS tras correr scripts/configure-nginx.js; el registro A debe apuntar a la EIP monitoring_public_ip)"
  value       = "https://grafana.ejemplo.com"
}

output "ssm_connect_commands" {
  description = "Comandos para abrir una sesión SSM en cada instancia"
  value = {
    for name, id in module.compute.instance_ids :
    name => "aws ssm start-session --target ${id} --region ${var.aws_region}"
  }
}
