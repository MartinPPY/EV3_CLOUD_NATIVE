output "sg_kafka_id" {
  description = "ID del security group sg-kafka"
  value       = aws_security_group.kafka.id
}

output "sg_app_id" {
  description = "ID del security group sg-app"
  value       = aws_security_group.app.id
}

output "sg_monitoring_id" {
  description = "ID del security group sg-monitoring"
  value       = aws_security_group.monitoring.id
}
