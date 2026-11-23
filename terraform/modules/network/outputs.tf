output "vpc_id" {
  description = "ID de la VPC"
  value       = aws_vpc.this.id
}

output "public_subnet_id" {
  description = "ID de la subred pública (NAT Gateway)"
  value       = aws_subnet.public.id
}

output "private_subnet_ids" {
  description = "IDs de las subredes privadas, en orden"
  value       = aws_subnet.private[*].id
}

output "nat_gateway_id" {
  description = "ID del NAT Gateway"
  value       = aws_nat_gateway.this.id
}

output "nat_public_ip" {
  description = "IP pública (EIP) del NAT Gateway"
  value       = aws_eip.nat.public_ip
}
