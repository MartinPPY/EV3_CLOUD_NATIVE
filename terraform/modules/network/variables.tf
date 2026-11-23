variable "project_name" {
  description = "Prefijo para los nombres de los recursos"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR de la VPC"
  type        = string
}

variable "public_subnet_cidr" {
  description = "CIDR de la subred pública (aloja el NAT Gateway)"
  type        = string
}

variable "private_subnet_cidrs" {
  description = "CIDRs de las subredes privadas (una por AZ)"
  type        = list(string)
}

variable "availability_zones" {
  description = "AZs a usar; la primera aloja la subred pública y cada subred privada usa una"
  type        = list(string)
}
