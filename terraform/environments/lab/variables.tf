variable "aws_region" {
  description = "Región de AWS (Learner Lab: us-east-1 o us-west-2)"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefijo para los nombres de los recursos"
  type        = string
  default     = "act3"
}

variable "vpc_cidr" {
  description = "CIDR de la VPC"
  type        = string
  default     = "10.10.0.0/22"
}

variable "public_subnet_cidr" {
  description = "CIDR de la subred pública (solo aloja el NAT Gateway)"
  type        = string
  default     = "10.10.0.0/24"
}

variable "private_subnet_cidrs" {
  description = "CIDRs de las 2 subredes privadas (una por AZ)"
  type        = list(string)
  default     = ["10.10.2.0/24", "10.10.3.0/24"]

  validation {
    condition     = length(var.private_subnet_cidrs) == 2
    error_message = "Se requieren exactamente 2 subredes privadas."
  }
}

variable "instance_type" {
  description = "Tipo de instancia EC2 (m3.medium está obsoleto y no existe en Learner Lab)"
  type        = string
  default     = "m5.large"
}

variable "instance_profile_name" {
  description = "Instance profile existente con permisos SSM; AWS Academy no permite crear roles IAM"
  type        = string
  default     = "LabInstanceProfile"
}

variable "root_volume_size" {
  description = "Tamaño del volumen raíz en GB"
  type        = number
  default     = 20
}

variable "eip_public_ip" {
  description = "Valor de la IP pública de la Elastic IP creada a mano (Name = eip-cloud-native); se asocia a app-zookeper en cada despliegue"
  type        = string
}

variable "eip_grafana_public_ip" {
  description = "Valor de la IP pública de la Elastic IP creada a mano (Name = eip-grafana); se asocia a app-monitoring en cada despliegue"
  type        = string
}

variable "tags" {
  description = "Tags adicionales aplicados a todos los recursos"
  type        = map(string)
  default     = {}
}
