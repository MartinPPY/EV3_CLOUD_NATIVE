variable "project_name" {
  description = "Prefijo para los nombres de los recursos"
  type        = string
}

variable "vpc_id" {
  description = "ID de la VPC donde se crean los security groups"
  type        = string
}
