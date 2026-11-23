variable "project_name" {
  description = "Prefijo para los nombres de los recursos"
  type        = string
}

variable "private_subnet_ids" {
  description = "IDs de las 2 subredes privadas (app-broker-0 en la 1.ª, app en la 2.ª)"
  type        = list(string)
}

variable "public_subnet_id" {
  description = "ID de la subred pública; app-zookeper vive aquí para exponer Kafka UI"
  type        = string
}

variable "eip_allocation_id" {
  description = "Allocation ID de la Elastic IP (creada a mano) que se asocia a app-zookeper"
  type        = string
}

variable "eip_grafana_id" {
  description = "Allocation ID de la Elastic IP (creada a mano) que se asocia a app-monitoring"
  type        = string
}

variable "sg_kafka_id" {
  description = "Security group sg-kafka compartido por app-zookeper y app-broker-0"
  type        = string
}

variable "sg_app_id" {
  description = "Security group sg-app asignado a la instancia app"
  type        = string
}

variable "sg_monitoring_id" {
  description = "Security group sg-monitoring asignado a la instancia app-monitoring"
  type        = string
}

variable "nat_gateway_id" {
  description = "ID del NAT Gateway; las instancias no se crean hasta que exista"
  type        = string
}

variable "ami_id" {
  description = "AMI Ubuntu a usar"
  type        = string
}

variable "instance_type" {
  description = "Tipo de instancia EC2"
  type        = string
}

variable "instance_profile_name" {
  description = "Instance profile con permisos SSM (LabInstanceProfile en AWS Academy)"
  type        = string
}

variable "root_volume_size" {
  description = "Tamaño del volumen raíz en GB"
  type        = number
}

variable "user_data" {
  description = "Script de arranque (instala Docker, Docker Compose, AWS CLI y asegura SSM Agent)"
  type        = string
}

variable "nginx_user_data" {
  description = "Script de arranque con nginx y certbot (base + nginx.sh.tftpl) para app-zookeper y app-monitoring"
  type        = string
}

variable "private_ips" {
  description = "IP privada fija por instancia (clave app-N); las ausentes la reciben por DHCP de la subred"
  type        = map(string)
  default     = {}
}
