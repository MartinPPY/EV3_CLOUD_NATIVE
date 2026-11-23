terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Credenciales de Learner Lab (access key, secret y session token) vía variables de entorno.
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = merge({ Project = var.project_name, Environment = "lab" }, var.tags)
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# AWS Academy no permite crear roles IAM: se reutiliza el perfil existente del lab.
data "aws_iam_instance_profile" "lab" {
  name = var.instance_profile_name
}

# EIP creada a mano: se busca por su IP (tfvars) y por su nombre fijo; así se obtiene el allocation ID.
data "aws_eip" "cloud_native" {
  public_ip = var.eip_public_ip

  filter {
    name   = "tag:Name"
    values = ["eip-cloud-native"]
  }
}

# EIP de monitoring, creada a mano igual que la de Kafka (Name = eip-grafana): mantiene fija la IP del registro A de Grafana.
data "aws_eip" "grafana" {
  public_ip = var.eip_grafana_public_ip

  filter {
    name   = "tag:Name"
    values = ["eip-grafana"]
  }
}

module "network" {
  source = "../../modules/network"

  project_name         = var.project_name
  vpc_cidr             = var.vpc_cidr
  public_subnet_cidr   = var.public_subnet_cidr
  private_subnet_cidrs = var.private_subnet_cidrs
  availability_zones   = slice(data.aws_availability_zones.available.names, 0, 2)
}

module "security" {
  source = "../../modules/security"

  project_name = var.project_name
  vpc_id       = module.network.vpc_id
}

locals {
  base_user_data = templatefile("${path.root}/../../resources/user_data.sh.tftpl", { default_user = "ubuntu" })

  # app-zookeper y app-monitoring llevan nginx + certbot para servir Kafka UI y monitoreo por HTTPS
  nginx_user_data = "${local.base_user_data}\n${file("${path.root}/../../resources/nginx.sh.tftpl")}"

  # IPs fijas (host .10 de cada subred): los .env de ec2/ las referencian al desplegar Kafka a mano.
  # app-zookeper y app-monitoring viven en la subred pública (necesitan IP pública para la UI y Grafana).
  # app-monitoring (Loki) y app (métricas) también van fijas: monitoring/*.env y el dashboard las referencian.
  private_ips = {
    "app-zookeper"   = cidrhost(var.public_subnet_cidr, 10)
    "app-monitoring" = cidrhost(var.public_subnet_cidr, 20)
    "app-broker-0"   = cidrhost(var.private_subnet_cidrs[0], 10)
    "app"            = cidrhost(var.private_subnet_cidrs[1], 10)
  }
}

module "compute" {
  source = "../../modules/compute"

  project_name          = var.project_name
  private_subnet_ids    = module.network.private_subnet_ids
  public_subnet_id      = module.network.public_subnet_id
  eip_allocation_id     = data.aws_eip.cloud_native.id
  eip_grafana_id        = data.aws_eip.grafana.id
  sg_kafka_id           = module.security.sg_kafka_id
  sg_app_id             = module.security.sg_app_id
  sg_monitoring_id      = module.security.sg_monitoring_id
  nat_gateway_id        = module.network.nat_gateway_id
  ami_id                = data.aws_ami.ubuntu.id
  instance_type         = var.instance_type
  instance_profile_name = data.aws_iam_instance_profile.lab.name
  root_volume_size      = var.root_volume_size
  user_data             = local.base_user_data
  nginx_user_data       = local.nginx_user_data
  private_ips           = local.private_ips

  # Ninguna instancia arranca antes de que exista el NAT Gateway, la ruta 0.0.0.0/0 -> NAT y todas las reglas de SG.
  depends_on = [module.network, module.security]
}
