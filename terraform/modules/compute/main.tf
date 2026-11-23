locals {
  # app-zookeper y app-broker-0 usan sg-kafka; app usa sg-app; app-monitoring usa sg-monitoring.
  # app-zookeper y app-monitoring van en la subred pública (nginx + certbot);
  # app-broker-0 y app en subredes privadas.
  instances = {
    "app-zookeper"   = { subnet_id = var.public_subnet_id, public = true, user_data = var.nginx_user_data, sg_id = var.sg_kafka_id }
    "app-broker-0"   = { subnet_id = var.private_subnet_ids[0], public = false, user_data = var.user_data, sg_id = var.sg_kafka_id }
    "app"            = { subnet_id = var.private_subnet_ids[1], public = false, user_data = var.user_data, sg_id = var.sg_app_id }
    "app-monitoring" = { subnet_id = var.public_subnet_id, public = true, user_data = var.nginx_user_data, sg_id = var.sg_monitoring_id }
  }
}

# Las variables no pueden ir en depends_on; este recurso ancla el orden al NAT Gateway.
resource "terraform_data" "nat_ready" {
  input = var.nat_gateway_id
}

resource "aws_instance" "app" {
  for_each = local.instances

  ami                    = var.ami_id
  instance_type          = var.instance_type
  subnet_id              = each.value.subnet_id
  vpc_security_group_ids = [each.value.sg_id]
  iam_instance_profile   = var.instance_profile_name

  private_ip = lookup(var.private_ips, each.key, null)
  # IP pública para instancias en la subred pública (app-zookeper y app-monitoring): sin ella el user_data
  # no tendría internet (el NAT solo sirve a las subredes privadas). En ambas la EIP la reemplaza después.
  associate_public_ip_address = each.value.public
  user_data                   = each.value.user_data
  user_data_replace_on_change = true

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_size
    encrypted   = true
  }

  tags = {
    Name = "${var.project_name}-${each.key}"
  }

  depends_on = [terraform_data.nat_ready]
}

# La EIP se crea a mano (para que la IP y el DNS no cambien entre despliegues) y se re-asocia aquí.
resource "aws_eip_association" "zookeeper" {
  instance_id   = aws_instance.app["app-zookeper"].id
  allocation_id = var.eip_allocation_id
}

resource "aws_eip_association" "monitoring" {
  instance_id   = aws_instance.app["app-monitoring"].id
  allocation_id = var.eip_grafana_id
}
