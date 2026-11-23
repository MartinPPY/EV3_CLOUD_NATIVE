# SG para las instancias de Kafka/ZooKeeper (app-zookeper y app-broker-0):
# se comunican por 9092 (broker) y 2181 (ZooKeeper), aceptan 9092 desde sg-app y métricas desde sg-monitoring.
resource "aws_security_group" "kafka" {
  name        = "${var.project_name}-sg-kafka"
  description = "Kafka y ZooKeeper entre instancias"
  vpc_id      = var.vpc_id

  tags = {
    Name = "${var.project_name}-sg-kafka"
  }
}

resource "aws_vpc_security_group_ingress_rule" "kafka_broker" {
  security_group_id            = aws_security_group.kafka.id
  referenced_security_group_id = aws_security_group.kafka.id
  ip_protocol                  = "tcp"
  from_port                    = 9092
  to_port                      = 9092
  description                  = "Kafka broker (9092) entre miembros del SG"
}

resource "aws_vpc_security_group_ingress_rule" "kafka_zookeeper" {
  security_group_id            = aws_security_group.kafka.id
  referenced_security_group_id = aws_security_group.kafka.id
  ip_protocol                  = "tcp"
  from_port                    = 2181
  to_port                      = 2181
  description                  = "ZooKeeper (2181) entre miembros del SG"
}

# Kafka UI y su front nginx/certbot en app-zookeper (el único con IP pública). El SG es compartido,
# pero las otras instancias están en subredes privadas y no son alcanzables desde internet.
resource "aws_vpc_security_group_ingress_rule" "kafka_ui" {
  security_group_id = aws_security_group.kafka.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 8080
  to_port           = 8080
  description       = "Kafka UI (8080)"
}

resource "aws_vpc_security_group_ingress_rule" "http" {
  security_group_id = aws_security_group.kafka.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
  description       = "HTTP (80): desafio HTTP-01 de certbot y redireccion a HTTPS"
}

resource "aws_vpc_security_group_ingress_rule" "https" {
  security_group_id = aws_security_group.kafka.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  description       = "HTTPS (443) hacia nginx"
}

# Egress abierto: necesario para NAT (apt, Docker) y para SSM (443).
resource "aws_vpc_security_group_egress_rule" "kafka_all" {
  security_group_id = aws_security_group.kafka.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Salida a internet via NAT"
}

# Acceso de la app Java (sg-app) a los brokers Kafka
resource "aws_vpc_security_group_ingress_rule" "kafka_from_app" {
  security_group_id            = aws_security_group.kafka.id
  referenced_security_group_id = aws_security_group.app.id
  ip_protocol                  = "tcp"
  from_port                    = 9092
  to_port                      = 9092
  description                  = "Kafka broker (9092) desde sg-app"
}

# Métricas de Kafka/ZooKeeper consumidas por sg-monitoring
resource "aws_vpc_security_group_ingress_rule" "kafka_metrics_9404" {
  security_group_id            = aws_security_group.kafka.id
  referenced_security_group_id = aws_security_group.monitoring.id
  ip_protocol                  = "tcp"
  from_port                    = 9404
  to_port                      = 9404
  description                  = "Metricas Kafka (9404) desde sg-monitoring"
}

resource "aws_vpc_security_group_ingress_rule" "kafka_metrics_9405" {
  security_group_id            = aws_security_group.kafka.id
  referenced_security_group_id = aws_security_group.monitoring.id
  ip_protocol                  = "tcp"
  from_port                    = 9405
  to_port                      = 9405
  description                  = "Metricas Kafka (9405) desde sg-monitoring"
}

resource "aws_vpc_security_group_ingress_rule" "kafka_node_exporter" {
  security_group_id            = aws_security_group.kafka.id
  referenced_security_group_id = aws_security_group.monitoring.id
  ip_protocol                  = "tcp"
  from_port                    = 9100
  to_port                      = 9100
  description                  = "node_exporter (9100) desde sg-monitoring"
}

# SG dedicado para la instancia app (productor y Kafka Streams)
resource "aws_security_group" "app" {
  name        = "${var.project_name}-sg-app"
  description = "Aplicacion de ventas y exposicion de metricas a monitoreo"
  vpc_id      = var.vpc_id

  tags = {
    Name = "${var.project_name}-sg-app"
  }
}

resource "aws_vpc_security_group_ingress_rule" "app_metrics_9406" {
  security_group_id            = aws_security_group.app.id
  referenced_security_group_id = aws_security_group.monitoring.id
  ip_protocol                  = "tcp"
  from_port                    = 9406
  to_port                      = 9406
  description                  = "Metricas app (9406) desde sg-monitoring"
}

resource "aws_vpc_security_group_ingress_rule" "app_metrics_9407" {
  security_group_id            = aws_security_group.app.id
  referenced_security_group_id = aws_security_group.monitoring.id
  ip_protocol                  = "tcp"
  from_port                    = 9407
  to_port                      = 9407
  description                  = "Metricas app (9407) desde sg-monitoring"
}

resource "aws_vpc_security_group_ingress_rule" "app_metrics_9408" {
  security_group_id            = aws_security_group.app.id
  referenced_security_group_id = aws_security_group.monitoring.id
  ip_protocol                  = "tcp"
  from_port                    = 9408
  to_port                      = 9408
  description                  = "Metricas app 2a replica Streams (9408) desde sg-monitoring"
}

resource "aws_vpc_security_group_ingress_rule" "app_node_exporter" {
  security_group_id            = aws_security_group.app.id
  referenced_security_group_id = aws_security_group.monitoring.id
  ip_protocol                  = "tcp"
  from_port                    = 9100
  to_port                      = 9100
  description                  = "node_exporter (9100) desde sg-monitoring"
}

resource "aws_vpc_security_group_egress_rule" "app_all" {
  security_group_id = aws_security_group.app.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Salida a brokers Kafka, NAT y SSM"
}

# SG dedicado para la instancia app-monitoring (HTTP 80 y HTTPS 443 públicos; Loki 3100 solo desde otros SG)
resource "aws_security_group" "monitoring" {
  name        = "${var.project_name}-sg-monitoring"
  description = "Monitoreo (HTTP 80, HTTPS 443 y Loki 3100 interno)"
  vpc_id      = var.vpc_id

  tags = {
    Name = "${var.project_name}-sg-monitoring"
  }
}

# Alloy (en kafka y app) empuja logs a Loki (3100); Grafana no se expone: nginx (80/443) la publica por HTTPS
resource "aws_vpc_security_group_ingress_rule" "monitoring_loki_from_kafka" {
  security_group_id            = aws_security_group.monitoring.id
  referenced_security_group_id = aws_security_group.kafka.id
  ip_protocol                  = "tcp"
  from_port                    = 3100
  to_port                      = 3100
  description                  = "Loki (3100) desde sg-kafka"
}

resource "aws_vpc_security_group_ingress_rule" "monitoring_loki_from_app" {
  security_group_id            = aws_security_group.monitoring.id
  referenced_security_group_id = aws_security_group.app.id
  ip_protocol                  = "tcp"
  from_port                    = 3100
  to_port                      = 3100
  description                  = "Loki (3100) desde sg-app"
}

resource "aws_vpc_security_group_ingress_rule" "monitoring_http" {
  security_group_id = aws_security_group.monitoring.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
  description       = "HTTP (80) hacia monitoreo"
}

resource "aws_vpc_security_group_ingress_rule" "monitoring_https" {
  security_group_id = aws_security_group.monitoring.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  description       = "HTTPS (443) hacia monitoreo"
}

resource "aws_vpc_security_group_egress_rule" "monitoring_all" {
  security_group_id = aws_security_group.monitoring.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Salida a internet (apt, Docker, SSM) y scraping de metricas"
}
