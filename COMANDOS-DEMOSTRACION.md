## Comandos para demostrar en Kafka

**Crear topicos (en ec2-zookeper)**
en caso de que no se creen los topics por la app de java crearlos de esta
manera:

estos comandos se ejecutan en la ec2 que tiene zookeper!

`docker compose exec kafka-1 kafka-topics --bootstrap-server ip-ec2-zookeper:9092 --create --topic ventas-raw --partitions 4 --replication-factor 2`

`docker compose exec kafka-1 kafka-topics --bootstrap-server ip-ec2-zookeper:9092 --create --topic ventas-agregadas --partitions 2 --replication-factor 2`

`docker compose exec kafka-1 kafka-topics --bootstrap-server ip-ec2-zookeper:9092 --create --topic ventas-raw-dlq --partitions 4 --replication-factor 2`

**Verificar Lideres (Leads) ISR**

en la maquina de ec2 zookeper (deberia dar replicas: 1,2 e isr: 2,1)

`docker compose exec kafka-1 kafka-topics --bootstrap-server ip-ec2-zookeper:9092 --describe --topic ventas-raw`

**Ver logs de producer (en ec2-app)**

en la maquina ec2-app:

`docker compose logs -f producer`

**Ver agregados en vivo (desde ec2-zookeper)**

`docker compose exec kafka-1 kafka-console-consumer --bootstrap-server ip-ec2-zookeper:9092 --topic ventas-agregadas --property print.key=true`

**ver topics internos (desde ec2-zookeper)**

`docker compose exec -T kafka-1 kafka-topics --bootstrap-server ip-ec2-zookeper:9092 --list | grep 'ventas-aggregator'`

**Inspeccionar eventos invalidos a dlq (desde ec2-zookeper)**

el producer genera eventos invalidos (JSON ilegible o sin tienda) con probabilidad `INVALID_RATE` (en `app/.env`,
default 0.1) y se ven en su log con el prefijo `[INVALIDO]`. `--max-messages 10` espera hasta juntar 10 mensajes.

`docker compose exec kafka-1 kafka-console-consumer --bootstrap-server ip-ec2-zookeper:9092 --topic ventas-raw-dlq --from-beginning --max-messages 10 --property print.key=true`

**Escenario reproducible: inválidos, duplicado y tardíos (desde ec2-app, carpeta app/)**

Emite una secuencia fija e imprime el resultado esperado de cada paso (detalle en `app/README.md`).
Si se repite, esperar ~2 min entre corridas.

`docker compose --profile scenario run --rm scenario`

Un solo tipo de caso, sobre el stack en marcha (desde ec2-app, carpeta app/). Se ejecuta dentro del contenedor `consumer`;
`-e JAVA_TOOL_OPTIONS=` evita que la JVM nueva choque con el puerto 9407 del agente JMX. Funcionan en cualquier momento
(sin esperar 2 min, con el producer corriendo) y cada corrida usa `eventId` nuevos:

Uno de cada tema (1 inválido, 1 duplicado y 1 tardío: 3 registros en la DLQ con TOTAL_INCONSISTENTE, EVENT_ID_DUPLICADO y EVENTO_TARDIO):

`docker compose exec -T -e JAVA_TOOL_OPTIONS= consumer java -cp /app/app.jar cl.duoc.ventas.ScenarioProducer one-each`

Inválidos (5 a la DLQ: JSON_MALFORMADO, CAMPO_FALTANTE, CAMPO_INVALIDO, EVENT_TIME_INVALIDO, TOTAL_INCONSISTENTE):

`docker compose exec -T -e JAVA_TOOL_OPTIONS= consumer java -cp /app/app.jar cl.duoc.ventas.ScenarioProducer invalid`

Duplicado (1 a la DLQ: EVENT_ID_DUPLICADO):

`docker compose exec -T -e JAVA_TOOL_OPTIONS= consumer java -cp /app/app.jar cl.duoc.ventas.ScenarioProducer duplicate`

Tardío (uno dentro de la gracia se suma a su ventana; otro fuera de la gracia va a la DLQ como EVENTO_TARDIO):

`docker compose exec -T -e JAVA_TOOL_OPTIONS= consumer java -cp /app/app.jar cl.duoc.ventas.ScenarioProducer late`

Tardíos (fuera de la gracia, también van a la DLQ) y duplicados, por log (desde ec2-app):

`docker compose logs consumer | grep -E "TARDIO_DESCARTADO|DUPLICADO|DLQ"`

La DLQ trae el sobre con `sourceTopic`, `key`, `originalValue` y `reason`; esperados: JSON_MALFORMADO, CAMPO_FALTANTE,
CAMPO_INVALIDO, EVENT_TIME_INVALIDO, TOTAL_INCONSISTENTE, EVENT_ID_DUPLICADO y EVENTO_TARDIO (el tardío fuera de gracia, paso 13).
Agregados esperados: A W0 (3 ventas, 58000), A W1 (2, 16000), B W0 (1, 5000).

**Verificar flujo desde ec2 zookeper**

`docker compose exec kafka-1 kafka-console-consumer --bootstrap-server ip-ec2-zookeper:9092 --topic ventas-raw --from-beginning --max-messages 5`

**Ver ventas agregadas**

`docker compose exec kafka-1 kafka-console-consumer --bootstrap-server ip-ec2-zookeper:9092 --topic ventas-agregadas --from-beginning --max-messages 5`

**Ver consumer groups y lag**

`docker compose exec kafka-1 kafka-consumer-groups --bootstrap-server ip-ec2-zookeper:9092 --describe --group ventas-aggregator`

**Escalar consumer a 2**

`docker compose up -d --scale consumer=2`

Cada réplica publica sus métricas en un puerto del host distinto (9407 y 9408; máximo 2 réplicas, ver `ports` en `app/docker-compose.yml`).
Prometheus scrapea ambos (job `ventas-app-escala` para el 9408, DOWN mientras haya una sola réplica) y los `ventas_*_total` del dashboard suman las dos.
El 9408 requiere la regla `app_metrics_9408` de Terraform (`terraform apply` en `terraform/environments/<lab|prod>`) y `npm run deploy-monitoring` para recargar `prometheus.yml`.

### Probar fallo de docker

Configuración bajo prueba: 2 brokers, RF=2, `min.insync.replicas=1` (ver "Tolerancia a fallos y límites" en `README.md`).
Se tolera la caída de **un** broker; ZooKeeper (un solo nodo) es un **punto único de falla** (paso 4).

**Versión automatizada (desde cualquier EC2, p. ej. ec2-app, vía Session Manager; no desde la máquina local)**

`sudo bash /opt/ventas/demo-tolerancia-fallos.sh` (lo copia `npm run deploy-app`). Escala el consumer a 2, verifica tópicos/líderes/ISR,
detiene `kafka-2`, vuelve a verificar y recupera. Envía cada comando docker por AWS SSM a la EC2 correspondiente (el rol de la EC2
necesita SSM y `ec2:DescribeInstances`). Opciones: `--fallo daemon` (cae el servicio docker en vez del contenedor), `--no-recuperar`, `--espera`, `--pausa`, `--evidencia DIR`.

**1. Baseline (desde ec2-zookeper)**

`docker compose exec -T kafka-1 kafka-topics --bootstrap-server ip-ec2-zookeper:9092 --describe --topic ventas-raw`

Esperado: cada partición con `Replicas` de 2 brokers e `Isr` con los 2 (p. ej. `Replicas: 1,2  Isr: 1,2`).
En ec2-app dejar corriendo `docker compose logs -f producer` (siguen apareciendo ventas) y en otra terminal los agregados:
`docker compose exec kafka-1 kafka-console-consumer --bootstrap-server ip-ec2-zookeper:9092 --topic ventas-agregadas --property print.key=true`

**2. Caída de un broker: kafka-2 (en ec2-broker)**

`docker compose stop kafka-2`

Verificar ISR (desde ec2-zookeper):

`docker compose exec -T kafka-1 kafka-topics --bootstrap-server ip-ec2-zookeper:9092 --describe --topic ventas-raw | grep Isr`

Esperado: todas las particiones con `Leader: 1` e `Isr: 1`. El producer **no se detiene** (su log sigue avanzando), los agregados
siguen llegando y en Grafana el panel `UnderReplicatedPartitions` pasa a > 0. Luego recuperar:

`docker compose start kafka-2`

Esperado: tras unos segundos `Isr: 1,2` otra vez y `UnderReplicatedPartitions` vuelve a 0.

**3. Caída del otro broker: kafka-1 (en ec2-zookeper; ZooKeeper sigue vivo)**

`docker compose stop kafka-1`

Verificar desde ec2-broker (kafka-1 ya no responde, usar el broker 2):

`docker compose exec -T kafka-2 kafka-topics --bootstrap-server ip-ec2-broker:9092 --describe --topic ventas-raw | grep Isr`

Esperado: `Leader: 2` e `Isr: 2`; el producer y los agregados siguen. Recuperar con `docker compose start kafka-1`.

**4. Demostración del SPOF: ZooKeeper (en ec2-zookeper). Demo destructiva; recuperar al terminar.**

`docker compose stop zookeeper`

- Los brokers siguen sirviendo las particiones que ya lideran: el producer sigue enviando y se puede consumir.
- Sin ZooKeeper no hay elección de líder ni cambios de ISR. Con ZK aún caído, detener `kafka-2` (en ec2-broker):
  `docker compose stop kafka-2`. Las particiones que lideraba kafka-2 **no hacen failover** (el `describe` falla o muestra
  líder no disponible y el producer acumula timeouts), a diferencia del paso 2.
- Restaurar en este orden: `docker compose start zookeeper` (profile `zk`, en ec2-zookeper), esperar el healthcheck y luego
  `docker compose start kafka-2`. Verificar `Isr: 1,2`.

Nota: el comportamiento exacto con ZooKeeper caído (tiempos, mensajes de error) debe contrastarse en vivo y dejarse
registrado; lo documentado es el resultado esperado del diseño, no una medición.


## Monitoreo (Prometheus + Loki + Grafana + Alloy)

Orden de despliegue (desde `scripts/`, tras `terraform apply` en `terraform/environments/<lab|prod>`):

1. `npm run deploy-kafka` y `npm run deploy-app`: los compose cargan el JMX Prometheus javaagent
   (9404 brokers, 9405 ZooKeeper, 9406 producer, 9407 Kafka Streams).
2. `monitoring/monitoring.env` (copiar de `monitoring.env.example`; fuera de git) y `npm run deploy-monitoring`:
   levanta Prometheus/Loki/Grafana en app-monitoring y Alloy (logs -> Loki) en los 4 nodos.
3. El registro A de `GRAFANA_DOMAIN` debe apuntar a la EIP `eip-grafana` (`terraform output monitoring_public_ip` en el directorio del entorno; fija entre
   despliegues, Terraform la reasocia a app-monitoring) y luego correr `node configure-nginx.js` para el HTTPS.

**Verificar exporters (desde ec2-monitoring, vía SSM)**

`for t in 10.10.0.10:9404 10.10.2.10:9404 10.10.0.10:9405 10.10.3.10:9406 10.10.3.10:9407; do curl -s -o /dev/null -w "$t %{http_code}\n" http://$t/metrics; done`

**Verificar métricas de negocio (desde ec2-monitoring)**

`curl -s http://10.10.3.10:9407/metrics | grep ^ventas_`

Con 2 réplicas, repetir contra `:9408`: cada una cuenta solo lo que procesó; Grafana las suma.

Esperado: `ventas_procesadas_total`, `ventas_enviadas_dlq_total`, `ventas_invalidas_total`, `ventas_duplicadas_total` y
`ventas_tardias_total`. Tras el `scenario` deben subir duplicadas (+1), tardías (+1) e inválidas; `ventas_enviadas_dlq_total` suma las tres; se grafican en Grafana (fila "Negocio").

**Verificar targets de Prometheus (desde ec2-monitoring)**

`curl -s localhost:9090/api/v1/query --data-urlencode 'query=up' | head -c 600` (deben ser 5 con valor 1 + el de prometheus)

**Dashboard**: Grafana -> carpeta "Ventas Lab" -> "EA3 - Observabilidad integral Kafka y ventas". Usuario `admin`, contraseña en `monitoring/monitoring.env`.

Nota: los `docker compose exec kafka-1 kafka-topics ...` siguen funcionando; el agente JMX solo se carga en el proceso principal del contenedor.

