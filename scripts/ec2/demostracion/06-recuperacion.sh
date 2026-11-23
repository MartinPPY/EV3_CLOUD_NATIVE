#!/usr/bin/env bash
# Demostración 6: recuperación ante la caída de un broker (kafka-2) con el consumer escalado a 2. Muestra, ANTES, DURANTE y
# DESPUÉS de la caída, el ISR de los tópicos y el estado del productor y de los consumidores (contenedores, último evento
# producido, miembros y lag del grupo, offsets de ventas-raw y ventas-agregadas).
#
# DÓNDE SE EJECUTA: dentro de una EC2 del laboratorio (cualquiera; no en la máquina local), p. ej. desde una sesión
# de Session Manager en act3-app, donde `node deploy-app.js` ya lo copia a /opt/ventas/demostracion. Usa AWS CLI v2 (lo instala
# el user_data) con las credenciales del instance profile de la EC2 (rol con SSM + ec2:DescribeInstances, como
# LabInstanceProfile): no hay que exportar credenciales ni abrir SSH, y no necesita permisos de docker en la EC2
# donde corre. Cada comando docker se envía por AWS SSM (Run Command) a la EC2 que corresponde, y el script imprime
# en cada paso el nodo donde se ejecuta:
#
#   [ec2-app]        act3-app            /opt/ventas   escalar consumer, miembros del grupo, offsets
#   [ec2-zookeper]   act3-app-zookeper   /opt/kafka    kafka-1 (+ ZooKeeper): describir tópicos, líderes e ISR
#   [ec2-broker]     act3-app-broker-0   /opt/kafka    kafka-2: aquí se provoca y se revierte la caída
#
# Flujo:
#   1 escalar consumer a 2       docker compose up -d --scale consumer=2 (espera 2 miembros en el grupo)
#   2 líderes / ISR / tópicos    esperado: cada partición con Isr de los 2 brokers y 0 under-replicated
#   3 offsets + estado de la app línea base: offsets de ventas-raw/ventas-agregadas, producer y 2 consumers running, último evento producido
#   4 caída del broker 2         docker compose stop kafka-2 (o, con --fallo daemon, systemctl stop docker)
#   5 líderes / ISR              esperado: Leader 1 en todas las particiones, Isr de un solo broker, under-replicated > 0
#   6 pipeline vivo              los offsets de ventas-raw siguen subiendo; producer y consumers siguen running, el producer sigue
#                                produciendo (último evento reciente) y el grupo conserva sus 2 miembros
#   7 recuperación               docker compose start kafka-2 (con --fallo daemon: systemctl start docker y la
#                                política restart: unless-stopped levanta sola el contenedor)
#   8 líderes / ISR              esperado: Isr de los 2 brokers otra vez y 0 under-replicated
#   9 pipeline tras recuperar    offsets siguen subiendo, producer/consumers running, grupo con 2 miembros
#
# Uso:  bash /opt/ventas/demostracion/06-recuperacion.sh [--fallo contenedor|daemon] [--espera SEG] [--no-recuperar]
#                                                        [--pausa] [--evidencia DIR]
#   --fallo      contenedor (por defecto): detiene solo kafka-2 | daemon: detiene el servicio docker de la EC2 del broker
#   --espera     segundos con el broker caído antes de medir que el pipeline sigue (20)
#   --no-recuperar  omite los pasos 7-9 (deja kafka-2 caído; el script imprime cómo recuperarlo)
#   --pausa      espera Enter entre pasos (para narrar en vivo)
#   --evidencia  guarda la salida de cada paso en DIR (NN-nombre.out.txt)
# Env:  AWS_REGION (por defecto la región de la EC2 vía IMDS, o us-east-1) | APP_INSTANCE_NAME (act3-app) | ZOOKEEPER_INSTANCE_NAME (act3-app-zookeper)
#       | BROKER_INSTANCE_NAME (act3-app-broker-0) | APP_DIR (/opt/ventas) | KAFKA_DIR (/opt/kafka)
#       | TOPICS ("ventas-raw ventas-agregadas ventas-raw-dlq") | RAW_TOPIC (ventas-raw)
#       | BROKER_FALLA_ID (2) | BROKER_SOBREVIVE_ID (1)
# Efectos: deja el consumer escalado a 2 réplicas (puertos 9407-9408) y, hasta la recuperación, kafka-2 detenido.

set -uo pipefail

# Región: la de la propia EC2 (IMDSv2) si no se indica AWS_REGION
if [ -z "${AWS_REGION:-}" ]; then
  imds_token="$(curl -s -m 2 -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null)"
  AWS_REGION="$(curl -s -m 2 -H "X-aws-ec2-metadata-token: $imds_token" http://169.254.169.254/latest/meta-data/placement/region 2>/dev/null)"
fi
REGION="${AWS_REGION:-us-east-1}"
APP_NAME="${APP_INSTANCE_NAME:-act3-app}"
ZK_NAME="${ZOOKEEPER_INSTANCE_NAME:-act3-app-zookeper}"
BROKER_NAME="${BROKER_INSTANCE_NAME:-act3-app-broker-0}"
APP_DIR="${APP_DIR:-/opt/ventas}"
KAFKA_DIR="${KAFKA_DIR:-/opt/kafka}"
TOPICS="${TOPICS:-ventas-raw ventas-agregadas ventas-raw-dlq}"
RAW_TOPIC="${RAW_TOPIC:-ventas-raw}"
AGG_TOPIC_NOMBRE="${AGG_TOPIC:-ventas-agregadas}"
BROKER_FALLA_ID="${BROKER_FALLA_ID:-2}"
BROKER_SOBREVIVE_ID="${BROKER_SOBREVIVE_ID:-1}"
BROKER_SERVICE="kafka-2"
SSM_TIMEOUT=600

FALLO=contenedor
ESPERA=20
RECUPERAR=1
PAUSA=0
EVIDENCIA=""
while [ $# -gt 0 ]; do
  case "$1" in
    --fallo) shift; FALLO="${1:?--fallo requiere contenedor|daemon}" ;;
    --espera) shift; ESPERA="${1:?--espera requiere segundos}" ;;
    --no-recuperar) RECUPERAR=0 ;;
    --pausa) PAUSA=1 ;;
    --evidencia) shift; EVIDENCIA="${1:?--evidencia requiere un directorio}" ;;
    -h|--help) sed -n '2,/^[^#]/{/^[^#]/!p}' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "opción desconocida: $1" >&2; exit 2 ;;
  esac
  shift
done
case "$FALLO" in contenedor|daemon) ;; *) echo "--fallo debe ser contenedor o daemon" >&2; exit 2 ;; esac
case "$ESPERA" in ''|*[!0-9]*) echo "--espera debe ser un número de segundos" >&2; exit 2 ;; esac

# ---- precondiciones --------------------------------------------------------------------------------
command -v aws >/dev/null 2>&1 || { echo "Falta AWS CLI v2 (comando aws)." >&2; exit 1; }
command -v base64 >/dev/null 2>&1 || { echo "Falta el comando base64." >&2; exit 1; }
aws sts get-caller-identity --region "$REGION" --query Account --output text >/dev/null 2>&1 \
  || { echo "Sin credenciales AWS: este script debe correr en una EC2 con instance profile (SSM + ec2:DescribeInstances)." >&2; exit 1; }

# El ID de la instancia cambia en cada despliegue: se busca por tag Name (igual que scripts/deploy-*.js)
find_id() {
  local id
  id="$(aws ec2 describe-instances --region "$REGION" \
        --filters "Name=tag:Name,Values=$1" "Name=instance-state-name,Values=running" \
        --query 'Reservations[].Instances[].InstanceId | [0]' --output text | tr -d '\r')"
  { [ -n "$id" ] && [ "$id" != None ]; } || { echo "No hay una instancia running con tag Name=$1" >&2; exit 1; }
  printf '%s' "$id"
}
ID_APP="$(find_id "$APP_NAME")" || exit 1
ID_ZK="$(find_id "$ZK_NAME")" || exit 1
ID_BROKER="$(find_id "$BROKER_NAME")" || exit 1

node_id() { case "$1" in app) echo "$ID_APP" ;; zk) echo "$ID_ZK" ;; broker) echo "$ID_BROKER" ;; esac; }
node_label() { case "$1" in app) echo "ec2-app:$APP_NAME" ;; zk) echo "ec2-zookeper:$ZK_NAME" ;; broker) echo "ec2-broker:$BROKER_NAME" ;; esac; }

# ---- ejecución remota vía SSM ----------------------------------------------------------------------
# Envía el script (en base64, para no pelear con comillas) a la EC2 con AWS-RunShellScript, espera el
# resultado e imprime stdout; el stderr remoto va a stderr. Devuelve 1 si el comando no termina en Success.
ssm_run() { # nodo (app|zk|broker), comando visible, script remoto
  local id b64 cid st t0 res
  id="$(node_id "$1")"
  printf '[%s | %s] $ %s\n' "$(node_label "$1")" "$id" "$2"
  b64="$(printf '%s\n' "$3" | base64 | tr -d '\n')"
  cid="$(aws ssm send-command --region "$REGION" --instance-ids "$id" --document-name AWS-RunShellScript \
        --comment "demo tolerancia a fallos" --timeout-seconds "$SSM_TIMEOUT" \
        --parameters "commands=echo $b64 | base64 -d | bash" --query Command.CommandId --output text | tr -d '\r')"
  [ -n "$cid" ] || return 1
  t0=$SECONDS
  while :; do
    # Justo tras el envío la invocación aún puede no existir: el error se ignora y se reintenta
    st="$(aws ssm get-command-invocation --region "$REGION" --command-id "$cid" --instance-id "$id" --query Status --output text 2>/dev/null | tr -d '\r')"
    case "$st" in Success|Failed|Cancelled|TimedOut) break ;; esac
    [ $((SECONDS - t0)) -gt "$SSM_TIMEOUT" ] && { echo "SSM: sin respuesta tras ${SSM_TIMEOUT}s" >&2; return 1; }
    sleep 3
  done
  res="$(aws ssm get-command-invocation --region "$REGION" --command-id "$cid" --instance-id "$id" --query StandardOutputContent --output text | tr -d '\r')"
  [ "$res" = None ] || printf '%s\n' "$res"
  res="$(aws ssm get-command-invocation --region "$REGION" --command-id "$cid" --instance-id "$id" --query StandardErrorContent --output text | tr -d '\r')"
  [ "$res" = None ] || printf '%s\n' "$res" >&2
  [ "$st" = Success ] || { echo "SSM: el comando terminó en estado $st" >&2; return 1; }
}

def() { IFS= read -r -d '' "$1" || true; }

# Funciones remotas. ev lee una variable del .env de la EC2 (sin `source`: hay contraseñas con símbolos).
def PRE_ZK <<'EOF'
export HOME=/root; cd "$KAFKA_DIR" || exit 1
ev() { grep -E "^$1=" .env | head -1 | cut -d= -f2- | tr -d '\r'; }
BS="$(ev PRIVATE_IP):$(ev KAFKA_PORT)"
kt() { docker compose exec -T kafka-1 kafka-topics --bootstrap-server "$BS" "$@" </dev/null; }
desc() { for t in $T; do kt --describe --topic "$t"; done; }
urp() { kt --describe --under-replicated-partitions | grep -c "Partition: "; }
EOF

def REPORTE_ZK <<'EOF'
echo "## brokers registrados en ZooKeeper"
docker compose exec -T zookeeper zookeeper-shell localhost:2181 ls /brokers/ids </dev/null 2>/dev/null | tail -1
echo "## tópicos"
kt --list | grep -v '^__'
echo "## líderes / réplicas / ISR"
desc
echo "UNDER_REPLICATED=$(urp)"
EOF

def PRE_APP <<'EOF'
export HOME=/root; cd "$APP_DIR" || exit 1
ev() { grep -E "^$1=" .env | head -1 | cut -d= -f2- | tr -d '\r'; }
BS="$(ev KAFKA_BOOTSTRAP)"; G="$(ev GROUP_ID)"; G="${G:-ventas-aggregator}"; V="$(ev CONFLUENT_VERSION)"
AGG="$(ev AGG_TOPIC)"; AGG="${AGG:-ventas-agregadas}"
kcli() { docker run --rm --network host "confluentinc/cp-kafka:${V:-7.5.3}" "$@" </dev/null 2>/dev/null; }
miembros() { kcli kafka-consumer-groups --bootstrap-server "$BS" --describe --group "$G" --members | grep -c "^$G "; }
EOF

def GRUPO_APP <<'EOF'
echo "## miembros del grupo $G"
kcli kafka-consumer-groups --bootstrap-server "$BS" --describe --group "$G" --members
echo "## lag del grupo"
kcli kafka-consumer-groups --bootstrap-server "$BS" --describe --group "$G" | cut -c1-140
echo "MIEMBROS=$(miembros)"
EOF

# Estado del productor y de los consumers en la EC2 de la app. La hora del último evento producido sale del log del
# producer: cada línea es la venta en JSON y su eventTime es el momento del envío.
def ESTADO_APP <<'EOF'
echo "## contenedores producer / consumer"
docker compose ps -a --format 'table {{.Service}}\t{{.Name}}\t{{.Status}}' | grep -E 'SERVICE|producer|consumer'
# -q lista un id por CONTENEDOR (--services lista un nombre por servicio, así que con 2 réplicas daría 1)
echo "PRODUCER_RUNNING=$(docker compose ps --status running -q producer | grep -c .)"
echo "CONSUMER_RUNNING=$(docker compose ps --status running -q consumer | grep -c .)"
echo "## últimas ventas producidas (log del producer)"
docker compose logs --tail 40 producer 2>&1 | grep -a '"eventTime"' | tail -2 | sed 's/^[^|]*| //' | cut -c1-200
ult="$(docker compose logs --tail 40 producer 2>&1 | grep -a -o '"eventTime":"[^"]*"' | tail -1 | cut -d'"' -f4)"
[ -n "$ult" ] && echo "PRODUCER_ULTIMO_EVENTO_HACE_S=$(( $(date +%s) - $(date -d "$ult" +%s) ))"
echo "STREAMS_ERROR=$(docker compose logs --since 10m consumer 2>&1 | grep -c STREAMS_ERROR)"
EOF

# Script para la EC2 de kafka-1/ZooKeeper. modo: ver | esperar-caida (hasta que el broker caído no sea líder
# ni esté en ISR) | esperar-isr (hasta 0 under-replicated)
zk_remoto() {
  local espera=""
  case "$1" in
    esperar-caida) espera='for i in $(seq 1 45); do [ "$(desc | grep -cE "Leader: $F([^0-9]|\$)|Isr: ([0-9]+,)*$F(,[0-9]+)*([^0-9,]|\$)")" = 0 ] && break; sleep 2; done' ;;
    esperar-isr) espera='for i in $(seq 1 60); do [ "$(urp)" = 0 ] && break; sleep 3; done' ;;
  esac
  printf '%s\n' "KAFKA_DIR='$KAFKA_DIR'; T='$TOPICS'; F=$BROKER_FALLA_ID" "$PRE_ZK" "$espera" "$REPORTE_ZK"
}
app_remoto() { # cuerpo
  printf '%s\n' "APP_DIR='$APP_DIR'; RAW_TOPIC='$RAW_TOPIC'" "$PRE_APP" "$1"
}
OFFSETS_CMD='echo "OFFSETS_RAW=$(kcli kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server "$BS" --topic "$RAW_TOPIC" --time -1 | awk -F: "{s+=\$3} END{print s+0}")"
echo "OFFSETS_AGG=$(kcli kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server "$BS" --topic "$AGG" --time -1 | awk -F: "{s+=\$3} END{print s+0}")"'
COMPOSE_PS="docker compose ps -a --format 'table {{.Name}}\t{{.Service}}\t{{.Status}}'"

# ---- helpers de salida y verificación --------------------------------------------------------------
STEP=0; FAILS=0; OUT=""; FALLO_ACTIVO=0
paso() { # nombre-corto, titulo; ejecuta paso_cuerpo y deja su salida en $OUT
  STEP=$((STEP + 1))
  local n; n="$(printf '%02d' "$STEP")"
  printf '\n\033[1;36m==== [%s] %s ====\033[0m\n' "$n" "$2"
  local rc=0
  OUT="$(paso_cuerpo 2>&1)" || rc=$?
  printf '%s\n' "$OUT"
  if [ "$rc" -ne 0 ]; then printf '\033[1;31m  FALLO \033[0m el paso terminó con error\n'; FAILS=$((FAILS + 1)); fi
  [ -n "$EVIDENCIA" ] && printf '%s\n' "$OUT" > "$EVIDENCIA/$n-$1.out.txt"
  if [ "$PAUSA" = 1 ]; then read -r -p "-- Enter para continuar -- " _ </dev/tty || true; fi
}
check() { # descripcion, esperado, obtenido
  if [ "$2" = "$3" ]; then printf '\033[1;32m  OK \033[0m %s (esperado %s)\n' "$1" "$2"
  else printf '\033[1;31m  FALLO \033[0m %s (esperado %s, obtenido %s)\n' "$1" "$2" "$3"; FAILS=$((FAILS + 1)); fi
}
check_cond() { # descripcion, detalle, comando de prueba...
  local desc="$1" detalle="$2"; shift 2
  if "$@"; then printf '\033[1;32m  OK \033[0m %s (%s)\n' "$desc" "$detalle"
  else printf '\033[1;31m  FALLO \033[0m %s (%s)\n' "$desc" "$detalle"; FAILS=$((FAILS + 1)); fi
}
valor() { printf '%s\n' "$OUT" | sed -n "s/^$1=//p" | tail -1 | tr -d '[:space:]'; }
solo_num() { local n="${1//[!0-9]/}"; printf "%s" "${n:-0}"; }
cuenta() { printf '%s\n' "$OUT" | grep 'Partition: ' | grep -cE "$1" || true; }
# Cuenta particiones del describe en $OUT: TOTAL, con ISR de 2 brokers, y con líder en el broker sobreviviente
analiza() {
  TOTAL="$(cuenta '.')"; ISR2="$(cuenta 'Isr: [0-9]+,[0-9]+')"
  LIDER_VIVO="$(cuenta "Leader: $BROKER_SOBREVIVE_ID([^0-9]|\$)")"; URP="$(valor UNDER_REPLICATED)"; URP="${URP:-?}"
  URPN="${URP//[!0-9]/}"; URPN="${URPN:-0}"
}

[ -n "$EVIDENCIA" ] && mkdir -p "$EVIDENCIA"
aviso_caida() {
  [ "$FALLO_ACTIVO" = 1 ] || return 0
  echo
  echo "AVISO: el script terminó con el broker caído. Recuperar:"
  if [ "$FALLO" = daemon ]; then echo "  [en $BROKER_NAME] sudo systemctl start docker"
  else echo "  [en $BROKER_NAME, $KAFKA_DIR] docker compose start $BROKER_SERVICE"; fi
}
trap aviso_caida EXIT

echo "región=$REGION | modo de fallo=$FALLO | topics: $TOPICS"
echo "nodos: ec2-app=$ID_APP ec2-zookeper=$ID_ZK ec2-broker=$ID_BROKER"

# ---- 1. escalar consumer a 2 -----------------------------------------------------------------------
paso_cuerpo() {
  ssm_run app "docker compose up -d --scale consumer=2  (y espera 2 miembros en el grupo)" "$(app_remoto '
docker compose up -d --scale consumer=2 2>&1 | tail -6
for i in $(seq 1 30); do [ "$(miembros)" = 2 ] && break; sleep 4; done
'"$COMPOSE_PS"'
'"$GRUPO_APP")"
}
paso escalar-consumer "Escalar el consumer a 2 réplicas"
check "miembros del grupo de consumo" 2 "$(valor MIEMBROS)"

# ---- 2. líderes / ISR / tópicos --------------------------------------------------------------------
paso_cuerpo() { ssm_run zk "docker compose exec -T kafka-1 kafka-topics --describe (tópicos: $TOPICS)" "$(zk_remoto ver)"; }
paso lideres-isr-inicial "Tópicos, líderes e ISR con el consumer escalado (todo sano)"
analiza
check_cond "hay particiones que evaluar" "$TOTAL" test "$TOTAL" -gt 0
check "particiones con ISR de los 2 brokers" "$TOTAL" "$ISR2"
check "particiones under-replicated" 0 "$URP"

# ---- 3. línea base de offsets ----------------------------------------------------------------------
paso_cuerpo() { ssm_run app "offsets de $RAW_TOPIC / agregadas, estado de producer y consumers" "$(app_remoto "$OFFSETS_CMD"$'\n'"$ESTADO_APP")"; }
paso offsets-base "Antes del fallo: offsets de $RAW_TOPIC y estado del productor y de los consumers"
OFF0="$(valor OFFSETS_RAW)"; OFF0="${OFF0:-0}"; OFFA0="$(valor OFFSETS_AGG)"; OFFA0="${OFFA0:-0}"
check "contenedor producer running" 1 "$(valor PRODUCER_RUNNING)"
check "contenedores consumer running" 2 "$(valor CONSUMER_RUNNING)"

# ---- 4. caída del broker 2 -------------------------------------------------------------------------
if [ "$FALLO" = daemon ]; then
  CMD_FALLO='systemctl stop docker.socket docker 2>/dev/null || systemctl stop docker; echo "docker: $(systemctl is-active docker)"'
  VISIBLE_FALLO="sudo systemctl stop docker  (cae el daemon y con él $BROKER_SERVICE)"
else
  CMD_FALLO="cd '$KAFKA_DIR' && docker compose stop $BROKER_SERVICE && $COMPOSE_PS"
  VISIBLE_FALLO="docker compose stop $BROKER_SERVICE"
fi
FALLO_ACTIVO=1
paso_cuerpo() { ssm_run broker "$VISIBLE_FALLO" "export HOME=/root; $CMD_FALLO"; }
paso caida-broker "Caída del broker $BROKER_FALLA_ID ($FALLO)"

# ---- 5. líderes / ISR tras la caída ----------------------------------------------------------------
paso_cuerpo() { ssm_run zk "kafka-topics --describe (espera el failover: sin líder ni ISR en el broker $BROKER_FALLA_ID)" "$(zk_remoto esperar-caida)"; }
paso lideres-isr-caida "Líderes e ISR con el broker $BROKER_FALLA_ID caído (failover a $BROKER_SOBREVIVE_ID)"
analiza
check_cond "hay particiones que evaluar" "$TOTAL" test "$TOTAL" -gt 0
check "particiones con líder en el broker $BROKER_SOBREVIVE_ID" "$TOTAL" "$LIDER_VIVO"
check "particiones con ISR de 2 brokers" 0 "$ISR2"
check_cond "particiones under-replicated > 0" "obtenido $URP" test "$URPN" -gt 0

# ---- 6. el pipeline sigue vivo ---------------------------------------------------------------------
paso_cuerpo() {
  echo "Esperando ${ESPERA}s con el broker caído para medir si el producer y el consumer siguen avanzando..."
  sleep "$ESPERA"
  ssm_run app "offsets, estado de producer/consumers y del grupo" "$(app_remoto "$OFFSETS_CMD"$'\n'"$ESTADO_APP"$'\n'"$GRUPO_APP")"
}
paso pipeline-vivo "Con el broker caído: el productor y los consumers no se detienen"
OFF1="$(valor OFFSETS_RAW)"; OFF1="${OFF1:-0}"; OFFA1="$(valor OFFSETS_AGG)"; OFFA1="${OFFA1:-0}"
echo ">> offsets $RAW_TOPIC: antes=$OFF0 durante la caída=$OFF1 | offsets $AGG_TOPIC_NOMBRE: antes=$OFFA0 durante=$OFFA1 (solo suben al cerrarse ventanas)"
check_cond "el producer siguió escribiendo en $RAW_TOPIC" "+$((OFF1 - OFF0)) registros" test "$OFF1" -gt "$OFF0"
check "contenedor producer running" 1 "$(valor PRODUCER_RUNNING)"
check "contenedores consumer running (sin reinicios forzados)" 2 "$(valor CONSUMER_RUNNING)"
check_cond "el producer sigue produciendo (último evento reciente)" "hace $(valor PRODUCER_ULTIMO_EVENTO_HACE_S) s, máx. 30" test "$(solo_num "$(valor PRODUCER_ULTIMO_EVENTO_HACE_S)")" -le 30 -a -n "$(valor PRODUCER_ULTIMO_EVENTO_HACE_S)"
check "[STREAMS_ERROR] de los consumers (últimos 10 min)" 0 "$(valor STREAMS_ERROR)"
check "miembros del grupo de consumo" 2 "$(valor MIEMBROS)"

if [ "$RECUPERAR" = 0 ]; then
  echo
  echo "--no-recuperar: se omite la recuperación (pasos 7-9)."
else
  # ---- 7. recuperación -----------------------------------------------------------------------------
  if [ "$FALLO" = daemon ]; then
    CMD_REC='systemctl start docker; for i in $(seq 1 40); do [ -n "$(docker ps -q --filter name=kafka-2 --filter status=running)" ] && break; sleep 3; done; docker ps --format "table {{.Names}}\t{{.Status}}"'
    VISIBLE_REC="sudo systemctl start docker  ($BROKER_SERVICE vuelve solo por restart: unless-stopped)"
  else
    CMD_REC="cd '$KAFKA_DIR' && docker compose start $BROKER_SERVICE && $COMPOSE_PS"
    VISIBLE_REC="docker compose start $BROKER_SERVICE"
  fi
  paso_cuerpo() { ssm_run broker "$VISIBLE_REC" "export HOME=/root; $CMD_REC"; }
  paso recuperacion "Recuperación del broker $BROKER_FALLA_ID"
  FALLO_ACTIVO=0

  # ---- 8. líderes / ISR tras recuperar -------------------------------------------------------------
  paso_cuerpo() { ssm_run zk "kafka-topics --describe (espera hasta 0 under-replicated)" "$(zk_remoto esperar-isr)"; }
  paso lideres-isr-recuperado "Líderes e ISR tras la recuperación"
  analiza
  check_cond "hay particiones que evaluar" "$TOTAL" test "$TOTAL" -gt 0
  check "particiones con ISR de los 2 brokers" "$TOTAL" "$ISR2"
  check "particiones under-replicated" 0 "$URP"

  # ---- 9. pipeline tras recuperar ------------------------------------------------------------------
  paso_cuerpo() {
    sleep 10
    ssm_run app "offsets, estado de producer/consumers y del grupo" "$(app_remoto "$OFFSETS_CMD"$'\n'"$ESTADO_APP"$'\n'"$GRUPO_APP")"
  }
  paso pipeline-recuperado "Tras la recuperación: el productor y los consumers siguen operando"
  OFF2="$(valor OFFSETS_RAW)"; OFF2="${OFF2:-0}"; OFFA2="$(valor OFFSETS_AGG)"; OFFA2="${OFFA2:-0}"
  echo ">> offsets $RAW_TOPIC: durante la caída=$OFF1 tras recuperar=$OFF2 | offsets $AGG_TOPIC_NOMBRE: durante=$OFFA1 tras recuperar=$OFFA2"
  check_cond "el producer sigue escribiendo en $RAW_TOPIC" "+$((OFF2 - OFF1)) registros" test "$OFF2" -gt "$OFF1"
  check "contenedor producer running" 1 "$(valor PRODUCER_RUNNING)"
  check "contenedores consumer running" 2 "$(valor CONSUMER_RUNNING)"
  check_cond "el producer sigue produciendo (último evento reciente)" "hace $(valor PRODUCER_ULTIMO_EVENTO_HACE_S) s, máx. 30" test "$(solo_num "$(valor PRODUCER_ULTIMO_EVENTO_HACE_S)")" -le 30 -a -n "$(valor PRODUCER_ULTIMO_EVENTO_HACE_S)"
  check "miembros del grupo de consumo" 2 "$(valor MIEMBROS)"
fi

echo
if [ "$FAILS" -eq 0 ]; then printf '\033[1;32mDemostración completa: todas las verificaciones pasaron.\033[0m\n'
else printf '\033[1;31m%d verificación(es) fallaron.\033[0m\n' "$FAILS"; fi
[ -n "$EVIDENCIA" ] && echo "Evidencia en: $EVIDENCIA"
[ "$FAILS" -eq 0 ]
