#!/usr/bin/env bash
# Demuestra la deduplicación por eventId y la distingue de la idempotencia del producer / EOS de Streams.
# Se ejecuta DENTRO de la EC2 de la app (ec2-app), donde corren el consumer y el producer; no usa SSM ni
# credenciales AWS. Para hablar con Kafka usa la imagen confluentinc/cp-kafka que ya está en esa EC2
# (la usa init-topics), apuntando a los brokers de KAFKA_BOOTSTRAP.
#
# Flujo:
#   1 línea base          métricas antes
#   2 duplicado           ScenarioProducer duplicate: el mismo JSON se envía dos veces
#   3 idempotencia        el producer es idempotente (acks=all, ProducerId asignado) y aun así envió ambos
#   4 ventas-raw          el mismo eventId aparece 2 veces (la idempotencia no lo toca)
#   5 ventas-raw-dlq      1 EVENT_ID_DUPLICADO
#   6 métricas y log      ventas_duplicadas_total +1
#   7 crash del consumer  kill -9 y reinicio; el store de dedup se restaura desde el changelog
#   8 reenvío             mismo eventId con eventTime actual (un reenvío exacto sería EVENTO_TARDIO tras ~90 s)
#   9 ventas-raw-dlq      sigue saliendo EVENT_ID_DUPLICADO: el estado sobrevivió al crash
#
# Cómo llega a la EC2: `node deploy-app.js` (desde scripts/) lo copia a /opt/ventas con permiso de ejecución,
# pero no lo ejecuta. Se corre a mano, en la EC2 de la app, por ejemplo desde Session Manager.
#
# Uso:  sudo bash /opt/ventas/demo-duplicados.sh [--sin-crash] [--pausa] [--evidencia DIR]
#   --sin-crash      omite los pasos 7-9 (no reinicia el consumer)
#   --pausa          espera Enter entre pasos (para narrar en vivo)
#   --evidencia DIR  guarda la salida de cada paso en DIR (NN-nombre.out.txt)
# Env:  APP_DIR (/opt/ventas, donde están docker-compose.yml y .env) | LOOKBACK (1500 registros por partición)
#
# Efectos: escribe eventos de prueba en ventas-raw y ventas-raw-dlq. Los pasos 7-9 reinician el consumer
# (el estado se restaura solo; los contadores en memoria de BusinessMetrics vuelven a 0).

set -uo pipefail

APP_DIR="${APP_DIR:-/opt/ventas}"
LOOKBACK="${LOOKBACK:-1500}"

CRASH=1
PAUSA=0
EVIDENCIA=""
while [ $# -gt 0 ]; do
  case "$1" in
    --sin-crash) CRASH=0 ;;
    --pausa) PAUSA=1 ;;
    --evidencia) shift; EVIDENCIA="${1:?--evidencia requiere un directorio}" ;;
    -h|--help) sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "opción desconocida: $1" >&2; exit 2 ;;
  esac
  shift
done

# ---- precondiciones --------------------------------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then echo "Este script corre dentro de la EC2 de la app: no encuentro docker." >&2; exit 1; fi
if ! docker info >/dev/null 2>&1; then
  command -v sudo >/dev/null 2>&1 && exec sudo -E bash "$0" "$@"
  echo "Sin permisos para docker; ejecuta con sudo." >&2; exit 1
fi
[ -f "$APP_DIR/docker-compose.yml" ] && [ -f "$APP_DIR/.env" ] || { echo "No encuentro docker-compose.yml y .env en $APP_DIR (ajusta APP_DIR)." >&2; exit 1; }
cd "$APP_DIR" || exit 1
export HOME="${HOME:-/root}"

env_get() { grep -E "^$1=" .env | head -1 | cut -d= -f2- | tr -d '\r'; }
BS="$(env_get KAFKA_BOOTSTRAP)"
CONFLUENT_VERSION="$(env_get CONFLUENT_VERSION)"
KAFKA_IMAGE="confluentinc/cp-kafka:${CONFLUENT_VERSION:-7.5.3}"
RAW_TOPIC="$(env_get RAW_TOPIC)"; RAW_TOPIC="${RAW_TOPIC:-ventas-raw}"
DLQ_TOPIC="$(env_get DLQ_TOPIC)"; DLQ_TOPIC="${DLQ_TOPIC:-ventas-raw-dlq}"
[ -n "$BS" ] || { echo "KAFKA_BOOTSTRAP vacío en $APP_DIR/.env." >&2; exit 1; }
[ -n "$(docker compose ps --status running --services 2>/dev/null | grep -x consumer)" ] || { echo "El servicio consumer no está corriendo en $APP_DIR." >&2; exit 1; }

[ -n "$EVIDENCIA" ] && mkdir -p "$EVIDENCIA"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# ---- helpers ---------------------------------------------------------------------------------------
# CLI de Kafka dentro de un contenedor efímero (red del host). `timeout` va dentro del contenedor: así
# no queda un consumer huérfano si se corta. -i solo hace falta para el producer (lee de stdin).
kcli() { docker run --rm -i --network host "$KAFKA_IMAGE" "$@"; }

# Lee de cada partición solo los últimos $LOOKBACK registros (el producer sigue escribiendo, así que
# --from-beginning --timeout-ms nunca termina) y filtra por patrón.
read_topic() { # topic, patron
  kcli kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server "$BS" --topic "$1" --time -1 </dev/null 2>/dev/null | tr -d '\r' |
  while IFS=: read -r _ p e; do
    s=$((e - LOOKBACK)); [ "$s" -lt 0 ] && s=0
    kcli timeout 15 kafka-console-consumer --bootstrap-server "$BS" --topic "$1" --partition "$p" --offset "$s" \
      --property print.partition=true --property print.offset=true --property print.key=true </dev/null 2>/dev/null | grep -a "$2"
  done
}
dlq_sin_original() { sed -E 's/"originalValue":"([^"\\]|\\.)*",//'; }

metrics() {
  local out
  out="$(curl -s --max-time 5 localhost:9407/metrics | grep -E "^ventas_(procesadas|duplicadas|tardias|invalidas|enviadas_dlq)_total")"
  if [ -n "$out" ]; then printf '%s\n' "$out"; else echo "(el exporter no responde en :9407)"; fi
}
wait_running() { # desde-timestamp (solo cuenta logs posteriores, no un RUNNING de una corrida anterior)
  local i
  for i in $(seq 1 40); do
    [ -n "$(docker compose logs --since "$1" consumer 2>&1 | grep "State transition from REBALANCING to RUNNING")" ] && return 0
    sleep 3
  done
  return 1
}
count() { printf '%s\n' "$1" | grep -c "$2" || true; }

STEP=0; FAILS=0; OUT=""
paso() { # nombre-corto, titulo; ejecuta la función `paso_cuerpo` y deja la salida en $OUT
  STEP=$((STEP + 1))
  local n; n="$(printf '%02d' "$STEP")"
  printf '\n\033[1;36m==== [%s] %s ====\033[0m\n' "$n" "$2"
  OUT="$(paso_cuerpo 2>&1)"
  printf '%s\n' "$OUT"
  [ -n "$EVIDENCIA" ] && printf '%s\n' "$OUT" > "$EVIDENCIA/$n-$1.out.txt"
  if [ "$PAUSA" = 1 ]; then read -r -p "-- Enter para continuar -- " _ </dev/tty || true; fi
}
check() { # descripcion, esperado, obtenido
  if [ "$2" = "$3" ]; then printf '\033[1;32m  OK \033[0m %s (esperado %s)\n' "$1" "$2"
  else printf '\033[1;31m  FALLO \033[0m %s (esperado %s, obtenido %s)\n' "$1" "$2" "$3"; FAILS=$((FAILS + 1)); fi
}

echo "app=$APP_DIR | bootstrap=$BS | imagen CLI=$KAFKA_IMAGE | topics: $RAW_TOPIC / $DLQ_TOPIC"

# ---- 1. línea base ---------------------------------------------------------------------------------
paso_cuerpo() {
  docker compose ps --format "table {{.Name}}\t{{.Service}}\t{{.Status}}"
  grep -E "^(DEDUP_RETENTION_SECONDS|WINDOW_SECONDS|WINDOW_GRACE_SECONDS)=" .env
  metrics
}
paso baseline "Línea base: contenedores, configuración de dedup y métricas"

# ---- 2. duplicado de negocio -----------------------------------------------------------------------
paso_cuerpo() {
  docker compose exec -T -e JAVA_TOOL_OPTIONS= consumer java -cp /app/app.jar cl.duoc.ventas.ScenarioProducer duplicate 2>"$TMP/producer.err"
}
paso escenario-duplicate "ScenarioProducer duplicate: el mismo evento se envía dos veces"
RUN="$(printf '%s\n' "$OUT" | grep -o '"eventId":"[^"]*-d1"' | head -1 | cut -d'"' -f4)"
[ -n "$RUN" ] || { echo "No pude obtener el eventId del escenario; abortando." >&2; exit 1; }
echo ">> eventId de la prueba: $RUN"

# ---- 3. idempotencia del producer ------------------------------------------------------------------
paso_cuerpo() {
  grep -E "enable.idempotence|acks =|Instantiated an idempotent producer|ProducerId set to" "$TMP/producer.err" | sed 's/^\[[^]]*\] //;s/^[ \t]*//'
  echo "-> con idempotencia activa el broker descarta reintentos de UN mismo envío; aquí hubo dos send() distintos."
}
paso idempotencia-producer "El producer del escenario es idempotente (y aun así envió los dos)"

# ---- 4. ventas-raw ---------------------------------------------------------------------------------
paso_cuerpo() { read_topic "$RAW_TOPIC" "$RUN"; }
paso ventas-raw-dos-registros "$RAW_TOPIC: el mismo eventId aparece 2 veces (la idempotencia no lo elimina)"
check "registros con ese eventId en $RAW_TOPIC" 2 "$(count "$OUT" "$RUN")"
RAW_P="$(printf '%s\n' "$OUT" | grep -o 'Partition:[0-9]*' | head -1 | cut -d: -f2)"
RAW_OFFS="$(printf '%s\n' "$OUT" | grep -o 'Offset:[0-9]*' | cut -d: -f2 | sort -n)"
RAW_OFF1="$(printf '%s\n' "$RAW_OFFS" | head -1)"
RAW_OFF2="$(printf '%s\n' "$RAW_OFFS" | tail -1)"

# ---- 5. DLQ ----------------------------------------------------------------------------------------
paso_cuerpo() { read_topic "$DLQ_TOPIC" "$RUN" | dlq_sin_original; }
paso dlq-event-id-duplicado "$DLQ_TOPIC: 1 EVENT_ID_DUPLICADO"
check "EVENT_ID_DUPLICADO en la DLQ" 1 "$(count "$OUT" EVENT_ID_DUPLICADO)"

# ---- 6. métricas y log -----------------------------------------------------------------------------
paso_cuerpo() {
  metrics
  echo "-- log del consumer --"
  docker compose logs consumer 2>&1 | grep "\[DUPLICADO\]" | grep "$RUN" | sed 's/^[^|]*| //;s/^\[[^]]*\] //'
}
paso metricas-despues "Métricas y log del consumer (ventas_duplicadas_total debe subir en 1)"

if [ -n "$RAW_P" ] && [ -n "$RAW_OFF1" ]; then
  cat <<EOF

(Opcional, en la EC2 de ZooKeeper/broker: el log del broker guarda un producerId y una secuencia por envío.)
  docker compose exec -T kafka-1 bash -c 'for f in /var/lib/kafka/data/$RAW_TOPIC-$RAW_P/*.log; do kafka-dump-log --files \$f | grep -E "baseOffset: ($RAW_OFF1|$RAW_OFF2) "; done'
  Esperado: mismo producerId y baseSequence distintos para los offsets $RAW_OFF1 y $RAW_OFF2.
EOF
fi

# ---- 7-9. crash, reenvío y verificación ------------------------------------------------------------
if [ "$CRASH" = 1 ] && [ -z "$RAW_P" ]; then
  echo "No pude ubicar los registros en $RAW_TOPIC; se omiten los pasos 7-9."
  CRASH=0
  FAILS=$((FAILS + 1))
fi
if [ "$CRASH" = 1 ]; then
  paso_cuerpo() {
    local desde; desde="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    docker compose kill consumer
    # Tras el kill el contenedor suele tomar :9408 (rango 9407-9408 del compose); se corrige al final del script
    docker compose up -d consumer 2>&1 | tail -3
    wait_running "$desde" || echo "(el consumer no llegó a RUNNING en 2 min)"
    docker compose logs --since "$desde" consumer 2>&1 | grep -E "Finished restoring changelog .*eventids-vistos|State transition from REBALANCING to RUNNING" | sed 's/^[^|]*| //;s/^\[[^]]*\] //' | cut -c1-230
    docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
  }
  paso crash-y-reinicio-consumer "Crash del consumer (kill -9) y reinicio: el store de dedup se restaura desde el changelog"

  paso_cuerpo() {
    local orig key val now val2 tab
    tab="$(printf '\t')"
    orig="$(kcli timeout 15 kafka-console-consumer --bootstrap-server "$BS" --topic "$RAW_TOPIC" --partition "$RAW_P" --offset "$RAW_OFF1" \
            --max-messages 1 --property print.key=true </dev/null 2>/dev/null | tr -d '\r' | head -1)"
    key="${orig%%"$tab"*}"
    val="${orig#*"$tab"}"
    now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    val2="$(printf '%s' "$val" | sed -E "s/\"eventTime\":\"[^\"]*\"/\"eventTime\":\"$now\"/")"
    echo "key      : $key"
    echo "original : $val"
    echo "reenvio  : $val2"
    printf '%s|%s\n' "$key" "$val2" | kcli kafka-console-producer --bootstrap-server "$BS" --topic "$RAW_TOPIC" \
      --property parse.key=true --property key.separator='|' 2>&1 | grep -v '^>'
    echo "(enviado)"
  }
  paso reenvio-restamped "Reenvío del mismo eventId con eventTime actual (tras el reinicio)"

  DUPS=0
  for intento in 1 2 3 4 5; do
    sleep 6
    paso_cuerpo() { read_topic "$DLQ_TOPIC" "$RUN" | dlq_sin_original; }
    paso dlq-tras-reinicio "$DLQ_TOPIC tras el reinicio: debe haber 2 EVENT_ID_DUPLICADO (intento $intento)"
    DUPS="$(count "$OUT" EVENT_ID_DUPLICADO)"
    [ "$DUPS" -ge 2 ] && break
  done
  check "EVENT_ID_DUPLICADO acumulados (antes + después del crash)" 2 "$DUPS"

  paso_cuerpo() {
    docker compose logs consumer 2>&1 | grep "\[DUPLICADO\]" | grep "$RUN" | sed 's/^[^|]*| //;s/^\[[^]]*\] //'
    docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
    metrics
  }
  paso consumer-tras-reinicio "Consumer reiniciado: log de dedup, métricas y puerto (los contadores arrancan en 0)"
  if printf '%s\n' "$OUT" | grep -q "el exporter no responde"; then
    echo ">> El contenedor tomó otro puerto del rango; se recrea para volver a :9407."
    paso_cuerpo() {
      local desde; desde="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      docker compose up -d --force-recreate --no-deps consumer 2>&1 | tail -3
      wait_running "$desde" || true
      sleep 5
      docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
    }
    paso restaurar-puerto "Restaurar el puerto del exporter (recrear el consumer)"
  fi
fi

echo
if [ "$FAILS" -eq 0 ]; then printf '\033[1;32mDemostración completa: todas las verificaciones pasaron.\033[0m\n'
else printf '\033[1;31m%d verificación(es) fallaron.\033[0m\n' "$FAILS"; fi
[ -n "$EVIDENCIA" ] && echo "Evidencia en: $EVIDENCIA"
exit "$FAILS"
