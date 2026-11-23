#!/usr/bin/env bash
# Funciones compartidas por las demostraciones 01-05 y 07 (no se ejecuta solo). Cada demo hace:
#   source "$(dirname "$(readlink -f "$0")")/lib.sh"
#   demo_init "$@"
#
# Corre DENTRO de la EC2 de la app (act3-app, Ubuntu, /opt/ventas): solo usa bash, docker, curl, awk/sed/grep y
# GNU date. El CLI de Kafka va en un contenedor efímero de confluentinc/cp-kafka (la imagen de init-topics) con
# --network host contra KAFKA_BOOTSTRAP; no hace falta SSM ni credenciales AWS.
#
# Convenciones: las demos usan las tiendas reales del producer (STORE_A, STORE_B). Las que miden agregados pausan el producer
# (ver pausar_producer) y usan una ventana futura sin ventas en vivo; para que esa ventana emita su agregado hay que adelantar
# el stream time de su partición (`cerrar_ventana`): ver el aviso en cada cabecera.

APP_DIR="${APP_DIR:-/opt/ventas}"
STATE_FILE="${STATE_FILE:-/tmp/ventas-demo-stream-time}"
DEMO_SCRIPT="$(readlink -f "${BASH_SOURCE[1]}")"
PAUSA=0
EVIDENCIA=""
STEP=0; FAILS=0; OUT=""

# ---- argumentos y arranque -------------------------------------------------------------------------
demo_ayuda() { sed -n '2,/^[^#]/{/^[^#]/!p}' "$DEMO_SCRIPT" | sed 's/^# \{0,1\}//'; }

# Opciones comunes: --pausa, --evidencia DIR, -h. Una demo con opciones propias define demo_extra_arg, que recibe
# "$@" desde la opción, devuelve 0 si la reconoce y deja en EXTRA_N cuántos argumentos consumió (1 por defecto).
demo_parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --pausa) PAUSA=1 ;;
      --evidencia) shift; EVIDENCIA="${1:?--evidencia requiere un directorio}" ;;
      -h|--help) demo_ayuda; exit 0 ;;
      *)
        EXTRA_N=1
        if declare -F demo_extra_arg >/dev/null && demo_extra_arg "$@"; then shift $((EXTRA_N - 1))
        else echo "opción desconocida: $1" >&2; exit 2; fi ;;
    esac
    shift
  done
}

env_get() { grep -E "^$1=" .env | head -1 | cut -d= -f2- | tr -d '\r'; }
env_or() { local v; v="$(env_get "$1")"; printf '%s' "${v:-$2}"; }

demo_cleanup() { restaurar_producer; rm -rf "$TMP"; }

demo_init() {
  DEMO_ARGS=("$@")
  demo_parse_args "$@"
  command -v docker >/dev/null 2>&1 || { echo "Este script corre dentro de la EC2 de la app: no encuentro docker." >&2; exit 1; }
  if ! docker info >/dev/null 2>&1; then
    command -v sudo >/dev/null 2>&1 && exec sudo -E bash "$DEMO_SCRIPT" "${DEMO_ARGS[@]}"
    echo "Sin permisos para docker; ejecuta con sudo." >&2; exit 1
  fi
  [ -f "$APP_DIR/docker-compose.yml" ] && [ -f "$APP_DIR/.env" ] || { echo "No encuentro docker-compose.yml y .env en $APP_DIR (ajusta APP_DIR)." >&2; exit 1; }
  cd "$APP_DIR" || exit 1
  export HOME="${HOME:-/root}"

  BS="$(env_get KAFKA_BOOTSTRAP)"
  [ -n "$BS" ] || { echo "KAFKA_BOOTSTRAP vacío en $APP_DIR/.env." >&2; exit 1; }
  KAFKA_IMAGE="confluentinc/cp-kafka:$(env_or CONFLUENT_VERSION 7.5.3)"
  RAW_TOPIC="$(env_or RAW_TOPIC ventas-raw)"
  AGG_TOPIC="$(env_or AGG_TOPIC ventas-agregadas)"
  DLQ_TOPIC="$(env_or DLQ_TOPIC ventas-raw-dlq)"
  G="$(env_or GROUP_ID ventas-aggregator)"
  W="$(env_or WINDOW_SECONDS 60)"
  GRACE="$(env_or WINDOW_GRACE_SECONDS 30)"
  DEDUP_S="$(env_or DEDUP_RETENTION_SECONDS 3600)"
  REP_TOPIC="$G-por-tienda-repartition"
  CHANGELOG_TOPIC="$G-ventas-eventids-vistos-changelog"
  RUN="$(date +%s%N | md5sum | cut -c1-6)"

  [ -n "$(docker compose ps --status running --services 2>/dev/null | grep -x consumer)" ] \
    || { echo "El servicio consumer no está corriendo en $APP_DIR." >&2; exit 1; }

  [ -n "$EVIDENCIA" ] && mkdir -p "$EVIDENCIA"
  TMP="$(mktemp -d)"
  trap demo_cleanup EXIT
  echo "app=$APP_DIR | bootstrap=$BS | imagen CLI=$KAFKA_IMAGE | grupo=$G | ventana=${W}s gracia=${GRACE}s | corrida=$RUN"
}

# ---- pasos y verificaciones ------------------------------------------------------------------------
# paso nombre-corto "título": ejecuta la función paso_cuerpo (que cada demo define antes) mostrando su salida
# en vivo y dejándola también en $OUT. Corre en un subshell: lo que necesite el resto del script se calcula
# después a partir de $OUT.
paso() {
  STEP=$((STEP + 1))
  local n; n="$(printf '%02d' "$STEP")"
  printf '\n\033[1;36m==== [%s] %s ====\033[0m\n' "$n" "$2"
  paso_cuerpo 2>&1 | tee "$TMP/paso.out"
  OUT="$(cat "$TMP/paso.out")"
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
nota() { printf '\033[1;33m  >> \033[0m%s\n' "$*"; }
count() { printf '%s\n' "$1" | grep -c -- "$2" || true; }
valor() { printf '%s\n' "$OUT" | sed -n "s/^$1=//p" | tail -1 | tr -d '[:space:]'; }
solo_num() { local n="${1//[!0-9]/}"; printf '%s' "${n:-0}"; }
ge() { [ "$1" -ge "$2" ]; }

demo_fin() {
  echo
  if [ "$FAILS" -eq 0 ]; then printf '\033[1;32mDemostración completa: todas las verificaciones pasaron.\033[0m\n'
  else printf '\033[1;31m%d verificación(es) fallaron.\033[0m\n' "$FAILS"; fi
  [ -n "$EVIDENCIA" ] && echo "Evidencia en: $EVIDENCIA"
  [ "$FAILS" -eq 0 ]
  exit $?
}

# Reintenta cada 3 s hasta que el comando devuelva 0 o pasen $1 segundos.
esperar_hasta() {
  local lim="$1" t0=$SECONDS; shift
  while ! "$@"; do
    [ $((SECONDS - t0)) -ge "$lim" ] && return 1
    sleep 3
  done
}

# ---- Kafka CLI -------------------------------------------------------------------------------------
# Contenedor efímero en la red del host. kcli no hereda stdin (no se come el de un `while read`); kcli_i sí,
# para el console-producer.
kcli() { docker run --rm --network host "$KAFKA_IMAGE" "$@" </dev/null; }
kcli_i() { docker run --rm -i --network host "$KAFKA_IMAGE" "$@"; }

# Offsets finales de un tópico, "partición offset" por línea, en $TMP/<archivo>.
offsets_finales() { # tópico, archivo
  kcli kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server "$BS" --topic "$1" --time -1 2>/dev/null |
    tr -d '\r' | awk -F: 'NF==3 {print $2, $3}' | sort -n > "$TMP/$2"
}
snap_offsets() { offsets_finales "$1" "snap-$2"; } # tópico, etiqueta: marca el punto de partida
suma_offsets() { # tópico → suma de offsets finales de todas las particiones
  kcli kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server "$BS" --topic "$1" --time -1 2>/dev/null |
    tr -d '\r' | awk -F: 'NF==3 {s+=$3} END{print s+0}'
}

# Registros escritos en el tópico DESPUÉS de snap_offsets (mismo tópico y etiqueta), opcionalmente filtrados por
# patrón. Lee las particiones en paralelo y las imprime ordenadas: "Partition:P<TAB>Offset:O<TAB>key<TAB>valor".
# Los tópicos que escribe Streams (EOS) tienen marcadores de transacción que ocupan offsets sin entregarse:
# por eso la lectura termina por --timeout-ms y no por --max-messages.
leer_desde() { # tópico, etiqueta, [patrón]
  local topic="$1" tag="$2" pat="${3:-.}" p e s n
  offsets_finales "$topic" "fin-$tag"
  rm -f "$TMP"/rd-"$tag"-*
  while read -r p e; do
    s="$(awk -v p="$p" '$1==p {print $2}' "$TMP/snap-$tag")"; s="${s:-0}"
    n=$((e - s))
    [ "$n" -gt 0 ] || continue
    ( kcli kafka-console-consumer --bootstrap-server "$BS" --topic "$topic" --partition "$p" --offset "$s" \
        --max-messages "$n" --timeout-ms 5000 --property print.partition=true --property print.offset=true \
        --property print.key=true 2>/dev/null | grep -a -- "$pat" > "$TMP/rd-$tag-$p" ) &
  done < "$TMP/fin-$tag"
  wait
  cat "$TMP"/rd-"$tag"-* 2>/dev/null
  rm -f "$TMP"/rd-"$tag"-*
  return 0
}

# El sobre de la DLQ sin el valor original (para tablas legibles).
dlq_sin_original() { sed -E 's/"originalValue":"([^"\\]|\\.)*",//'; }
campo() { # json, campo → valor sin comillas (strings simples y números)
  printf '%s' "$1" | grep -o "\"$2\":\"\{0,1\}[^\",}]*" | head -1 | sed -E 's/^"[^"]*":"?//'
}
razon() { campo "$1" reason; }

# ---- consumer group --------------------------------------------------------------------------------
grupo_describe() { kcli kafka-consumer-groups --bootstrap-server "$BS" --describe --group "$G" 2>/dev/null | tr -d '\r'; }
grupo_offsets() { # tabla TOPIC PART CURRENT END LAG MIEMBRO(host)
  printf '%-44s %-5s %-10s %-10s %-6s %s\n' TOPIC PART CURRENT LOG-END LAG MIEMBRO
  grupo_describe | awk -v g="$G" '$1==g {printf "%-44s %-5s %-10s %-10s %-6s %s\n", $2, $3, $4, $5, $6, $8}' | sort -k1,1 -k2,2n
}
grupo_lag() { # → "total max" (solo particiones con lag numérico)
  grupo_describe | awk -v g="$G" '$1==g && $6 ~ /^[0-9]+$/ {t+=$6; if ($6+0>m) m=$6+0} END{print t+0, m+0}'
}
grupo_miembros() { kcli kafka-consumer-groups --bootstrap-server "$BS" --describe --group "$G" --members 2>/dev/null | tr -d '\r' | grep -c "^$G " || true; }

# ---- contenedores y métricas -----------------------------------------------------------------------
consumer_info() { # una línea por réplica: nombre, estado, inicio, reinicios
  local id
  for id in $(docker compose ps -q consumer); do
    docker inspect -f '{{.Name}} estado={{.State.Status}} inicio={{.State.StartedAt}} reinicios={{.RestartCount}}' "$id"
  done
}
log_consumer() { # desde (RFC3339), patrón → líneas del log sin el prefijo de compose
  docker compose logs --since "$1" consumer 2>&1 | grep -a -- "$2" | sed 's/^[^|]*| //;s/^\[[^]]*\] //' | cut -c1-300
}
wait_running() { # desde-timestamp (solo cuenta logs posteriores, no un RUNNING de una corrida anterior)
  local i
  for i in $(seq 1 40); do
    [ -n "$(docker compose logs --since "$1" consumer 2>&1 | grep "State transition from REBALANCING to RUNNING")" ] && return 0
    sleep 3
  done
  return 1
}

METRICAS="ventas_procesadas_total ventas_invalidas_total ventas_duplicadas_total ventas_tardias_total ventas_enviadas_dlq_total"
# Contador de negocio sumado entre réplicas (9407 y 9408); vacío si ningún exporter responde.
metric() {
  local port v tot=0 hay=0
  for port in 9407 9408; do
    v="$(curl -s --max-time 5 "localhost:$port/metrics" | awk -v m="$1" '$1==m || index($1, m "{")==1 {print $2; exit}')"
    [ -n "$v" ] && { tot="$(awk -v a="$tot" -v b="$v" 'BEGIN{printf "%d", a+b}')"; hay=1; }
  done
  [ "$hay" = 1 ] && printf '%s' "$tot"
}
metrics() {
  local m v
  for m in $METRICAS; do
    v="$(metric "$m")"
    printf '%-28s %s\n' "$m" "${v:-(el exporter no responde en :9407/:9408)}"
  done
}

# ---- ventas de prueba ------------------------------------------------------------------------------
iso() { date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ; }

# JSON de una venta válida (los 13 campos de Sale). total = cantidad × precio salvo que se pase un 7º argumento.
venta_json() { # eventId, eventTime(ISO), tienda, cantidad, precio, customerId, [total]
  local total="${7:-$(($4 * $5))}"
  printf '{"eventId":"%s","eventTime":"%s","store":"%s","storeFormat":"tienda","city":"Santiago","channel":"presencial","category":"ropa","product":"Polera","quantity":%d,"unitPriceClp":%d,"totalClp":%d,"customerId":"%s","paymentMethod":"debito"}' \
    "$1" "$2" "$3" "$4" "$5" "$total" "$6"
}

# Escribe en ventas-raw las líneas "key|json" que lleguen por stdin (la key es el customerId, como en ProducerApp).
enviar() {
  kcli_i kafka-console-producer --bootstrap-server "$BS" --topic "$RAW_TOPIC" \
    --property parse.key=true --property key.separator='|' 2>&1 | grep -v '^>'
}

# Tiendas reales del producer (las mismas que usa ScenarioProducer); las demos NO inventan tiendas.
STORE_A="Paris Costanera Center"
STORE_B="Falabella Plaza Oeste"
RESTART_PID="/tmp/ventas-demo-restart.pid"

# Los agregados de una tienda real se mezclan con las ventas que el producer escribe en vivo (eventTime = ahora). Para que
# el resultado de una demo sea exacto se pausa el producer y se usan ventanas que empiezan DESPUÉS de la pausa (ninguna
# venta en vivo puede caer ahí). El producer se reactiva solo al terminar, cuando el stream time adelantado por el cierre
# ya pasó (si no, sus ventas serían tardías). Debe llamarse una sola vez y antes de enviar los eventos de prueba.
PRODUCER_PAUSADO=0
pausar_producer() {
  local had_timer=0 pid=""
  if [ -f "$RESTART_PID" ]; then
    pid="$(cat "$RESTART_PID" 2>/dev/null || true)"
    [ -n "$pid" ] && kill -- -"$pid" "$pid" 2>/dev/null || true
    rm -f "$RESTART_PID"
    had_timer=1
  fi
  if [ -n "$(docker compose ps --status running --services 2>/dev/null | grep -x producer)" ] || [ "$had_timer" = 1 ]; then
    docker compose stop producer 2>&1 | tail -2
    PRODUCER_PAUSADO=1
    nota "producer pausado para que las ventas en vivo no se mezclen con las de la demo (se reactiva solo al terminar)"
  fi
}
restaurar_producer() {
  [ "$PRODUCER_PAUSADO" = 1 ] || return 0
  # cerrar_ventana corre en un subshell (paso): el instante llega por STATE_FILE. Al empezar, esperar_stream_time garantiza
  # que cualquier valor futuro es de esta corrida.
  local hasta espera
  hasta="$(cat "$STATE_FILE" 2>/dev/null || echo 0)"; espera=$((hasta - $(date +%s) + 3))
  [ "$espera" -gt 3 ] || espera=0
  if [ "$espera" -eq 0 ]; then
    docker compose start producer >/dev/null 2>&1 && nota "producer reactivado"
  else
    nota "el producer se reactivará solo en ${espera}s, cuando pase el stream time adelantado por el evento de cierre"
    ( cd "$APP_DIR" && nohup setsid bash -c "echo \$\$ > $RESTART_PID; sleep $espera; docker compose start producer >/dev/null 2>&1; rm -f $RESTART_PID" >/dev/null 2>&1 & )
  fi
}

# Inicio B de la primera ventana tumbling que empieza después de ahora (en WS). Con el producer pausado esa ventana, y las
# siguientes, no tienen ventas en vivo. Los eventTime de prueba son relativos a B (hasta ~2 ventanas hacia adelante, bajo
# el límite de MAX_FUTURE_SKEW_SECONDS): lo que importa a Streams es el stream time, no el reloj.
siguiente_ventana() {
  local now; now="$(date +%s)"; WS=$(((now / W + 1) * W))
  nota "ventana de la demo: [$(iso "$WS"), $(iso $((WS + W)))) (la siguiente, aún sin ventas)"
}

# Cierra las ventanas con fin <= $2: el stream time es por partición y solo avanza con eventos que llegan a la agregación,
# así que se envía una venta mínima (1 CLP) de la MISMA tienda con eventTime = fin + gracia + 10 s (<= ahora + ~210 s con la config
# por defecto; el validador acepta hasta MAX_FUTURE_SKEW_SECONDS). EFECTO: esa partición queda con stream time adelantado
# hasta ese instante (restaurar_producer espera a que pase) y queda una venta de 1 CLP (eventId <corrida>-cierre) en una
# ventana posterior de la tienda.
cerrar_ventana() { # tienda, fin-de-la-última-ventana (epoch)
  # +10 s de margen (como los tests y ScenarioProducer): suppress(untilWindowCloses) emite cuando el stream time PASA
  # fin + gracia; con eventTime justo en fin + gracia la ventana no se emite.
  local t=$(($2 + GRACE + 10))
  [ "$t" -le $(($(date +%s) + 280)) ] || { echo "el cierre quedaría demasiado en el futuro (>280 s)" >&2; return 1; }
  printf '%s|%s\n' "cliente-0001" "$(venta_json "$RUN-cierre" "$(iso "$t")" "$1" 1 1 cliente-0001)" | enviar
  echo "$t" > "$STATE_FILE"
  echo "evento de cierre enviado: eventTime=$(iso "$t") (fin de ventana + gracia + 10 s), 1 CLP, eventId $RUN-cierre"
}
esperar_stream_time() {
  local hasta ahora
  hasta="$(cat "$STATE_FILE" 2>/dev/null || echo 0)"; ahora="$(date +%s)"
  if [ "$hasta" -gt "$ahora" ]; then
    nota "una demo anterior adelantó el stream time hasta $(iso "$hasta"); espero $((hasta - ahora + 5)) s para no provocar falsos tardíos"
    sleep $((hasta - ahora + 5))
  fi
}

# ---- agregados -------------------------------------------------------------------------------------
# Espera hasta tener $3 resultados de la tienda $1 en ventas-agregadas desde el snapshot $2 (60 s como máximo).
agg_listo() { # tienda, etiqueta, n
  [ "$(leer_desde "$AGG_TOPIC" "$2" "$1" | wc -l)" -ge "$3" ]
}
mostrar_agg() { # líneas de ventas-agregadas por stdin
  printf '%-10s %-22s %-22s %-7s %-10s %s\n' PARTICION window_start window_end ventas total_clp ticket_prom
  local l p o k v
  while IFS=$'\t' read -r p o k v; do
    [ -n "$v" ] || continue
    printf '%-10s %-22s %-22s %-7s %-10s %s\n' "${p#Partition:}" "$(campo "$v" window_start)" "$(campo "$v" window_end)" \
      "$(campo "$v" sales_count)" "$(campo "$v" total_clp)" "$(campo "$v" avg_ticket_clp)"
  done
}
suma_agg() { # campo (sales_count|total_clp), líneas por stdin
  local t=0 l v
  while IFS= read -r l; do v="$(campo "$l" "$1")"; t=$((t + ${v:-0})); done
  echo "$t"
}
mostrar_raw() { # registros de ventas-raw (o del repartition) por stdin: "Partition:P<TAB>Offset:O<TAB>key<TAB>json"
  printf '%-10s %-8s %-24s %-24s %-22s %-14s %s\n' PARTICION OFFSET KEY eventId eventTime customerId totalClp
  local p o k v
  while IFS=$'\t' read -r p o k v; do
    [ -n "$v" ] || continue
    printf '%-10s %-8s %-24s %-24s %-22s %-14s %s\n' "${p#Partition:}" "${o#Offset:}" "$k" "$(campo "$v" eventId)" \
      "$(campo "$v" eventTime)" "$(campo "$v" customerId)" "$(campo "$v" totalClp)"
  done
}

# Patrón (grep) de los resultados de ventas-agregadas de una tienda para ciertas ventanas (inicios en epoch). Hace falta porque
# al cerrar la ventana de la demo Streams también emite las ventanas en vivo que ya vencieron para esa tienda.
pat_agg() { # tienda, inicio-de-ventana...
  local store="$1" alt="" ws; shift
  for ws in "$@"; do alt="${alt:+$alt\|}$(iso "$ws")"; done
  printf '"store":"%s".*"window_start":"\(%s\)"' "$store" "$alt"
}
