#!/usr/bin/env bash
# Demostración 3: mensajes inválidos. Se envían 5 eventos inválidos (uno por motivo de rechazo) seguidos de uno válido
# y se muestra que (a) el consumer NO se detiene ni se reinicia y sigue procesando, (b) el valor original y el
# motivo quedan disponibles en la DLQ (ventas-raw-dlq) y (c) el log del consumer explica cada rechazo.
#
# Se ejecuta DENTRO de la EC2 de la app (act3-app), por ejemplo desde Session Manager. Todos los eventos usan el mismo
# customerId (cliente-0103, del pool real) como key: caen en una sola partición y conservan el orden (5 inválidos y luego el válido).
#
# Flujo:
#   1 línea base      consumer (inicio, reinicios), métricas y lag antes de enviar
#   2 envío           5 inválidos + 1 válido en ventas-raw
#   3 ventas-raw      los 6 registros en orden (partición/offset)
#   4 DLQ             5 sobres con key, reason, detail y originalValue; esperados JSON_MALFORMADO, CAMPO_FALTANTE,
#                     CAMPO_INVALIDO, EVENT_TIME_INVALIDO y TOTAL_INCONSISTENTE
#   5 log             5 líneas [DLQ] del consumer con tópico, key, motivo, detalle y valor
#   6 continuidad     mismo contenedor y sin [STREAMS_ERROR]; el offset del grupo pasó los 6 registros; métricas
#
# Uso:  sudo bash /opt/ventas/demostracion/03-mensaje-invalido.sh [--pausa] [--evidencia DIR]
#   --pausa          espera Enter entre pasos (para narrar en vivo)
#   --evidencia DIR  guarda la salida de cada paso en DIR (NN-nombre.out.txt)
# Env:  APP_DIR (/opt/ventas, donde están docker-compose.yml y .env)
#
# Efectos: escribe 6 eventos de prueba en ventas-raw y 5 sobres en ventas-raw-dlq. No reinicia nada ni adelanta
# el stream time (el válido tiene eventTime = ahora).

set -uo pipefail
. "$(dirname "$(readlink -f "$0")")/lib.sh"
demo_init "$@"
esperar_stream_time

STORE="$STORE_A"
CUST="cliente-0103"
DESDE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
INFO_ANTES="$(consumer_info)"
P0="$(solo_num "$(metric ventas_procesadas_total)")"; I0="$(solo_num "$(metric ventas_invalidas_total)")"
snap_offsets "$RAW_TOPIC" raw; snap_offsets "$DLQ_TOPIC" dlq

# ---- 1. línea base ---------------------------------------------------------------------------------
paso_cuerpo() {
  echo "## consumer"
  consumer_info
  echo "## métricas de negocio (contadores en memoria, suma de réplicas)"
  metrics
  echo "## lag del grupo $G (total y máximo por partición)"
  grupo_lag
}
paso linea-base "Línea base: el consumer está corriendo"

# ---- 2. envío --------------------------------------------------------------------------------------
T="$(iso "$(date +%s)")"
# Marca de esta corrida: el RUN en el eventId o el eventTime de segundos exactos (los eventos en vivo llevan fracción de segundo)
PAT="$RUN\|$T"
V0="$(venta_json "$RUN-i1" "$T" "$STORE" 2 10000 "$CUST")"
CASOS=(
  "JSON_MALFORMADO|${V0:0:70}"
  "CAMPO_FALTANTE|$(venta_json "$RUN-i2" "$T" "$STORE" 1 1000 "$CUST" | sed 's/"eventId":"[^"]*",//')"
  "CAMPO_INVALIDO|$(venta_json "$RUN-i3" "$T" "$STORE" 0 1000 "$CUST")"
  "EVENT_TIME_INVALIDO|$(venta_json "$RUN-i4" ayer "$STORE" 1 1000 "$CUST")"
  "TOTAL_INCONSISTENTE|$(venta_json "$RUN-i5" "$T" "$STORE" 3 1000 "$CUST" 9999)"
)
VALIDO="$(venta_json "$RUN-ok" "$T" "$STORE" 1 7000 "$CUST")"
paso_cuerpo() {
  local c
  for c in "${CASOS[@]}"; do
    printf '%-20s %s\n' "${c%%|*}" "$(printf '%s' "${c#*|}" | cut -c1-110)..."
    printf '%s|%s\n' "$CUST" "${c#*|}" | enviar
  done
  printf '%-20s %s\n' "(valido)" "$(printf '%s' "$VALIDO" | cut -c1-110)..."
  printf '%s|%s\n' "$CUST" "$VALIDO" | enviar
  echo "enviados 5 inválidos y 1 válido, en ese orden"
}
paso envio "Envío de 5 eventos inválidos y 1 válido a $RAW_TOPIC"

# ---- 3. ventas-raw ---------------------------------------------------------------------------------
paso_cuerpo() { leer_desde "$RAW_TOPIC" raw "$PAT" | cut -c1-170; }
paso ventas-raw "$RAW_TOPIC: los 6 registros, en orden, en una misma partición"
RAW_P="$(printf '%s\n' "$OUT" | grep -o 'Partition:[0-9]*' | head -1 | cut -d: -f2)"
LAST_OFF="$(printf '%s\n' "$OUT" | grep -- "$RUN-ok" | grep -o 'Offset:[0-9]*' | head -1 | cut -d: -f2)"
check "registros en $RAW_TOPIC" 6 "$(printf '%s\n' "$OUT" | grep -c 'Partition:')"

# ---- 4. DLQ ----------------------------------------------------------------------------------------
dlq_hay_5() { [ "$(leer_desde "$DLQ_TOPIC" dlq "$PAT" | grep -c 'Partition:')" -ge 5 ]; }
paso_cuerpo() {
  esperar_hasta 60 dlq_hay_5 || echo "(no aparecieron los 5 sobres en 60 s)"
  local n=0 p o k v
  leer_desde "$DLQ_TOPIC" dlq "$PAT" | while IFS=$'\t' read -r p o k v; do
    n=$((n + 1))
    printf '[%d] reason=%s  key=%s  (%s %s)\n' "$n" "$(razon "$v")" "$k" "$p" "$o"
    printf '    detail  : %s\n' "$(printf '%s' "$v" | sed -E 's/.*"detail":"(([^"\\]|\\.)*)".*/\1/' | cut -c1-150)"
    printf '    original: %s\n' "$(printf '%s' "$v" | sed -E 's/.*"originalValue":"(([^"\\]|\\.)*)".*/\1/; s/\\"/"/g' | cut -c1-150)"
  done
}
paso dlq "$DLQ_TOPIC: el valor original y el motivo de cada rechazo"
for r in JSON_MALFORMADO CAMPO_FALTANTE CAMPO_INVALIDO EVENT_TIME_INVALIDO TOTAL_INCONSISTENTE; do
  check "DLQ reason=$r" 1 "$(count "$OUT" "reason=$r ")"
done

# ---- 5. log del consumer ---------------------------------------------------------------------------
paso_cuerpo() {
  local i
  for i in 1 2 3 4 5; do
    [ "$(log_consumer "$DESDE" '\[DLQ\]' | grep -c -- "$PAT")" -ge 5 ] && break
    sleep 3
  done
  log_consumer "$DESDE" '\[DLQ\]' | grep -- "$PAT" | cut -c1-240
}
paso log-consumer "Log del consumer: una línea [DLQ] por rechazo, con motivo, detalle y valor"
check "líneas [DLQ] de esta corrida en el log" 5 "$(count "$OUT" '\[DLQ\]')"

# ---- 6. continuidad --------------------------------------------------------------------------------
paso_cuerpo() {
  local i og=""
  echo "## consumer"
  consumer_info
  echo "STREAMS_ERROR=$(docker compose logs --since "$DESDE" consumer 2>&1 | grep -c STREAMS_ERROR)"
  for i in 1 2 3 4 5 6 7 8 9 10; do
    og="$(grupo_describe | awk -v g="$G" -v t="$RAW_TOPIC" -v p="$RAW_P" '$1==g && $2==t && $3==p {print $4}')"
    [ "${og:-0}" -gt "${LAST_OFF:-0}" ] && break
    sleep 3
  done
  echo "## offset del grupo en $RAW_TOPIC partición $RAW_P: $og (el válido está en el offset $LAST_OFF)"
  echo "OFFSET_GRUPO=${og:-0}"
  echo "## offsets y lag del grupo"
  grupo_offsets | grep -E "^TOPIC|^$RAW_TOPIC "
  echo "## métricas de negocio"
  metrics
}
paso continuidad "El consumer continúa: sin reinicios, sin errores y con el offset del grupo avanzando"
P1="$(solo_num "$(metric ventas_procesadas_total)")"; I1="$(solo_num "$(metric ventas_invalidas_total)")"
check "consumer sin reinicios (mismo contenedor, mismo inicio)" "$INFO_ANTES" "$(consumer_info)"
check "[STREAMS_ERROR] en el log" 0 "$(valor STREAMS_ERROR)"
check_cond "el grupo consumió también el válido (offset > $LAST_OFF)" "offset del grupo $(valor OFFSET_GRUPO)" test "$(valor OFFSET_GRUPO)" -gt "${LAST_OFF:-0}"
check_cond "ventas_invalidas_total subió >= 5" "$I0 -> $I1" ge "$((I1 - I0))" 5
check_cond "ventas_procesadas_total sigue subiendo" "$P0 -> $P1" test "$P1" -gt "$P0"

demo_fin
