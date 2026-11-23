#!/usr/bin/env bash
# Demostración 2: flujo completo de una venta. Un JSON con eventTime y customerId entra a ventas-raw, se asigna a una
# partición por su key, Streams lo revalida, lo reparticiona por tienda y la ventana tumbling lo agrega; el resultado
# sale en ventas-agregadas con inicio/fin de ventana, cantidad de ventas y monto.
#
# Se ejecuta DENTRO de la EC2 de la app (act3-app), por ejemplo desde Session Manager. Usa una tienda real del producer
# (Paris Costanera Center) y clientes del pool real (cliente-0101, cliente-0102). Pausa el producer mientras dura la demo
# y usa la siguiente ventana, aún sin ventas: así el resultado agregado contiene solo estas 3 ventas.
#
# Flujo:
#   1 la venta          JSON de ejemplo y qué campos deciden la partición y la ventana
#   2 ventas-raw        3 ventas (2 clientes): partición/offset de cada registro (misma key => misma partición)
#   3 repartition       Streams las reparticiona por tienda (key = tienda): las 3 caen en la misma partición
#   4 ventas-agregadas  se cierra la ventana y sale UN resultado: window_start/end, sales_count=3, total_clp=55000
#
# Uso:  sudo bash /opt/ventas/demostracion/02-flujo-completo.sh [--pausa] [--evidencia DIR]
#   --pausa          espera Enter entre pasos (para narrar en vivo)
#   --evidencia DIR  guarda la salida de cada paso en DIR (NN-nombre.out.txt)
# Env:  APP_DIR (/opt/ventas, donde están docker-compose.yml y .env)
#
# Efectos: PAUSA el producer (docker compose stop producer) y lo reactiva solo al terminar, cuando pasa el stream time que
# adelanta el evento de cierre (hasta ~2.5 min). Escribe 4 ventas en ventas-raw: las 3 de la demo y un cierre de 1 CLP
# (eventTime = fin de ventana + gracia + 10 s) que queda en una ventana posterior de la tienda. Las ventas de prueba quedan en
# ventas-agregadas como cualquier otra venta de la tienda.

set -uo pipefail
. "$(dirname "$(readlink -f "$0")")/lib.sh"
demo_init "$@"

STORE="$STORE_A"
C1="cliente-0101"; C2="cliente-0102"

pausar_producer
esperar_stream_time
siguiente_ventana
PATAGG="$(pat_agg "$STORE" "$WS")"
T1="$(iso $((WS + 1)))"; T2="$(iso $((WS + 2)))"; T3="$(iso $((WS + 3)))"
S1="$(venta_json "$RUN-v1" "$T1" "$STORE" 2 10000 "$C1")"
S2="$(venta_json "$RUN-v2" "$T2" "$STORE" 1 5000 "$C2")"
S3="$(venta_json "$RUN-v3" "$T3" "$STORE" 1 30000 "$C1")"
snap_offsets "$RAW_TOPIC" raw; snap_offsets "$AGG_TOPIC" agg; snap_offsets "$REP_TOPIC" rep

# ---- 1. la venta -----------------------------------------------------------------------------------
paso_cuerpo() {
  echo "$S1"
  cat <<EOF

- eventTime  $T1  -> momento de la venta (en la ventana siguiente, que el producer pausado deja sin ventas): Streams lo usa como timestamp del registro y decide la ventana tumbling de ${W}s
             [$(iso "$WS"), $(iso $((WS + W)))). Se emite al cerrar la ventana: fin + gracia (${GRACE}s) = $((W + GRACE)) s tras su inicio.
- customerId $C1   -> es la KEY del registro en ventas-raw (como hace ProducerApp): partición = murmur2(key) mod nº de particiones.
- store      $STORE -> en Streams se reparticiona por tienda: todas las ventas de una tienda llegan a una misma tarea y ventana.
EOF
}
paso venta "La venta: JSON con eventTime y customerId"

# ---- 2. ventas-raw ---------------------------------------------------------------------------------
paso_cuerpo() {
  printf '%s|%s\n%s|%s\n%s|%s\n' "$C1" "$S1" "$C2" "$S2" "$C1" "$S3" | enviar
  echo "enviadas 3 ventas: $C1 (2 x 10000 = 20000), $C2 (1 x 5000), $C1 (1 x 30000)"
  echo "## $RAW_TOPIC: partición y offset asignados (key = customerId)"
  leer_desde "$RAW_TOPIC" raw "$RUN-v" | mostrar_raw
}
paso ventas-raw "$RAW_TOPIC: asignación a partición según la key (customerId)"
RAW_FILAS="$(printf '%s\n' "$OUT" | grep -c -- "$RUN-v")"
check "ventas visibles en $RAW_TOPIC" 3 "$RAW_FILAS"
check "particiones distintas usadas por $C1 (misma key => misma partición)" 1 \
  "$(printf '%s\n' "$OUT" | awk -v k="$C1" '$3==k {print $1}' | sort -u | wc -l | tr -d ' ')"

# ---- 3. repartition por tienda ---------------------------------------------------------------------
paso_cuerpo() {
  local i L=""
  for i in $(seq 1 15); do
    L="$(leer_desde "$REP_TOPIC" rep "$RUN-v")"
    [ "$(printf '%s\n' "$L" | grep -c .)" -ge 3 ] && break
    sleep 3
  done
  echo "## $REP_TOPIC (key = tienda): Streams reparticionó las ventas por tienda"
  printf '%s\n' "$L" | mostrar_raw
}
paso repartition "Repartition por tienda: las 3 ventas quedan en la misma partición"
check "ventas en el repartition" 3 "$(printf '%s\n' "$OUT" | grep -c -- "$RUN-v")"
check "particiones usadas por la tienda $STORE" 1 "$(printf '%s\n' "$OUT" | grep -F -- "$RUN-v" | awk '{print $1}' | sort -u | wc -l | tr -d ' ')"

# ---- 4. cierre de ventana y agregado ---------------------------------------------------------------
paso_cuerpo() {
  cerrar_ventana "$STORE" $((WS + W)) || exit 1
  echo "esperando el resultado de la ventana (Streams lo emite al cerrarse)..."
  esperar_hasta 75 agg_listo "$PATAGG" agg 1 || echo "(no apareció el agregado en 75 s)"
  L="$(leer_desde "$AGG_TOPIC" agg "$PATAGG")"
  echo "## $AGG_TOPIC (key = tienda): mensaje tal como sale"
  printf '%s\n' "$L"
  echo
  echo "## resumen"
  printf '%s\n' "$L" | mostrar_agg
}
paso ventas-agregadas "$AGG_TOPIC: resultado agregado de la ventana"
FILA="$(printf '%s\n' "$OUT" | grep -E '^[0-9]+ +20' | head -1)"
check "window_start" "$(iso "$WS")" "$(printf '%s' "$FILA" | awk '{print $2}')"
check "window_end" "$(iso $((WS + W)))" "$(printf '%s' "$FILA" | awk '{print $3}')"
check "sales_count" 3 "$(printf '%s' "$FILA" | awk '{print $4}')"
check "total_clp" 55000 "$(printf '%s' "$FILA" | awk '{print $5}')"
check "avg_ticket_clp (55000 / 3 redondeado)" 18333 "$(printf '%s' "$FILA" | awk '{print $6}')"

demo_fin
