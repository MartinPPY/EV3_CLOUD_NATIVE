#!/usr/bin/env bash
# Demostración 5: eventos tardíos. Política: una ventana tumbling de 60 s acepta eventos fuera de orden hasta que se cierra
# (fin de ventana + gracia). Un evento cuya ventana ya cerró (fin + gracia <= stream time) NO se agrega: va a la DLQ como
# EVENTO_TARDIO, con log [TARDIO_DESCARTADO] y contador; uno dentro de la gracia se suma a la ventana que le corresponde.
#
# Se ejecuta DENTRO de la EC2 de la app (act3-app), por ejemplo desde Session Manager. Usa una tienda real del producer
# (Falabella Plaza Oeste) y un cliente del pool real (cliente-0106). Pausa el producer mientras dura la demo y usa dos ventanas
# consecutivas aún sin ventas (C1 = [B, B+60 s) y C2 = [B+60 s, B+120 s), con B el inicio de la próxima ventana): así los
# agregados contienen solo las ventas de la demo. Lo que decide Streams es el stream time, no el reloj, por eso los eventTime
# son relativos a B.
#
# Eventos (el stream time de la partición es el mayor eventTime visto):
#   L1  eventTime = B + 61 s    ventana C2: se agrega y deja el stream time en B + 61 s
#   L2  eventTime = B + 10 s    ventana C1 (fin B+60): fin + gracia = B+90 > B+61 -> DENTRO de la gracia, se suma a C1
#   L3  eventTime = B - 10 s    ventana [B-60, B) (fin B): fin + gracia = B+30 <= B+61 -> FUERA de la gracia: DLQ EVENTO_TARDIO
#
# Flujo:
#   1 política          ventanas, cierre = fin + gracia y veredicto esperado de cada evento
#   2 envío             L1, L2 y L3 en ventas-raw (en ese orden)
#   3 DLQ y log         1 EVENTO_TARDIO (L3), log [TARDIO_DESCARTADO] con windowEnd/streamTime y ventas_tardias_total +1
#   4 agregados         se cierran las ventanas: C1 = L2 (8000) y C2 = L1 (7000); L3 (4000) no está en ninguna
#   5 limitación        reenviar L1 con su ventana ya cerrada sale como EVENTO_TARDIO (no EVENT_ID_DUPLICADO)
#   6 límites           qué se observa y qué no cubre esta política
#
# Uso:  sudo bash /opt/ventas/demostracion/05-evento-tardio.sh [--pausa] [--evidencia DIR]
#   --pausa          espera Enter entre pasos (para narrar en vivo)
#   --evidencia DIR  guarda la salida de cada paso en DIR (NN-nombre.out.txt)
# Env:  APP_DIR (/opt/ventas, donde están docker-compose.yml y .env)
#
# Efectos: PAUSA el producer (docker compose stop producer) y lo reactiva solo al terminar, cuando pasa el stream time que
# adelanta el evento de cierre (hasta ~3.5 min). Escribe 5 ventas en ventas-raw (L1, L2, L3, un cierre de 1 CLP y el reenvío
# de L1) y 2 sobres en ventas-raw-dlq. Las ventas de prueba quedan en ventas-agregadas como cualquier otra venta de la tienda.

set -uo pipefail
. "$(dirname "$(readlink -f "$0")")/lib.sh"
demo_init "$@"

STORE="$STORE_B"
CUST="cliente-0106"

pausar_producer
esperar_stream_time
siguiente_ventana
B=$WS
DESDE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
E1=$((B + W + 1)); E2=$((B + 10)); E3=$((B - 10)); ST=$E1
L1="$(venta_json "$RUN-l1" "$(iso "$E1")" "$STORE" 1 7000 "$CUST")"
L2="$(venta_json "$RUN-l2" "$(iso "$E2")" "$STORE" 1 8000 "$CUST")"
L3="$(venta_json "$RUN-l3" "$(iso "$E3")" "$STORE" 1 4000 "$CUST")"
PATAGG="$(pat_agg "$STORE" "$B" $((B + W)))"
T0="$(solo_num "$(metric ventas_tardias_total)")"
snap_offsets "$RAW_TOPIC" raw; snap_offsets "$DLQ_TOPIC" dlq; snap_offsets "$AGG_TOPIC" agg

# ---- 1. política -----------------------------------------------------------------------------------
fila_politica() { # nombre, epoch del evento
  local fin=$(( $2 / W * W + W )) cierre veredicto
  cierre=$((fin + GRACE))
  if [ "$cierre" -le "$ST" ]; then veredicto="TARDIO: cierra $(iso "$cierre") <= stream time -> DLQ EVENTO_TARDIO"
  else veredicto="DENTRO DE GRACIA: cierra $(iso "$cierre") > stream time -> se agrega"; fi
  printf '%-4s %-22s [%s, %s)  %s\n' "$1" "$(iso "$2")" "$(iso $((fin - W)))" "$(iso "$fin")" "$veredicto"
}
paso_cuerpo() {
  cat <<EOF
Política (StreamsApp / DedupLateProcessor): ventana tumbling = ${W}s, gracia = ${GRACE}s. Una ventana se cierra cuando
stream time >= fin + gracia; el stream time de una partición es el mayor eventTime que ha visto (L1 lo deja en $(iso "$ST")).
  - evento cuya ventana sigue abierta (fin + gracia > stream time)  -> se agrega a SU ventana por eventTime
  - evento cuya ventana ya cerró (fin + gracia <= stream time)      -> DLQ EVENTO_TARDIO + log + ventas_tardias_total

EOF
  printf '%-4s %-22s %-44s %s\n' EVT eventTime ventana veredicto
  fila_politica L1 "$E1"; fila_politica L2 "$E2"; fila_politica L3 "$E3"
}
paso politica "Política de eventos tardíos en $STORE: ventana, gracia y veredicto esperado por evento"
check_cond "L2 queda dentro de la gracia" "fin de su ventana $(iso $((B + W))) + gracia ${GRACE}s > stream time $(iso "$ST")" test $((B + W + GRACE)) -gt "$ST"
check_cond "L3 queda fuera de la gracia" "fin de su ventana $(iso "$B") + gracia ${GRACE}s <= stream time $(iso "$ST")" test $((B + GRACE)) -le "$ST"

# ---- 2. envío --------------------------------------------------------------------------------------
paso_cuerpo() {
  printf '%s|%s\n%s|%s\n%s|%s\n' "$CUST" "$L1" "$CUST" "$L2" "$CUST" "$L3" | enviar
  echo "enviados L1 (7000), L2 (8000) y L3 (4000), en ese orden"
  leer_desde "$RAW_TOPIC" raw "$RUN-l" | mostrar_raw
}
paso envio "Envío de L1 (adelanta el stream time), L2 (dentro de gracia) y L3 (fuera de gracia)"
check "registros en $RAW_TOPIC" 3 "$(printf '%s\n' "$OUT" | grep -c -- "$RUN-l")"

# ---- 3. DLQ y log ----------------------------------------------------------------------------------
dlq_tiene_l3() { [ "$(leer_desde "$DLQ_TOPIC" dlq "$RUN-l" | grep -c 'Partition:')" -ge 1 ]; }
paso_cuerpo() {
  esperar_hasta 60 dlq_tiene_l3 || echo "(no apareció el sobre en la DLQ en 60 s)"
  echo "## $DLQ_TOPIC"
  leer_desde "$DLQ_TOPIC" dlq "$RUN-l" | while IFS=$'\t' read -r p o k v; do
    printf '%s %s key=%s reason=%s eventId=%s\n    detail: %s\n' "$p" "$o" "$k" "$(razon "$v")" \
      "$(printf '%s' "$v" | grep -o "$RUN-l[0-9]" | head -1)" \
      "$(printf '%s' "$v" | sed -E 's/.*"detail":"(([^"\\]|\\.)*)".*/\1/')"
  done
  echo "## log del consumer"
  log_consumer "$DESDE" 'TARDIO_DESCARTADO' | grep -- "$RUN"
}
paso dlq-y-log "DLQ y log: L3 se descarta de la agregación y queda auditable"
T1="$(solo_num "$(metric ventas_tardias_total)")"
check "EVENTO_TARDIO en la DLQ" 1 "$(count "$OUT" 'reason=EVENTO_TARDIO')"
check "el evento descartado es L3 (fuera de gracia)" 1 "$(count "$OUT" "reason=EVENTO_TARDIO eventId=$RUN-l3")"
check "L1 y L2 no están en la DLQ" 0 "$(count "$OUT" "reason=[A-Z_]* eventId=$RUN-l[12]")"
check "línea [TARDIO_DESCARTADO] en el log" 1 "$(count "$OUT" 'TARDIO_DESCARTADO')"
check_cond "ventas_tardias_total subió >= 1" "$T0 -> $T1" ge "$((T1 - T0))" 1

# ---- 4. ventanas cerradas y agregados --------------------------------------------------------------
paso_cuerpo() {
  cerrar_ventana "$STORE" $((B + 2 * W)) || exit 1
  echo "esperando los resultados de las ventanas (se emiten al cerrarse)..."
  esperar_hasta 75 agg_listo "$PATAGG" agg 2 || echo "(no aparecieron 2 agregados en 75 s)"
  echo "## $AGG_TOPIC ($STORE, ventanas $(iso "$B") y $(iso $((B + W))))"
  leer_desde "$AGG_TOPIC" agg "$PATAGG" | mostrar_agg
  echo "-> C1 suma L2 (8000) aunque llegó después de L1, porque estaba dentro de la gracia; L3 (4000) no está en ninguna."
}
paso agregados "Agregados: L2 (dentro de gracia) se suma a su ventana; L3 (fuera de gracia) no aparece"
FILAS="$(printf '%s\n' "$OUT" | grep -E '^[0-9]+ +20')"
check "ventanas emitidas para la tienda" 2 "$(printf '%s\n' "$FILAS" | grep -c .)"
check "C1 [$(iso "$B")] (L2): ventas / total" "1 / 8000" \
  "$(printf '%s\n' "$FILAS" | awk -v s="$(iso "$B")" '$2==s {print $4 " / " $5}')"
check "C2 [$(iso $((B + W)))] (L1): ventas / total" "1 / 7000" \
  "$(printf '%s\n' "$FILAS" | awk -v s="$(iso $((B + W)))" '$2==s {print $4 " / " $5}')"
check "total agregado (8000 + 7000; sin los 4000 de L3)" 15000 "$(printf '%s\n' "$FILAS" | awk '{t+=$5} END{print t+0}')"

# ---- 5. limitación: reenvío tardío -----------------------------------------------------------------
snap_offsets "$DLQ_TOPIC" dlq3
dlq3_hay() { [ "$(leer_desde "$DLQ_TOPIC" dlq3 "$RUN-l1" | grep -c 'Partition:')" -ge 1 ]; }
paso_cuerpo() {
  printf '%s|%s\n' "$CUST" "$L1" | enviar
  echo "reenviado L1 (mismo eventId $RUN-l1, eventTime $(iso "$E1")) cuando su ventana ya cerró"
  esperar_hasta 60 dlq3_hay || echo "(no apareció el sobre en 60 s)"
  leer_desde "$DLQ_TOPIC" dlq3 "$RUN-l1" | dlq_sin_original
}
paso reenvio-tardio "Limitación: un duplicado de una ventana ya cerrada se etiqueta EVENTO_TARDIO"
check "reenvío etiquetado EVENTO_TARDIO" 1 "$(count "$OUT" '"reason":"EVENTO_TARDIO"')"
check "reenvío etiquetado EVENT_ID_DUPLICADO" 0 "$(count "$OUT" '"reason":"EVENT_ID_DUPLICADO"')"

# ---- 6. límites ------------------------------------------------------------------------------------
paso_cuerpo() {
  cat <<EOF
- Precedencia de chequeos: validación -> tardío -> duplicado. Un reenvío cuya ventana ya cerró cuenta como tardío, no como duplicado
  (arriba); solo los reenvíos con la ventana abierta se marcan EVENT_ID_DUPLICADO. En ambos casos el evento no suma al agregado.
- El stream time es POR PARTICIÓN y solo avanza con eventos: una ventana se emite cuando llega un evento posterior a esa misma partición.
  Con tráfico real ocurre solo; en esta demo se envía un evento de cierre (eventTime = fin + gracia + 10 s).
- El veredicto depende del stream time, no del reloj: con una partición casi sin tráfico un evento muy atrasado puede aceptarse
  mientras nadie cierre su ventana, y con ráfagas el stream time salta y hay más tardíos.
- La gracia (${GRACE}s) retrasa cada resultado: una ventana de ${W}s se publica ${W}+${GRACE} = $((W + GRACE)) s después de su inicio.
- Los tardíos no se reprocesan solos: quedan en ${DLQ_TOPIC} (reason=EVENTO_TARDIO, con la venta original) para auditoría o reproceso manual.
EOF
}
paso limites "Límites de la política y limitaciones observadas"

demo_fin
