#!/usr/bin/env bash
# Demostración 4: duplicados. Dos registros con el mismo eventId entran a ventas-raw (la idempotencia del producer no
# los evita), pero el monto y el conteo de negocio NO se duplican: el consumer deriva el segundo a la DLQ como
# EVENT_ID_DUPLICADO. Además muestra cómo se conserva ese estado (store con changelog) y que sobrevive a un crash.
# Se ejecuta DENTRO de la EC2 de la app (ec2-app), donde corren el consumer y el producer; no usa SSM ni credenciales AWS.
#
# Flujo:
#   1 línea base          contenedores, configuración de dedup y métricas
#   2 duplicado           ScenarioProducer duplicate: el mismo JSON se envía dos veces
#   3 idempotencia        el producer es idempotente (acks=all, ProducerId asignado) y aun así envió ambos
#   4 ventas-raw          el mismo eventId aparece 2 veces (la idempotencia no lo toca)
#   5 ventas-raw-dlq      1 EVENT_ID_DUPLICADO
#   6 métricas y log      ventas_duplicadas_total +1
#   7 estado              el store de dedup (ventas-eventids-vistos) y su changelog: cómo se conserva el estado
#   8 crash del consumer  kill -9 y reinicio; el store se restaura desde el changelog y los offsets del grupo continúan
#   9 reenvío             mismo eventId con eventTime actual (un reenvío exacto sería EVENTO_TARDIO tras ~90 s)
#   10 ventas-raw-dlq     sigue saliendo EVENT_ID_DUPLICADO: el estado sobrevivió al crash
#   11 envío (negocio)    en Paris Costanera Center (producer pausado): venta S1 (10000) enviada 2 veces + venta S2 (5000)
#   12 agregado           ventas-raw tiene 3 registros, la DLQ 1 duplicado y el agregado cuenta 2 ventas / 15000 (no 3 / 25000)
#
# Cómo llega a la EC2: `node deploy-app.js` (desde scripts/) lo copia a /opt/ventas/demostracion con permiso de ejecución,
# pero no lo ejecuta. Se corre a mano, en la EC2 de la app, por ejemplo desde Session Manager.
#
# Uso:  sudo bash /opt/ventas/demostracion/04-duplicados.sh [--sin-crash] [--pausa] [--evidencia DIR]
#   --sin-crash      omite los pasos 8-10 (no reinicia el consumer)
#   --pausa          espera Enter entre pasos (para narrar en vivo)
#   --evidencia DIR  guarda la salida de cada paso en DIR (NN-nombre.out.txt)
# Env:  APP_DIR (/opt/ventas, donde están docker-compose.yml y .env)
#
# Efectos: escribe eventos de prueba en ventas-raw y ventas-raw-dlq, con la tienda real Paris Costanera Center y clientes del
# pool real. Los pasos 8-10 reinician el consumer (el estado se restaura solo; los contadores en memoria de BusinessMetrics
# vuelven a 0). Los pasos 11-12 PAUSAN el producer (para que el agregado no se mezcle con ventas en vivo) y lo reactivan solo
# al terminar, cuando pasa el stream time que adelanta el evento de cierre (hasta ~2.5 min); el cierre es una venta de 1 CLP.

set -uo pipefail
. "$(dirname "$(readlink -f "$0")")/lib.sh"

CRASH=1
demo_extra_arg() { case "$1" in --sin-crash) CRASH=0; return 0 ;; esac; return 1; }
demo_init "$@"
esperar_stream_time

D0="$(solo_num "$(metric ventas_duplicadas_total)")"
snap_offsets "$RAW_TOPIC" raw; snap_offsets "$DLQ_TOPIC" dlq

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
SC="$(printf '%s\n' "$OUT" | grep -o '"eventId":"[^"]*-d1"' | head -1 | cut -d'"' -f4)"
[ -n "$SC" ] || { echo "No pude obtener el eventId del escenario; abortando." >&2; exit 1; }
echo ">> eventId de la prueba: $SC"

# ---- 3. idempotencia del producer ------------------------------------------------------------------
paso_cuerpo() {
  grep -E "enable.idempotence|acks =|Instantiated an idempotent producer|ProducerId set to" "$TMP/producer.err" | sed 's/^\[[^]]*\] //;s/^[ \t]*//'
  echo "-> con idempotencia activa el broker descarta reintentos de UN mismo envío; aquí hubo dos send() distintos."
}
paso idempotencia-producer "El producer del escenario es idempotente (y aun así envió los dos)"

# ---- 4. ventas-raw ---------------------------------------------------------------------------------
paso_cuerpo() { leer_desde "$RAW_TOPIC" raw "$SC"; }
paso ventas-raw-dos-registros "$RAW_TOPIC: el mismo eventId aparece 2 veces (la idempotencia no lo elimina)"
check "registros con ese eventId en $RAW_TOPIC" 2 "$(count "$OUT" "$SC")"
RAW_P="$(printf '%s\n' "$OUT" | grep -o 'Partition:[0-9]*' | head -1 | cut -d: -f2)"
RAW_OFFS="$(printf '%s\n' "$OUT" | grep -o 'Offset:[0-9]*' | cut -d: -f2 | sort -n)"
RAW_OFF1="$(printf '%s\n' "$RAW_OFFS" | head -1)"
RAW_OFF2="$(printf '%s\n' "$RAW_OFFS" | tail -1)"

# ---- 5. DLQ ----------------------------------------------------------------------------------------
dlq_tiene_sc() { [ "$(leer_desde "$DLQ_TOPIC" dlq "$SC" | grep -c 'Partition:')" -ge 1 ]; }
paso_cuerpo() { esperar_hasta 45 dlq_tiene_sc; leer_desde "$DLQ_TOPIC" dlq "$SC" | dlq_sin_original; }
paso dlq-event-id-duplicado "$DLQ_TOPIC: 1 EVENT_ID_DUPLICADO"
check "EVENT_ID_DUPLICADO en la DLQ" 1 "$(count "$OUT" '"reason":"EVENT_ID_DUPLICADO"')"

# ---- 6. métricas y log -----------------------------------------------------------------------------
paso_cuerpo() {
  metrics
  echo "-- log del consumer --"
  docker compose logs consumer 2>&1 | grep "\[DUPLICADO\]" | grep "$SC" | sed 's/^[^|]*| //;s/^\[[^]]*\] //'
}
paso metricas-despues "Métricas y log del consumer (ventas_duplicadas_total debe subir en 1)"
D1="$(solo_num "$(metric ventas_duplicadas_total)")"
check_cond "ventas_duplicadas_total subió >= 1" "$D0 -> $D1" ge "$((D1 - D0))" 1

if [ -n "$RAW_P" ] && [ -n "$RAW_OFF1" ]; then
  cat <<EOF

(Opcional, en la EC2 de ZooKeeper/broker: el log del broker guarda un producerId y una secuencia por envío.)
  docker compose exec -T kafka-1 bash -c 'for f in /var/lib/kafka/data/$RAW_TOPIC-$RAW_P/*.log; do kafka-dump-log --files \$f | grep -E "baseOffset: ($RAW_OFF1|$RAW_OFF2) "; done'
  Esperado: mismo producerId y baseSequence distintos para los offsets $RAW_OFF1 y $RAW_OFF2.
EOF
fi

# ---- 7. cómo se conserva el estado -----------------------------------------------------------------
paso_cuerpo() {
  echo "## changelog del store de deduplicación: $CHANGELOG_TOPIC"
  kcli kafka-topics --bootstrap-server "$BS" --describe --topic "$CHANGELOG_TOPIC" 2>/dev/null | tr -d '\r'
  echo "## offsets finales (suma de particiones) = registros escritos al changelog: $(suma_offsets "$CHANGELOG_TOPIC")"
  cat <<EOF

- DedupLateProcessor corre DESPUÉS del repartition por tienda: los reenvíos de un mismo evento caen en la misma tarea.
- Cada eventId aceptado se guarda en un WindowStore ('ventas-eventids-vistos': key=eventId, timestamp=eventTime) durante
  DEDUP_RETENTION_SECONDS=${DEDUP_S} s. Un eventId ya presente en [eventTime - retención, eventTime + retención] es duplicado:
  va a la DLQ y no se agrega.
- El store vive en disco local del consumer (RocksDB) y, además, cada put se escribe al changelog de arriba (RF=2, compactado),
  en la misma transacción EOS que el resultado y los offsets. Si el consumer cae o la tarea cambia de instancia, Streams
  restaura el store leyendo el changelog antes de volver a procesar (paso siguiente).
- Límite: solo se recuerda ${DEDUP_S} s. Además, un reenvío cuya ventana ya cerró (fin + gracia) se trata como EVENTO_TARDIO, no como duplicado.
EOF
}
paso estado-dedup "Cómo se conserva el estado: store de dedup y su changelog"
check "changelog con ReplicationFactor 2" 1 "$(count "$OUT" 'ReplicationFactor: 2')"

# ---- 8-10. crash, reenvío y verificación ----------------------------------------------------------
if [ "$CRASH" = 1 ] && [ -z "$RAW_P" ]; then
  echo "No pude ubicar los registros en $RAW_TOPIC; se omiten los pasos 8-10."
  CRASH=0
  FAILS=$((FAILS + 1))
fi
if [ "$CRASH" = 1 ]; then
  paso_cuerpo() {
    local desde; desde="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "## offsets del grupo ANTES del crash"
    grupo_offsets | grep -E "^TOPIC|^$RAW_TOPIC "
    docker compose kill consumer
    # Tras el kill el contenedor suele tomar :9408 (rango 9407-9408 del compose); se corrige al final del script
    docker compose up -d consumer 2>&1 | tail -3
    wait_running "$desde" || echo "(el consumer no llegó a RUNNING en 2 min)"
    docker compose logs --since "$desde" consumer 2>&1 | grep -E "Finished restoring changelog .*eventids-vistos|State transition from REBALANCING to RUNNING" | sed 's/^[^|]*| //;s/^\[[^]]*\] //' | cut -c1-230
    docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
    echo "## offsets del grupo DESPUÉS del reinicio (continúan donde quedaron, no vuelven a 0)"
    grupo_offsets | grep -E "^TOPIC|^$RAW_TOPIC "
  }
  paso crash-y-reinicio-consumer "Crash del consumer (kill -9) y reinicio: el store de dedup se restaura desde el changelog"

  paso_cuerpo() {
    local orig key val now val2 tab
    tab="$(printf '\t')"
    orig="$(kcli kafka-console-consumer --bootstrap-server "$BS" --topic "$RAW_TOPIC" --partition "$RAW_P" --offset "$RAW_OFF1" \
            --max-messages 1 --timeout-ms 15000 --property print.key=true 2>/dev/null | tr -d '\r' | head -1)"
    key="${orig%%"$tab"*}"
    val="${orig#*"$tab"}"
    now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    val2="$(printf '%s' "$val" | sed -E "s/\"eventTime\":\"[^\"]*\"/\"eventTime\":\"$now\"/")"
    echo "key      : $key"
    echo "original : $val"
    echo "reenvio  : $val2"
    printf '%s|%s\n' "$key" "$val2" | enviar
    echo "(enviado)"
  }
  paso reenvio-restamped "Reenvío del mismo eventId con eventTime actual (tras el reinicio)"

  DUPS=0
  for intento in 1 2 3 4 5; do
    sleep 6
    paso_cuerpo() { leer_desde "$DLQ_TOPIC" dlq "$SC" | dlq_sin_original; }
    paso dlq-tras-reinicio "$DLQ_TOPIC tras el reinicio: debe haber 2 EVENT_ID_DUPLICADO (intento $intento)"
    DUPS="$(count "$OUT" '"reason":"EVENT_ID_DUPLICADO"')"
    [ "$DUPS" -ge 2 ] && break
  done
  check "EVENT_ID_DUPLICADO acumulados (antes + después del crash)" 2 "$DUPS"

  paso_cuerpo() {
    docker compose logs consumer 2>&1 | grep "\[DUPLICADO\]" | grep "$SC" | sed 's/^[^|]*| //;s/^\[[^]]*\] //'
    docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
    metrics
  }
  paso consumer-tras-reinicio "Consumer reiniciado: log de dedup, métricas y puerto (los contadores arrancan en 0)"
  if ! curl -s --max-time 5 -o /dev/null localhost:9407/metrics; then
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

# ---- 10-11. impacto en el negocio: envío (al final: el cierre adelanta el stream time y el producer queda pausado) ---------------------------------------------------------------
STORE="$STORE_A"
CA="cliente-0104"; CB="cliente-0105"
pausar_producer
esperar_stream_time
siguiente_ventana
PATAGG="$(pat_agg "$STORE" "$WS")"
S1="$(venta_json "$RUN-s1" "$(iso $((WS + 1)))" "$STORE" 1 10000 "$CA")"
S2="$(venta_json "$RUN-s2" "$(iso $((WS + 2)))" "$STORE" 1 5000 "$CB")"
snap_offsets "$RAW_TOPIC" raw2; snap_offsets "$DLQ_TOPIC" dlq2; snap_offsets "$AGG_TOPIC" agg2
paso_cuerpo() {
  printf '%s|%s\n%s|%s\n%s|%s\n' "$CA" "$S1" "$CA" "$S1" "$CB" "$S2" | enviar
  echo "tienda $STORE (producer pausado, ventana $(iso "$WS") sin ventas en vivo): S1 (eventId $RUN-s1, 10000) enviada 2 veces + S2 (eventId $RUN-s2, 5000)"
  echo "## $RAW_TOPIC: 3 registros (la venta S1 está dos veces)"
  leer_desde "$RAW_TOPIC" raw2 "$RUN-s" | mostrar_raw
}
paso negocio-envio "Mismo eventId dos veces en $STORE: ventas-raw recibe 3 registros"
check "registros en $RAW_TOPIC (S1 x2 + S2)" 3 "$(printf '%s\n' "$OUT" | grep -cE '^[0-9]+ +[0-9]+ ')"

# ---- agregado de la ventana ------------------------------------------------------------
paso_cuerpo() {
  cerrar_ventana "$STORE" $((WS + W)) || exit 1
  echo "esperando el resultado de la ventana..."
  esperar_hasta 75 agg_listo "$PATAGG" agg2 1 || echo "(no apareció el agregado en 75 s)"
  echo "## $DLQ_TOPIC (filtrado por esta corrida)"
  leer_desde "$DLQ_TOPIC" dlq2 "$RUN-s" | dlq_sin_original
  echo
  echo "## $AGG_TOPIC: agregado de la ventana"
  leer_desde "$AGG_TOPIC" agg2 "$PATAGG" | mostrar_agg
  echo "-> sin deduplicación serían 3 ventas y 25000; el conteo y el monto de negocio cuentan S1 una sola vez."
}
paso negocio-agregado "El agregado cuenta 2 ventas y 15000: el duplicado no suma"
FILA="$(printf '%s\n' "$OUT" | grep -E '^[0-9]+ +20' | head -1)"
check "EVENT_ID_DUPLICADO de esta tienda en la DLQ" 1 "$(count "$OUT" '"reason":"EVENT_ID_DUPLICADO"')"
check "sales_count (S1 una vez + S2)" 2 "$(printf '%s' "$FILA" | awk '{print $4}')"
check "total_clp (10000 + 5000)" 15000 "$(printf '%s' "$FILA" | awk '{print $5}')"

demo_fin
