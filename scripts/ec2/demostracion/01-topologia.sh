#!/usr/bin/env bash
# Demostración 1: topología y configuración real del clúster (particiones, replicación, retención, ISR) y por qué
# el clúster base, con 2 brokers, no puede alojar tópicos con RF=3.
#
# Se ejecuta DENTRO de la EC2 de la app (act3-app), por ejemplo desde Session Manager. Solo lee la configuración
# del clúster; el único "escribir" es un intento de crear un tópico con RF=3 que DEBE fallar (y se comprueba que
# no quedó creado).
#
# Flujo:
#   1 brokers              brokers registrados (esperado: 2)
#   2 tópicos y ISR        particiones, RF, Leader/Replicas/Isr de ventas-raw, ventas-agregadas y ventas-raw-dlq
#   3 configuración        retención efectiva, min.insync.replicas y demás configs de ventas-raw + configs del broker
#   4 tópicos internos     repartition/changelog de Streams, __consumer_offsets: todos con RF=2
#   5 intento RF=3         kafka-topics --create --replication-factor 3 falla: "larger than available brokers: 2"
#   6 explicación          por qué 2 brokers => RF máximo 2 y qué implica para min.insync.replicas
#
# Uso:  sudo bash /opt/ventas/demostracion/01-topologia.sh [--pausa] [--evidencia DIR]
#   --pausa          espera Enter entre pasos (para narrar en vivo)
#   --evidencia DIR  guarda la salida de cada paso en DIR (NN-nombre.out.txt)
# Env:  APP_DIR (/opt/ventas, donde están docker-compose.yml y .env)

set -uo pipefail
. "$(dirname "$(readlink -f "$0")")/lib.sh"
demo_init "$@"

RAW_P_ESP="$(env_or RAW_PARTITIONS 4)"; AGG_P_ESP="$(env_or AGG_PARTITIONS 2)"; DLQ_P_ESP="$(env_or DLQ_PARTITIONS 4)"
RF_ESP="$(env_or TOPIC_REPLICATION_FACTOR 2)"

# ---- 1. brokers ------------------------------------------------------------------------------------
paso_cuerpo() {
  kcli kafka-broker-api-versions --bootstrap-server "$BS" 2>/dev/null | tr -d '\r' |
    grep -E '^[^ ]+:[0-9]+ \(id: [0-9]+' | sed -E 's/ -> .*//' | sort
}
paso brokers "Brokers del clúster (bootstrap: $BS)"
NBROK="$(count "$OUT" 'id: ')"
BID="$(printf '%s\n' "$OUT" | sed -n 's/.*(id: \([0-9]*\).*/\1/p' | head -1)"
check "brokers disponibles" 2 "$NBROK"

# ---- 2. tópicos, particiones, RF, ISR --------------------------------------------------------------
paso_cuerpo() {
  local t
  for t in "$RAW_TOPIC" "$AGG_TOPIC" "$DLQ_TOPIC"; do
    kcli kafka-topics --bootstrap-server "$BS" --describe --topic "$t" 2>/dev/null | tr -d '\r' | tee "$TMP/desc-$t"
  done
}
paso topicos-isr "Tópicos de la app: particiones, factor de replicación, líderes e ISR"
verifica_topico() { # tópico, particiones esperadas
  local d parts rf lineas isr2
  d="$(cat "$TMP/desc-$1" 2>/dev/null)"
  parts="$(printf '%s\n' "$d" | grep -o 'PartitionCount: [0-9]*' | head -1 | grep -o '[0-9]*$')"
  rf="$(printf '%s\n' "$d" | grep -o 'ReplicationFactor: [0-9]*' | head -1 | grep -o '[0-9]*$')"
  lineas="$(printf '%s\n' "$d" | grep -c 'Partition: ')"
  isr2="$(printf '%s\n' "$d" | grep -cE 'Isr: [0-9]+,[0-9]+')"
  check "$1: particiones" "$2" "${parts:-?}"
  check "$1: replication factor" "$RF_ESP" "${rf:-?}"
  check "$1: particiones con ISR de los 2 brokers" "$lineas" "$isr2"
}
verifica_topico "$RAW_TOPIC" "$RAW_P_ESP"
verifica_topico "$AGG_TOPIC" "$AGG_P_ESP"
verifica_topico "$DLQ_TOPIC" "$DLQ_P_ESP"

# ---- 3. configuración efectiva ---------------------------------------------------------------------
paso_cuerpo() {
  echo "## $RAW_TOPIC: kafka-configs --describe --all (valor y de dónde viene)"
  kcli kafka-configs --bootstrap-server "$BS" --entity-type topics --entity-name "$RAW_TOPIC" --describe --all 2>/dev/null |
    tr -d '\r' | grep -E '^ +(cleanup.policy|retention.ms|retention.bytes|segment.ms|segment.bytes|min.insync.replicas|max.message.bytes)=' |
    sed -E 's/ sensitive=false synonyms=\{/   [/; s/\}$/]/' | tee "$TMP/cfg-raw"
  local ret; ret="$(sed -n 's/^ *retention.ms=\([0-9-]*\).*/\1/p' "$TMP/cfg-raw" | head -1)"
  echo "RETENCION_MS=$ret"
  [ -n "$ret" ] && [ "$ret" -gt 0 ] && echo "-> los registros se conservan $((ret / 3600000)) h ($((ret / 86400000)) días) o hasta el tamaño máximo de segmento; luego el broker los elimina"
  echo "MIN_ISR=$(sed -n 's/^ *min.insync.replicas=\([0-9]*\).*/\1/p' "$TMP/cfg-raw" | head -1)"
  echo
  echo "## broker ${BID:-1}: parámetros que fijan retención y replicación por defecto"
  kcli kafka-configs --bootstrap-server "$BS" --entity-type brokers --entity-name "${BID:-1}" --describe --all 2>/dev/null |
    tr -d '\r' | grep -E '^ +(log.retention.hours|log.retention.ms|log.retention.bytes|log.segment.bytes|default.replication.factor|min.insync.replicas|num.partitions|offsets.topic.replication.factor|auto.create.topics.enable)=' |
    sed -E 's/ sensitive=false synonyms=\{/   [/; s/\}$/]/'
  echo
  echo "## configuración de la app (.env)"
  grep -E '^(RAW_PARTITIONS|AGG_PARTITIONS|DLQ_PARTITIONS|TOPIC_REPLICATION_FACTOR|WINDOW_SECONDS|WINDOW_GRACE_SECONDS|DEDUP_RETENTION_SECONDS|GROUP_ID)=' .env
}
paso configuracion "Configuración efectiva: retención, min.insync.replicas y valores por defecto del broker"
check "min.insync.replicas efectivo (con 2 brokers no puede ser 2)" 1 "$(valor MIN_ISR)"
RET_MS="$(valor RETENCION_MS)"
check_cond "retención de $RAW_TOPIC definida" "retention.ms=$RET_MS" test -n "$RET_MS"
RET_H=$(( $(solo_num "$RET_MS") / 3600000 )); [ "$RET_H" -gt 0 ] || RET_H=168

# ---- 4. tópicos internos ---------------------------------------------------------------------------
paso_cuerpo() {
  local t
  for t in $(kcli kafka-topics --bootstrap-server "$BS" --list 2>/dev/null | tr -d '\r' | grep -E "^($G-|__consumer_offsets|__transaction_state)" | sort); do
    kcli kafka-topics --bootstrap-server "$BS" --describe --topic "$t" 2>/dev/null | tr -d '\r' | grep '^Topic:' |
      sed -E 's/TopicId: [^ \t]+[ \t]+//; s/[ \t]+/  /g'
  done
}
paso topicos-internos "Tópicos internos (Streams: repartition y changelog; Kafka: offsets de grupos y transacciones)"
INTERNOS="$(count "$OUT" '^Topic:')"
check_cond "hay tópicos internos que evaluar" "$INTERNOS" test "$INTERNOS" -gt 0
check "tópicos internos con ReplicationFactor: 2" "$INTERNOS" "$(count "$OUT" 'ReplicationFactor: *2')"

# ---- 5. intento con RF=3 ---------------------------------------------------------------------------
paso_cuerpo() {
  echo "\$ kafka-topics --create --topic demo-rf3-$RUN --partitions 1 --replication-factor 3"
  kcli kafka-topics --bootstrap-server "$BS" --create --topic "demo-rf3-$RUN" --partitions 1 --replication-factor 3 2>&1 |
    tr -d '\r' | grep -E 'Error|larger than|InvalidReplicationFactor' | cut -c1-200 | head -3
  echo "## ¿quedó creado? (kafka-topics --list | grep demo-rf3)"
  echo "TOPICO_RF3_EXISTE=$(kcli kafka-topics --bootstrap-server "$BS" --list 2>/dev/null | grep -c "demo-rf3-$RUN")"
}
paso intento-rf3 "Intento de crear un tópico con RF=3 en un clúster de 2 brokers (debe fallar)"
check_cond "el broker rechaza RF=3" "mensaje: larger than available brokers: $NBROK" grep -q "larger than available brokers: $NBROK" <<<"$OUT"
check "tópico RF=3 creado" 0 "$(valor TOPICO_RF3_EXISTE)"

# ---- 6. explicación --------------------------------------------------------------------------------
paso_cuerpo() {
  cat <<EOF
- Cada réplica de una partición vive en un broker distinto: el RF máximo es el número de brokers ($NBROK). Con RF=3 el controlador
  no tiene dónde ubicar la tercera copia y rechaza la creación ($RF_ESP es el máximo; lo usan los 3 tópicos, __consumer_offsets,
  __transaction_state y los tópicos internos de Streams).
- Consecuencia 1: min.insync.replicas=1. Con 2 réplicas y minISR=2, apenas cae un broker las escrituras acks=all se bloquean
  (NotEnoughReplicas). Con minISR=1 el pipeline sigue, pero con un solo broker vivo "acks=all" equivale a "confirmado por el líder":
  si ese broker falla antes de que el otro se resincronice, se pueden perder los mensajes de ese intervalo.
- Consecuencia 2: se tolera la caída de UN broker (ISR 2 -> 1, el líder pasa al vivo), no la de dos.
- Para garantizar 2 copias tolerando un fallo (RF=3 + min.insync.replicas=2) harían falta >= 3 brokers.
- Aparte de la replicación de datos, ZooKeeper corre en un solo nodo: es un punto único de falla del plano de control.
- Retención: ventas-raw conserva ${RET_H} h por defecto del broker (log.retention.hours); la deduplicación recuerda los eventId ${DEDUP_S} s
  (DEDUP_RETENTION_SECONDS) en un store con changelog (ver demostración 4).
EOF
}
paso explicacion "Por qué el clúster base (2 brokers) no puede alojar RF=3"

demo_fin
