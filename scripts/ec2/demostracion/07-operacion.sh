#!/usr/bin/env bash
# Demostración 7: operación del pipeline. Muestra el estado del consumer group, sus offsets, el lag por partición y la
# distribución de particiones entre miembros y entre brokers (la misma información que Kafka UI -> Consumers / Topics / Brokers,
# aquí por CLI), y relaciona cada métrica con la acción operativa que dispara.
#
# Se ejecuta DENTRO de la EC2 de la app (act3-app), por ejemplo desde Session Manager. Es de solo lectura salvo con --escalar.
#
# Flujo:
#   1 estado del grupo      estado (Stable/Rebalancing), coordinador, miembros
#   2 offsets y lag         por partición: offset actual, offset final y lag; total y máximo
#   3 distribución          qué miembro tiene qué partición/tarea (--members --verbose)
#   (3b --escalar)          escala el consumer a 2 réplicas y muestra la redistribución de particiones
#   4 líderes por broker    líderes y réplicas de cada broker; particiones under-replicated
#   5 entrada vs consumo    muestra de 10 s (MUESTRA_S): registros/s que entran a ventas-raw y registros/s que consume el grupo
#   6 métricas de negocio   procesadas, inválidas, duplicadas, tardías y enviadas a la DLQ
#   7 métrica -> acción     tabla evaluada con los valores reales de esta corrida y su equivalente en Kafka UI / Grafana
#
# Uso:  sudo bash /opt/ventas/demostracion/07-operacion.sh [--escalar] [--lag-alerta N] [--pausa] [--evidencia DIR]
#   --escalar        escala el consumer a 2 réplicas (docker compose up -d --scale consumer=2) y muestra el reparto
#   --lag-alerta N   umbral de lag total para recomendar escalar (100 por defecto; también env LAG_ALERTA)
#   --pausa          espera Enter entre pasos (para narrar en vivo)
#   --evidencia DIR  guarda la salida de cada paso en DIR (NN-nombre.out.txt)
# Env:  APP_DIR (/opt/ventas) | LAG_ALERTA (100) | MUESTRA_S (10, segundos de la muestra de tasas)
#
# Efectos: ninguno salvo --escalar, que deja el consumer con 2 réplicas (volver con: docker compose up -d --scale consumer=1).

set -uo pipefail
. "$(dirname "$(readlink -f "$0")")/lib.sh"

ESCALAR=0
LAG_ALERTA="${LAG_ALERTA:-100}"
MUESTRA_S="${MUESTRA_S:-10}"
demo_extra_arg() {
  case "$1" in
    --escalar) ESCALAR=1; return 0 ;;
    --lag-alerta) LAG_ALERTA="${2:?--lag-alerta requiere un número}"; EXTRA_N=2; return 0 ;;
  esac
  return 1
}
demo_init "$@"

grupo_consumido() { # suma del offset consumido por el grupo en ventas-raw
  grupo_describe | awk -v g="$G" -v t="$RAW_TOPIC" '$1==g && $2==t {s+=$4} END{print s+0}'
}
reparto() { # particiones/tareas por miembro (host)
  grupo_describe | awk -v g="$G" '$1==g {n[$8]++} END{for (h in n) printf "  %-22s %d particiones\n", h, n[h]}' | sort
}

# ---- 1. estado del grupo ---------------------------------------------------------------------------
paso_cuerpo() {
  local st
  st="$(kcli kafka-consumer-groups --bootstrap-server "$BS" --describe --group "$G" --state 2>/dev/null | tr -d '\r')"
  printf '%s\n' "$st"
  echo "ESTADO=$(printf '%s\n' "$st" | awk -v g="$G" '$1==g {print $(NF-1)}')"
  echo "MIEMBROS=$(printf '%s\n' "$st" | awk -v g="$G" '$1==g {print $NF}')"
  echo "## miembros (--members)"
  kcli kafka-consumer-groups --bootstrap-server "$BS" --describe --group "$G" --members 2>/dev/null | tr -d '\r'
}
paso grupo-estado "Consumer group $G: estado, coordinador y miembros"
ESTADO="$(valor ESTADO)"; MIEMBROS="$(solo_num "$(valor MIEMBROS)")"
check "estado del grupo" Stable "$ESTADO"
check_cond "el grupo tiene miembros" "$MIEMBROS" test "$MIEMBROS" -ge 1

# ---- 2. offsets y lag ------------------------------------------------------------------------------
paso_cuerpo() {
  grupo_offsets
  local t m
  read -r t m < <(grupo_lag)
  echo "LAG_TOTAL=$t"
  echo "LAG_MAX=$m"
  echo "-> CURRENT = offset que el grupo ya procesó y confirmó; LOG-END = último offset escrito; LAG = LOG-END - CURRENT"
}
paso offsets-lag "Offsets y lag por partición"
LAG_TOTAL="$(solo_num "$(valor LAG_TOTAL)")"; LAG_MAX="$(solo_num "$(valor LAG_MAX)")"
NPART="$(printf '%s\n' "$OUT" | grep -cE "^($RAW_TOPIC|$REP_TOPIC) ")"
check_cond "hay particiones con offsets del grupo" "$NPART particiones" test "$NPART" -ge 1

# ---- 3. distribución de particiones entre miembros -------------------------------------------------
paso_cuerpo() {
  echo "## partición -> miembro (kafka-consumer-groups --members --verbose)"
  kcli kafka-consumer-groups --bootstrap-server "$BS" --describe --group "$G" --members --verbose 2>/dev/null | tr -d '\r' | cut -c1-230
  echo "## particiones por miembro"
  reparto
  echo "-> Streams crea una tarea por partición de cada tópico fuente (ventas-raw y el repartition por tienda): más réplicas que particiones de ventas-raw no aportan"
}
paso distribucion "Distribución de particiones entre los miembros del grupo"

# ---- 3b. escalar (opcional) ------------------------------------------------------------------------
if [ "$ESCALAR" = 1 ]; then
  paso_cuerpo() {
    local i
    docker compose up -d --scale consumer=2 2>&1 | tail -4
    for i in $(seq 1 30); do
      [ "$(grupo_miembros)" = 2 ] && [ -z "$(grupo_describe | awk -v g="$G" '$1==g && $7=="-"')" ] && break
      sleep 4
    done
    echo "## particiones por miembro tras escalar a 2 réplicas"
    reparto
    echo "MIEMBROS=$(grupo_miembros)"
    kcli kafka-consumer-groups --bootstrap-server "$BS" --describe --group "$G" --members --verbose 2>/dev/null | tr -d '\r' | cut -c1-230
    echo "-> el rebalanceo reparte las tareas; el estado de cada tarea se restaura desde su changelog en el miembro que la recibe"
    echo "-> para volver a 1 réplica: docker compose up -d --scale consumer=1"
  }
  paso escalar "Escalar el consumer a 2 réplicas: redistribución de particiones"
  check "miembros del grupo tras escalar" 2 "$(valor MIEMBROS)"
  MIEMBROS=2
fi

# ---- 4. líderes por broker -------------------------------------------------------------------------
paso_cuerpo() {
  local t
  : > "$TMP/part-lines"
  for t in "$RAW_TOPIC" "$AGG_TOPIC" "$DLQ_TOPIC"; do
    kcli kafka-topics --bootstrap-server "$BS" --describe --topic "$t" 2>/dev/null | tr -d '\r' | grep 'Partition: ' >> "$TMP/part-lines"
  done
  printf '%-8s %-10s %s\n' BROKER LIDERES REPLICAS
  awk '{for (i=1;i<=NF;i++) { if ($i=="Leader:") l[$(i+1)]++; if ($i=="Replicas:") { n=split($(i+1),r,","); for (j=1;j<=n;j++) rep[r[j]]++ } } }
       END{for (b in rep) printf "%-8s %-10d %d\n", b, l[b]+0, rep[b]}' "$TMP/part-lines" | sort -n
  awk '{for (i=1;i<=NF;i++) if ($i=="Leader:") l[$(i+1)]++} END{mn=-1; mx=0; for (b in l) { if (mn<0||l[b]<mn) mn=l[b]; if (l[b]>mx) mx=l[b] } print "LIDERES_DIF=" mx-(mn<0?0:mn)}' "$TMP/part-lines"
  echo "UNDER_REPLICATED=$(kcli kafka-topics --bootstrap-server "$BS" --describe --under-replicated-partitions 2>/dev/null | grep -c 'Partition: ')"
  echo "## ISR de las particiones (Leader / Replicas / Isr)"
  sed -E 's/^[ \t]*//; s/[ \t]+/  /g' "$TMP/part-lines"
}
paso lideres-brokers "Distribución de particiones entre brokers: líderes, réplicas y under-replicated"
LIDERES_DIF="$(solo_num "$(valor LIDERES_DIF)")"; URP="$(solo_num "$(valor UNDER_REPLICATED)")"
check "particiones under-replicated" 0 "$URP"

# ---- 5. entrada vs consumo -------------------------------------------------------------------------
paso_cuerpo() {
  local a0 a1 g0 g1 t0 dt
  a0="$(suma_offsets "$RAW_TOPIC")"; g0="$(grupo_consumido)"; t0=$SECONDS
  echo "muestreando ${MUESTRA_S}s (offsets finales de $RAW_TOPIC y offsets confirmados del grupo)..."
  sleep "$MUESTRA_S"
  a1="$(suma_offsets "$RAW_TOPIC")"; g1="$(grupo_consumido)"; dt=$((SECONDS - t0)); [ "$dt" -ge 1 ] || dt=1
  awk -v a0="$a0" -v a1="$a1" -v g0="$g0" -v g1="$g1" -v dt="$dt" 'BEGIN{
    printf "%-34s %s -> %s\n", "log-end de ventas-raw:", a0, a1
    printf "%-34s %s -> %s\n", "offset consumido por el grupo:", g0, g1
    printf "%-34s %d -> %d\n", "lag de ventas-raw:", a0-g0, a1-g1
    printf "ENTRADA_POR_S=%.2f\n", (a1-a0)/dt
    printf "CONSUMO_POR_S=%.2f\n", (g1-g0)/dt }'
}
paso tasas "Entrada vs consumo: ¿el grupo alcanza al productor?"
ENTRADA="$(valor ENTRADA_POR_S)"; CONSUMO="$(valor CONSUMO_POR_S)"

# ---- 6. métricas de negocio ------------------------------------------------------------------------
paso_cuerpo() {
  metrics
  echo "-> contadores en memoria desde el último arranque del consumer (se reinician con el contenedor); con 2 réplicas se suman :9407 y :9408"
}
paso metricas "Métricas de negocio del consumer"
PROC="$(solo_num "$(metric ventas_procesadas_total)")"; INV="$(solo_num "$(metric ventas_invalidas_total)")"
DUP="$(solo_num "$(metric ventas_duplicadas_total)")"; TAR="$(solo_num "$(metric ventas_tardias_total)")"
DLQN="$(solo_num "$(metric ventas_enviadas_dlq_total)")"

# ---- 7. métrica -> acción --------------------------------------------------------------------------
fila() { printf '%-30s %-14s %-9s %s\n' "$1" "$2" "$3" "$4"; }
flt() { awk -v a="$1" -v b="$2" -v f="$3" 'BEGIN{exit !(a < b * f)}'; } # a < b*f
paso_cuerpo() {
  local pct
  pct="$(awk -v d="$DLQN" -v p="$PROC" 'BEGIN{ if (d+p>0) printf "%.1f", 100*d/(d+p); else print "0.0" }')"
  fila SEÑAL VALOR ESTADO "ACCIÓN OPERATIVA"
  if [ "$LAG_TOTAL" -gt "$LAG_ALERTA" ]; then
    fila "lag total del grupo" "$LAG_TOTAL" ALTO "escalar: docker compose up -d --scale consumer=2 (máx. útil = 4, particiones de $RAW_TOPIC); revisar CPU de act3-app (Grafana: Infraestructura EC2)"
  else
    fila "lag total del grupo" "$LAG_TOTAL" OK "sin acción; vigilar la tendencia (umbral $LAG_ALERTA)"
  fi
  fila "lag max. por particion" "$LAG_MAX" "$([ "$LAG_MAX" -gt $((LAG_ALERTA / 2)) ] && echo REVISAR || echo OK)" "si una sola partición concentra el lag: key sesgada (un cliente/tienda dominante) o tarea lenta; escalar no la reparte"
  if flt "$CONSUMO" "$ENTRADA" 0.9; then
    fila "consumo vs entrada (reg/s)" "$CONSUMO / $ENTRADA" "ALERTA" "el grupo consume menos de lo que entra: el lag crecerá; escalar consumers o revisar el procesamiento"
  else
    fila "consumo vs entrada (reg/s)" "$CONSUMO / $ENTRADA" OK "el consumo alcanza la entrada"
  fi
  if [ "$ESTADO" != Stable ]; then
    fila "estado del grupo" "$ESTADO" ALERTA "rebalanceo en curso o inestable: revisar reinicios del consumer (docker compose ps, logs [STREAMS_ERROR])"
  else
    fila "estado del grupo" "$ESTADO" OK "$MIEMBROS miembro(s) estable(s); sin acción"
  fi
  if [ "$URP" -gt 0 ]; then
    fila "particiones under-replicated" "$URP" ALERTA "un broker caído o lento: revisar docker compose ps en su EC2 y recuperarlo (demostración 6); el ISR vuelve a 2 al resincronizar"
  else
    fila "particiones under-replicated" "$URP" OK "ISR completo; sin acción"
  fi
  if [ "$LIDERES_DIF" -gt 1 ]; then
    fila "desbalance de lideres" "$LIDERES_DIF" REVISAR "rebalancear: kafka-leader-election --election-type PREFERRED --all-topic-partitions"
  else
    fila "desbalance de lideres" "$LIDERES_DIF" OK "líderes repartidos entre los brokers"
  fi
  fila "% de salida enviada a la DLQ" "${pct}%" "$(awk -v p="$pct" 'BEGIN{print (p > 20) ? "ALERTA" : "OK"}')" "inspeccionar reason en $DLQ_TOPIC (demostración 3); si crece, revisar el productor (INVALID_RATE) o el esquema de origen"
  fila "duplicadas / tardias" "$DUP / $TAR" "$([ $((DUP + TAR)) -gt 0 ] && echo REVISAR || echo OK)" "duplicadas: reenvíos del origen/red (demostración 4); tardías: latencia aguas arriba o ajustar WINDOW_GRACE_SECONDS (demostración 5)"
  echo
  cat <<EOF
Dónde verlo en Kafka UI (https://<KAFKA_DOMAIN>, en act3-app-zookeper) y Grafana:
  - Consumers -> $G: estado, miembros, partición asignada y lag por partición (estado, offsets y distribución).
  - Topics -> $RAW_TOPIC -> Partitions: líder, réplicas, ISR y offsets; Brokers: particiones y líderes por broker (paso "líderes por broker").
  - Topics -> $DLQ_TOPIC -> Messages: reason y valor original de los descartes.
  - Grafana (carpeta Ventas Lab -> "EA3 - Observabilidad integral Kafka y ventas"): lag, UnderReplicatedPartitions y fila "Negocio" (paso "métricas de negocio").
EOF
}
paso metrica-accion "Métrica -> acción operativa (con los valores de esta corrida)"

demo_fin
