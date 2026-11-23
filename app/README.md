# App de ventas: agregación por tienda con Kafka Streams

Lee ventas de `ventas-raw`, las valida, descarta duplicados y tardíos, y publica en `ventas-agregadas`
el resumen por tienda en ventanas tumbling de 60 s. Los eventos rechazados, duplicados y tardíos van a `ventas-raw-dlq`.

## Flujo

```
ventas-raw ──► validar ──inválido──► ventas-raw-dlq
                 │
                 ▼ (rekey por tienda + repartition "por-tienda")
        ┌─ ¿tardío fuera de gracia? ──► ventas-raw-dlq (EVENTO_TARDIO) + log + contador
        └─ ¿eventId ya visto? ────────► ventas-raw-dlq (EVENT_ID_DUPLICADO)
                 │
                 ▼
     ventana tumbling por eventTime ► suppress(untilWindowCloses) ──► ventas-agregadas
```

## Decisiones de diseño

### 1. Eventos JSON tipados
`Sale` (entrada), `TicketAggregate` (estado) y `WindowResult` (salida) son DTOs/records. En `Sale` los
numéricos son `Integer` y no `int`: así un campo ausente es `null` y se rechaza, en vez de entrar como `0`.
El `ObjectMapper` de validación es estricto: no coacciona `"5"` ni `5.7` a entero y rechaza texto después
del JSON (`SaleValidator`).

### 2. Ventana tumbling por `eventTime`
- `EventTimeExtractor` usa el `eventTime` del JSON como timestamp del record. Sin esto Kafka Streams usaría
  la hora en que el producer escribió, y el resultado dependería de retrasos de red o del consumer.
- Ventana de **60 s** por tienda: da un KPI por minuto, legible en una demo, y se cierra rápido.
- Gracia de **30 s** (`WINDOW_GRACE_SECONDS`):
  - El producer usa `customerId` como key sobre 4 particiones, así que los eventos de una misma tienda se
    consumen intercalados y llegan algo desordenados. Además hay reintentos del producer y lag del consumer.
  - 30 s cubre ese desorden sin retrasar demasiado la salida. Una gracia menor descartaría eventos válidos;
    una mayor demora el resultado final.
  - Costo: una ventana se emite al llegar el stream time a `fin + 30 s`, o sea **90 s** después de su inicio.
- `suppress(untilWindowCloses)` publica **un único resultado final** por ventana y tienda, sin revisiones
  parciales.
- Un evento con `eventTime` futuro (más de `MAX_FUTURE_SKEW_SECONDS` = 300 s sobre la hora actual) es
  inválido. Si no, uno solo adelantaría el stream time y cerraría ventanas legítimas. El extractor lo ignora
  para ese cálculo.

### 3. Salida: campos de la ventana
`ventas-agregadas` (key = tienda):

```json
{"window_start":"2026-01-01T11:50:00Z","window_end":"2026-01-01T11:51:00Z","store":"Paris Online",
 "sales_count":2,"total_clp":50000,"avg_ticket_clp":25000}
```
`window_start` es inclusivo y `window_end` exclusivo. `avg_ticket_clp` es `total_clp / sales_count` redondeado.

### 4. Validación y DLQ
`SaleValidator` rechaza con un código de motivo:

| Código | Cuándo |
|---|---|
| `JSON_MALFORMADO` | JSON ilegible, `null`, texto sobrante o tipo incorrecto (`"5"`, `5.7`) |
| `CAMPO_FALTANTE` | falta o está vacío alguno de los 13 campos |
| `CAMPO_INVALIDO` | `quantity <= 0` o `unitPriceClp <= 0` |
| `EVENT_TIME_INVALIDO` | no es ISO-8601 o está en el futuro |
| `TOTAL_INCONSISTENTE` | `totalClp != quantity × unitPriceClp` |
| `EVENT_ID_DUPLICADO` | `eventId` ya visto (ver punto 5) |
| `EVENTO_TARDIO` | JSON válido pero con la ventana ya cerrada (fin + gracia ≤ stream time; ver punto 6) |
| `ERROR_VALIDACION` | el validador lanzó una excepción inesperada (red de seguridad: el evento va a la DLQ en vez de detener Streams) |

Ante un inválido el flujo **continúa**: se escribe un log `[DLQ]` con tópico, key, motivo, detalle y valor, y se
publica en `ventas-raw-dlq` un sobre JSON (`DiscardRecord`) con la **misma key** y los campos `sourceTopic`,
`key`, `originalValue` (el valor original sin tocar), `reason`, `detail` y `discardedAt`.

El sobre nunca supera el máximo de mensaje del broker: si `originalValue` pasa de 64 K caracteres se trunca y el
sobre agrega `originalChars` con el largo real (solo aparece cuando hubo truncado). Como última red, un fallo no
controlado en un hilo de Streams se loguea (`[STREAMS_ERROR]`) y el hilo se reemplaza en vez de dejar el consumidor
detenido.

### 5. Duplicados de negocio
Un `WindowStore` de Kafka Streams (`ventas-eventids-vistos`, con changelog) guarda cada `eventId` aceptado
durante `DEDUP_RETENTION_SECONDS` (3600). Un `eventId` repetido se loguea como `[DUPLICADO]` y va a la DLQ con
motivo `EVENT_ID_DUPLICADO`; no se suma al agregado.
- La detección ocurre **después** del repartition por tienda, así los reenvíos del mismo evento caen en la
  misma tarea y se evita un segundo repartition por `eventId`. Supuesto: un `eventId` siempre pertenece a la
  misma tienda.
- En el sobre del duplicado, `key` es la tienda (la key original se pierde al reparticionar) y `originalValue`
  es el JSON re-serializado de la venta.

### 6. Eventos tardíos
Se evalúan en `DedupLateProcessor` con el stream time de la tarea, que es el que usa la ventana:

| Caso | Condición | Qué pasa |
|---|---|---|
| Fuera de orden, **dentro de la gracia** | `fin de ventana + gracia > stream time` | Se acepta y se suma a la ventana que le corresponde por `eventTime`. |
| **Después de la gracia** | `fin de ventana + gracia <= stream time` | Se **descarta** de la agregación y va a `ventas-raw-dlq` con motivo `EVENTO_TARDIO`; además log `[TARDIO_DESCARTADO]` y contador. |

Un tardío tiene JSON válido, pero igual va a la DLQ para no perder el dato: así se puede auditar o reprocesar a mano.
Su sobre trae `reason=EVENTO_TARDIO`, en `detail` el cierre de la ventana y el stream time, y en `originalValue` la venta
re-serializada; como en los duplicados, `key` es la tienda (la key original se pierde al reparticionar). Se cuenta en
`ventas_tardias_total` y no en `ventas_invalidas_total`.

**Precedencia de los chequeos** (en `DedupLateProcessor`): validación → ¿tardío? → ¿duplicado? → aceptado. Consecuencia:
un reenvío del mismo `eventId` cuya ventana ya cerró (fin + gracia) se cuenta como **tardío** (`ventas_tardias_total`,
log `[TARDIO_DESCARTADO]`) y va a la DLQ como `EVENTO_TARDIO`, no como `EVENT_ID_DUPLICADO`. Solo los reenvíos que llegan
con la ventana aún abierta (dentro de gracia) se marcan `EVENT_ID_DUPLICADO`. En ambos casos el evento no suma al agregado. Cubierto por el test
`duplicateResentAfterGraceIsLateNotDuplicate`.

**Cómo se detectan los descartes** (no son silenciosos; Kafka Streams por sí solo los perdería con un WARN interno):
- Log por cada uno, con `store`, `eventId`, `eventTime`, `windowEnd`, `streamTime` y `gracia`:
  `docker compose logs consumer | grep TARDIO_DESCARTADO`
- `BusinessMetrics` cuenta `procesadas`, `invalidos`, `duplicados` y `tardios`, y el resumen sale en cada log de descarte.
- Los inválidos, duplicados y tardíos además quedan en `ventas-raw-dlq` (rastro durable, a diferencia de los contadores en memoria):
  `kafka-console-consumer ... --topic ventas-raw-dlq` y filtrar por `reason`.

**Métricas de negocio en Prometheus/Grafana.** `BusinessMetrics` es un MBean JMX (`cl.duoc.ventas:type=BusinessMetrics`) que el
javaagent del proceso Streams (puerto 9407, reglas en `jmx/app.yml`) publica como contadores; Prometheus ya los scrapea
(job `ventas-app`, `node=app-streams`) y el dashboard `ea3-observabilidad` los grafica en la fila "Negocio":

| Métrica | Qué cuenta |
|---|---|
| `ventas_procesadas_total` | ventas válidas, no tardías ni duplicadas, que entran a la agregación |
| `ventas_enviadas_dlq_total` | registros enviados a la DLQ (= inválidas + duplicadas + tardías) |
| `ventas_invalidas_total` | rechazadas por validación |
| `ventas_duplicadas_total` | `eventId` repetido |
| `ventas_tardias_total` | fuera de la gracia: no se agregan y van a la DLQ (`EVENTO_TARDIO`) |

Son contadores en memoria: se reinician con el contenedor (usar `rate()`/`increase()`) y son métricas operativas, no contables.
Con `docker compose up -d --scale consumer=2` cada réplica tiene su propio MBean y se publica en 9407 / 9408 (máx. 2);
los paneles usan `sum()`, así que muestran el total de ambas.
Verificación: `curl -s http://localhost:9407/metrics | grep ^ventas_`.

Los tardíos comparten la DLQ con los inválidos (no hay un tópico aparte): se distinguen por `reason=EVENTO_TARDIO`, así
un consumidor de la DLQ puede reprocesarlos o ignorarlos sin otro tópico que crear y monitorear.

**Limitación:** el stream time es **por partición**. Un tardío se mide contra el avance de las ventas de su
propia partición, y una ventana solo se cierra cuando llega un evento posterior a esa misma partición. En
tráfico real eso ocurre solo; en pruebas hay que enviar un evento de cierre por tienda (lo hace el escenario).

## Pruebas

### Tests automáticos
```bash
docker run --rm -v "$PWD:/src" -w /src maven:3.9-eclipse-temurin-21 mvn -B test
```
`SaleValidatorTest` (un caso por código de rechazo) y `StreamsTopologyTest` (`TopologyTestDriver`): ventana por
`eventTime` y campos de salida, DLQ con contexto y flujo que continúa, total inconsistente, evento futuro,
duplicado, tardío dentro de la gracia, tardío después de la gracia, fallo inesperado del validador (`ERROR_VALIDACION`),
truncado del sobre DLQ y precedencia tardío/duplicado.

### Escenario reproducible (`ScenarioProducer`)
```bash
docker compose --profile scenario run --rm scenario
```
Emite una secuencia fija con `eventTime` explícitos relativos a `T0` (una ventana alineada en el pasado) e
imprime el resultado esperado de cada paso. La key de cada record es la tienda, para que sus eventos conserven
el orden dentro de una partición.

| Paso | Evento | Resultado esperado |
|---|---|---|
| 1-3 | tres ventas válidas (A, B, A) en W0 | se agregan |
| 4 | JSON truncado | DLQ `JSON_MALFORMADO` |
| 5 | sin `eventId` | DLQ `CAMPO_FALTANTE` |
| 6 | `quantity = 0` | DLQ `CAMPO_INVALIDO` |
| 7 | `eventTime = "ayer"` | DLQ `EVENT_TIME_INVALIDO` |
| 8 | `3 × 1000 ≠ 9999` | DLQ `TOTAL_INCONSISTENTE` |
| 9 | reenvío del primer `eventId` | DLQ `EVENT_ID_DUPLICADO` |
| 10-11 | evento de W1 y luego uno anterior de W0 | el de W0 es **tardío dentro de gracia**: se suma a W0 |
| 12 | evento que adelanta el stream time | cierra W0 de A |
| 13 | evento de W0 después del cierre | **tardío fuera de gracia**: DLQ `EVENTO_TARDIO` + log `[TARDIO_DESCARTADO]` |
| 14-15 | un evento de cierre por tienda | cierra W1 de A y W0 de B |

Resultado esperado en `ventas-agregadas`: **A W0** (`sales_count=3`, `total_clp=58000`), **A W1** (`2`, `16000`),
**B W0** (`1`, `5000`). Si el escenario se repite, esperar ~2 min: el `T0` de la corrida anterior deja el stream
time por delante y los eventos nuevos serían tardíos.

### Un solo tipo de caso (`docker compose exec`)
`ScenarioProducer` acepta un modo como argumento: `all` (default, la secuencia de arriba), `one-each` (uno de cada tema),
`invalid`, `duplicate` o `late`.
Con el stack arriba se ejecuta dentro del contenedor `consumer` (ya trae `app.jar`, `KAFKA_BOOTSTRAP`, `RAW_TOPIC` y las
variables de ventana). `-e JAVA_TOOL_OPTIONS=` es obligatorio: sin él la JVM nueva intentaría abrir otra vez el puerto 9407
del agente JMX y fallaría.

Un inválido, un duplicado y un tardío en un solo comando:

```bash
docker compose exec -T -e JAVA_TOOL_OPTIONS= consumer java -cp /app/app.jar cl.duoc.ventas.ScenarioProducer one-each
```

O un solo tipo de caso por comando:

```bash
docker compose exec -T -e JAVA_TOOL_OPTIONS= consumer java -cp /app/app.jar cl.duoc.ventas.ScenarioProducer invalid
docker compose exec -T -e JAVA_TOOL_OPTIONS= consumer java -cp /app/app.jar cl.duoc.ventas.ScenarioProducer duplicate
docker compose exec -T -e JAVA_TOOL_OPTIONS= consumer java -cp /app/app.jar cl.duoc.ventas.ScenarioProducer late
```

| Modo | Eventos | Resultado esperado |
|---|---|---|
| `one-each` | una venta válida, una con `3 × 1000 ≠ 9999`, el reenvío de la válida y una con la ventana ya cerrada | exactamente 3 registros en la DLQ: `TOTAL_INCONSISTENTE`, `EVENT_ID_DUPLICADO`, `EVENTO_TARDIO` |
| `invalid` | JSON truncado, sin `eventId`, `quantity = 0`, `eventTime = "ayer"`, `3 × 1000 ≠ 9999` | 5 registros en la DLQ: `JSON_MALFORMADO`, `CAMPO_FALTANTE`, `CAMPO_INVALIDO`, `EVENT_TIME_INVALIDO`, `TOTAL_INCONSISTENTE` |
| `duplicate` | una venta válida y su reenvío con el mismo `eventId` | la primera se agrega; el reenvío va a la DLQ como `EVENT_ID_DUPLICADO` |
| `late` | evento de "ahora" (adelanta el stream time), uno dentro de la gracia y uno con la ventana ya cerrada | el segundo se suma a su ventana; el tercero va a la DLQ como `EVENTO_TARDIO` + log `[TARDIO_DESCARTADO]` |

A diferencia de `all`, estos tres modos usan `eventTime` relativo a "ahora" (no a un `T0` en el pasado): se pueden repetir
en cualquier momento, sin esperar 2 min y con el `producer` corriendo. Cada corrida usa `eventId` nuevos. Un modo
desconocido termina con código 2 e imprime el uso.

## Configuración (`.env`)

| Variable | Default | Uso |
|---|---|---|
| `WINDOW_SECONDS` | 60 | tamaño de la ventana |
| `WINDOW_GRACE_SECONDS` | 30 | período de gracia |
| `DEDUP_RETENTION_SECONDS` | 3600 | cuánto se recuerdan los `eventId` |
| `MAX_FUTURE_SKEW_SECONDS` | 300 | tolerancia de `eventTime` futuro |
| `GROUP_ID` | `ventas-aggregator-v2` | `application.id`; se cambió de v1 porque la topología cambió |

Al cambiar `GROUP_ID` quedan huérfanos los tópicos internos `ventas-aggregator-*` de la versión anterior; se
pueden borrar con `kafka-topics --delete`.
