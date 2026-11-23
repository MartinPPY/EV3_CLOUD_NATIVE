package cl.duoc.ventas;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * Emite una secuencia determinista de eventos válidos y casos controlados, con eventTime explícito
 * relativo a T0 (inicio de una ventana alineada en el pasado). Cada paso imprime el resultado esperado.
 * Sin argumentos corre todo ({@code all}); también acepta {@code invalid}, {@code duplicate} o {@code late}
 * para provocar un solo tipo de caso, y {@code one-each} para uno de cada tema (estos usan eventTime relativo a "ahora").
 *
 * La key de cada record es la tienda: así los eventos de una tienda caen en una sola partición y
 * conservan su orden, que es lo que hace reproducibles los casos de tardíos. Para volver a ejecutarlo
 * hay que esperar ~2 min, o T0 quedaría detrás del stream time que dejó la corrida anterior.
 */
public final class ScenarioProducer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String STORE_A = "Paris Costanera Center";
    private static final String STORE_B = "Falabella Plaza Oeste";

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private int step = 0;

    private ScenarioProducer(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    private static final String USAGE = "uso: ScenarioProducer [all|invalid|duplicate|late|one-each]";

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "all";
        if (!Set.of("all", "invalid", "duplicate", "late", "one-each").contains(mode)) {
            System.err.println("modo desconocido: " + mode + "\n" + USAGE);
            System.exit(2);
        }
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = env("RAW_TOPIC", "ventas-raw");
        long w = Long.parseLong(env("WINDOW_SECONDS", "60"));
        long g = Long.parseLong(env("WINDOW_GRACE_SECONDS", "30"));

        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // T0 queda en el pasado para que ningún evento (ni el de cierre) sea "futuro".
        long now = Instant.now().getEpochSecond();
        Instant t0 = Instant.ofEpochSecond((now / w) * w - 4 * w - g);
        System.out.printf("escenario '%s' en '%s' via %s | ventana=%ds gracia=%ds | T0=%s%n", mode, topic, bootstrap, w, g, t0);

        try (var producer = new KafkaProducer<String, String>(props)) {
            var scenario = new ScenarioProducer(producer, topic);
            String run = UUID.randomUUID().toString().substring(0, 8);
            Instant ahora = Instant.ofEpochSecond(now);
            switch (mode) {
                case "invalid" -> {
                    scenario.invalidCases(run, sale(run + "-i0", STORE_A, ahora, 2, 10_000, 20_000), ahora);
                    System.out.println("fin: esperados 5 registros en la DLQ (JSON_MALFORMADO, CAMPO_FALTANTE, CAMPO_INVALIDO, EVENT_TIME_INVALIDO, TOTAL_INCONSISTENTE)");
                }
                case "one-each" -> scenario.oneEach(run, ahora, w, g);
                case "duplicate" -> scenario.duplicate(run, ahora);
                case "late" -> scenario.late(run, ahora, w, g);
                default -> scenario.run(t0, w, g);
            }
            producer.flush();
        }
    }

    /**
     * Inválidos: la validación ocurre antes que cualquier lógica de ventanas, así que no dependen del stream time
     * y se pueden repetir en cualquier momento. {@code valid} se usa (truncado) para el JSON malformado.
     */
    private void invalidCases(String run, String valid, Instant base) throws Exception {
        send(STORE_A, valid.substring(0, valid.length() / 2), "DLQ JSON_MALFORMADO");
        send(STORE_A, without(sale(run + "-a3", STORE_A, base, 1, 1_000, 1_000), "eventId"),
                "DLQ CAMPO_FALTANTE (eventId)");
        send(STORE_A, sale(run + "-a4", STORE_A, base.plusSeconds(1), 0, 1_000, 0),
                "DLQ CAMPO_INVALIDO (quantity=0)");
        send(STORE_A, sale(run + "-a5", STORE_A, base.plusSeconds(2), 1, 1_000, 1_000)
                .replace(base.plusSeconds(2).toString(), "ayer"), "DLQ EVENT_TIME_INVALIDO (eventTime=ayer)");
        send(STORE_A, sale(run + "-a6", STORE_A, base.plusSeconds(3), 3, 1_000, 9_999),
                "DLQ TOTAL_INCONSISTENTE (3 x 1000 != 9999)");
    }

    /**
     * Uno de cada tema: una venta válida (con eventTime = ahora, adelanta el stream time), un inválido, el reenvío
     * de la válida (duplicado) y un tardío con la ventana ya cerrada. Deja exactamente 3 registros en la DLQ.
     */
    private void oneEach(String run, Instant now, long w, long g) throws Exception {
        String valid = sale(run + "-u1", STORE_A, now, 1, 10_000, 10_000);
        send(STORE_A, valid, "OK  -> se agrega a la ventana actual de A (adelanta el stream time)");
        send(STORE_A, sale(run + "-u2", STORE_A, now, 3, 1_000, 9_999),
                "DLQ TOTAL_INCONSISTENTE (3 x 1000 != 9999)");
        send(STORE_A, valid, "DLQ EVENT_ID_DUPLICADO (reenvío de " + run + "-u1)");
        send(STORE_A, sale(run + "-u3", STORE_A, now.minusSeconds(2 * w + g), 1, 4_000, 4_000),
                "DLQ EVENTO_TARDIO (log [TARDIO_DESCARTADO]; ventana ya cerrada)");
        System.out.println("fin: esperados 3 registros en la DLQ (TOTAL_INCONSISTENTE, EVENT_ID_DUPLICADO, EVENTO_TARDIO)");
    }

    /** Duplicado: eventTime = ahora (ventana abierta), así no depende de corridas anteriores. */
    private void duplicate(String run, Instant now) throws Exception {
        String original = sale(run + "-d1", STORE_A, now, 1, 10_000, 10_000);
        send(STORE_A, original, "OK  -> se agrega a la ventana actual de A");
        send(STORE_A, original, "DLQ EVENT_ID_DUPLICADO (reenvío de " + run + "-d1)");
        System.out.println("fin: esperado 1 registro en la DLQ (EVENT_ID_DUPLICADO)");
    }

    /**
     * Tardíos: primero un evento con eventTime = ahora adelanta el stream time de la partición de A; luego uno
     * dentro de la gracia (se acepta) y otro con la ventana ya cerrada (DLQ). Funciona en cualquier momento, también
     * con el producer corriendo, porque todo se mide contra "ahora".
     */
    private void late(String run, Instant now, long w, long g) throws Exception {
        send(STORE_A, sale(run + "-l1", STORE_A, now, 1, 7_000, 7_000),
                "OK  -> ventana actual de A; adelanta el stream time de A hasta ahora");
        send(STORE_A, sale(run + "-l2", STORE_A, now.minusSeconds(g / 2), 1, 8_000, 8_000),
                "OK  -> TARDIO DENTRO DE GRACIA, se suma a su ventana");
        // fin de ventana <= ahora - w, así que fin + gracia < stream time.
        send(STORE_A, sale(run + "-l3", STORE_A, now.minusSeconds(2 * w + g), 1, 4_000, 4_000),
                "DLQ EVENTO_TARDIO (log [TARDIO_DESCARTADO]; ventana ya cerrada)");
        System.out.println("fin: esperado 1 registro en la DLQ (EVENTO_TARDIO)");
    }

    private void run(Instant t0, long w, long g) throws Exception {
        String run = UUID.randomUUID().toString().substring(0, 8);

        // --- Ventana W0 [T0, T0+w): válidos
        String a1 = sale(run + "-a1", STORE_A, t0.plusSeconds(5), 2, 10_000, 20_000);
        send(STORE_A, a1, "OK  -> W0 de A");
        send(STORE_B, sale(run + "-b1", STORE_B, t0.plusSeconds(10), 1, 5_000, 5_000), "OK  -> W0 de B");
        send(STORE_A, sale(run + "-a2", STORE_A, t0.plusSeconds(20), 1, 30_000, 30_000), "OK  -> W0 de A");

        // --- Inválidos: todos a la DLQ y el flujo continúa
        invalidCases(run, a1, t0.plusSeconds(25));

        // --- Duplicado de negocio: mismo eventId que a1
        send(STORE_A, a1, "DLQ EVENT_ID_DUPLICADO (reenvío de " + run + "-a1)");

        // --- Tardío dentro de gracia: llega un evento de W1 y luego uno de W0 (stream time < fin W0 + gracia)
        send(STORE_A, sale(run + "-a7", STORE_A, t0.plusSeconds(w + 10), 1, 7_000, 7_000), "OK  -> W1 de A");
        send(STORE_A, sale(run + "-a8", STORE_A, t0.plusSeconds(w - 10), 1, 8_000, 8_000),
                "OK  -> TARDIO DENTRO DE GRACIA, se suma a W0 de A");

        // --- Avanza el stream time de A hasta cerrar W0 (fin W0 + gracia)
        send(STORE_A, sale(run + "-a9", STORE_A, t0.plusSeconds(w + g + 10), 1, 9_000, 9_000),
                "OK  -> W1 de A; cierra W0 de A (emite: count=3, total=58000)");

        // --- Tardío después de la gracia: W0 ya está cerrada
        send(STORE_A, sale(run + "-a10", STORE_A, t0.plusSeconds(w - 5), 1, 4_000, 4_000),
                "DLQ EVENTO_TARDIO (log [TARDIO_DESCARTADO]; no cambia W0)");

        // --- Cierres: el stream time es por partición, hace falta un evento por tienda
        long close = 2 * w + g + 10;
        send(STORE_A, sale(run + "-a11", STORE_A, t0.plusSeconds(close), 1, 2_000, 2_000),
                "OK  -> W2 de A; cierra W1 de A (emite: count=2, total=16000)");
        send(STORE_B, sale(run + "-b2", STORE_B, t0.plusSeconds(close), 1, 2_000, 2_000),
                "OK  -> W2 de B; cierra W0 de B (emite: count=1, total=5000)");

        System.out.println("fin del escenario. Esperado en ventas-agregadas: A W0 (3, 58000), A W1 (2, 16000), B W0 (1, 5000)");
    }

    private void send(String key, String json, String expected) throws Exception {
        producer.send(new ProducerRecord<>(topic, key, json)).get();
        System.out.printf("[%02d] ESPERADO: %s%n     %s%n", ++step, expected, json);
    }

    private static String sale(String eventId, String store, Instant eventTime, int quantity, int unitPrice, int total)
            throws Exception {
        return MAPPER.writeValueAsString(new Sale(eventId, eventTime.toString(), store, "tienda", "Santiago",
                "presencial", "ropa", "Polera algodón", quantity, unitPrice, total, "cliente-0001", "débito"));
    }

    private static String without(String json, String field) throws Exception {
        ObjectNode node = (ObjectNode) MAPPER.readTree(json);
        node.remove(field);
        return MAPPER.writeValueAsString(node);
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
