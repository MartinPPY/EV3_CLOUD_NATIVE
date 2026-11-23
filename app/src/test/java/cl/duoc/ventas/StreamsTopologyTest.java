package cl.duoc.ventas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamsTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String STORE = "Paris Online";

    // Ventana 60s, gracia 30s. t0 está alineado a minuto: W0=[t0,t0+60), W1=[t0+60,t0+120), ...
    private static final Instant NOW = Instant.parse("2026-01-01T12:00:00Z");
    private static final Instant T0 = Instant.parse("2026-01-01T11:50:00Z");

    private final BusinessMetrics metrics = new BusinessMetrics();
    private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
    private Properties props;
    private TopologyTestDriver driver;
    private TestInputTopic<String, String> raw;
    private TestOutputTopic<String, String> agg;
    private TestOutputTopic<String, String> dlq;

    @BeforeEach
    void setUp(@TempDir Path stateDir) {
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());

        open(new SaleValidator(clock, Duration.ofSeconds(300)));
    }

    private void open(SaleValidator validator) {
        if (driver != null) {
            driver.close();
        }
        var topology = StreamsApp.buildTopology("raw", "agg", "dlq", 60, 30, 3600,
                Duration.ofSeconds(300), metrics, clock, validator);
        driver = new TopologyTestDriver(topology, props);
        raw = driver.createInputTopic("raw", new StringSerializer(), new StringSerializer());
        agg = driver.createOutputTopic("agg", new StringDeserializer(), new StringDeserializer());
        dlq = driver.createOutputTopic("dlq", new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private static String sale(String eventId, Instant eventTime, int quantity, int unitPrice) {
        return SaleValidator.toJson(new Sale(eventId, eventTime.toString(), STORE, "online", "Santiago",
                "web", "ropa", "Polera", quantity, unitPrice, quantity * unitPrice, "cliente-0001", "débito"));
    }

    private void send(String eventId, Instant eventTime, int quantity, int unitPrice) {
        raw.pipeInput("cliente-0001", sale(eventId, eventTime, quantity, unitPrice), eventTime);
    }

    private static JsonNode json(String value) throws Exception {
        return MAPPER.readTree(value);
    }

    @Test
    void windowsUseEventTimeAndPublishWindowFields() throws Exception {
        // El timestamp del record (2020) no debe influir: manda el eventTime del JSON.
        raw.pipeInput("c1", sale("e1", T0.plusSeconds(5), 2, 10000), Instant.parse("2020-01-01T00:00:00Z"));
        raw.pipeInput("c2", sale("e2", T0.plusSeconds(20), 1, 30000), Instant.parse("2020-01-01T00:00:00Z"));
        assertTrue(agg.isEmpty(), "la ventana sigue abierta hasta fin + gracia");

        send("e3", T0.plusSeconds(100), 1, 1000); // stream time >= T0+60+30: cierra W0

        List<KeyValue<String, String>> out = agg.readKeyValuesToList();
        assertEquals(1, out.size());
        assertEquals(STORE, out.get(0).key);
        JsonNode result = json(out.get(0).value);
        assertEquals(T0.toString(), result.get("window_start").asText());
        assertEquals(T0.plusSeconds(60).toString(), result.get("window_end").asText());
        assertEquals(STORE, result.get("store").asText());
        assertEquals(2, result.get("sales_count").asLong());
        assertEquals(50000, result.get("total_clp").asLong());
        assertEquals(25000, result.get("avg_ticket_clp").asLong());
    }

    @Test
    void invalidEventGoesToDlqWithContextAndStreamContinues() throws Exception {
        String bad = "{\"eventId\":\"e-bad\",\"eventTi";
        raw.pipeInput("c9", bad, T0.plusSeconds(1));
        send("e1", T0.plusSeconds(5), 1, 1000);
        send("e2", T0.plusSeconds(100), 1, 1000);

        List<KeyValue<String, String>> dead = dlq.readKeyValuesToList();
        assertEquals(1, dead.size());
        assertEquals("c9", dead.get(0).key);
        JsonNode envelope = json(dead.get(0).value);
        assertEquals("raw", envelope.get("sourceTopic").asText());
        assertEquals("c9", envelope.get("key").asText());
        assertEquals(bad, envelope.get("originalValue").asText());
        assertEquals("JSON_MALFORMADO", envelope.get("reason").asText());
        assertEquals(1, metrics.invalid.get());
        assertEquals(2, metrics.processed.get());

        assertEquals(1, json(agg.readValue()).get("sales_count").asLong(), "el flujo siguió procesando");
    }

    @Test
    void inconsistentTotalGoesToDlq() throws Exception {
        String bad = sale("e1", T0.plusSeconds(5), 3, 1000).replace("\"totalClp\":3000", "\"totalClp\":9999");
        raw.pipeInput("c1", bad, T0.plusSeconds(5));

        assertEquals("TOTAL_INCONSISTENTE", json(dlq.readValue()).get("reason").asText());
        assertTrue(agg.isEmpty());
    }

    @Test
    void futureEventIsInvalidAndDoesNotAdvanceStreamTime() throws Exception {
        send("e1", T0.plusSeconds(5), 1, 1000);
        raw.pipeInput("c1", sale("e-future", NOW.plus(Duration.ofHours(1)), 1, 1000), T0.plusSeconds(6));

        assertEquals("EVENT_TIME_INVALIDO", json(dlq.readValue()).get("reason").asText());
        assertTrue(agg.isEmpty(), "un evento futuro no debe cerrar ventanas");
    }

    @Test
    void duplicateEventIdGoesToDlqAndIsCountedOnce() throws Exception {
        send("e1", T0.plusSeconds(5), 1, 1000);
        send("e1", T0.plusSeconds(5), 1, 1000);
        send("e2", T0.plusSeconds(100), 1, 1000);

        List<KeyValue<String, String>> dead = dlq.readKeyValuesToList();
        assertEquals(1, dead.size());
        assertEquals("EVENT_ID_DUPLICADO", json(dead.get(0).value).get("reason").asText());
        assertEquals(1, metrics.duplicate.get());
        assertEquals(2, metrics.processed.get(), "e1 y e2; el reenvío de e1 no cuenta");
        assertEquals(1, json(agg.readValue()).get("sales_count").asLong());
    }

    @Test
    void unexpectedValidatorFailureGoesToDlqAndStreamContinues() throws Exception {
        open(new SaleValidator(clock, Duration.ofSeconds(300)) {
            @Override
            public Result validate(String json) {
                if (json.contains("boom")) {
                    throw new IllegalStateException("fallo simulado");
                }
                return super.validate(json);
            }
        });
        raw.pipeInput("c9", "boom", T0.plusSeconds(1));
        send("e1", T0.plusSeconds(5), 1, 1000);
        send("e2", T0.plusSeconds(100), 1, 1000);

        JsonNode envelope = json(dlq.readValue());
        assertEquals("ERROR_VALIDACION", envelope.get("reason").asText());
        assertTrue(envelope.get("detail").asText().contains("fallo simulado"));
        assertEquals("boom", envelope.get("originalValue").asText());
        assertEquals(1, metrics.invalid.get());
        assertEquals(1, json(agg.readValue()).get("sales_count").asLong(), "el flujo siguió procesando");
    }

    @Test
    void oversizedInvalidValueIsTruncatedInDlqEnvelope() throws Exception {
        String huge = "x".repeat(DiscardRecord.MAX_ORIGINAL_CHARS + 500);
        raw.pipeInput("c9", huge, T0.plusSeconds(1));

        JsonNode envelope = json(dlq.readValue());
        assertEquals("JSON_MALFORMADO", envelope.get("reason").asText());
        assertEquals(DiscardRecord.MAX_ORIGINAL_CHARS, envelope.get("originalValue").asText().length());
        assertEquals(huge.length(), envelope.get("originalChars").asInt());
    }

    @Test
    void duplicateResentAfterGraceIsLateNotDuplicate() throws Exception {
        send("e1", T0.plusSeconds(5), 1, 1000);
        send("e2", T0.plusSeconds(100), 1, 1000);  // cierra W0
        send("e1", T0.plusSeconds(5), 1, 1000);    // reenvío de e1 con W0 ya cerrada

        assertEquals(1, metrics.late.get(), "el chequeo de tardío va antes que el de duplicado");
        assertEquals(0, metrics.duplicate.get());
        assertEquals("EVENTO_TARDIO", json(dlq.readValue()).get("reason").asText());
        assertTrue(dlq.isEmpty());
    }

    @Test
    void lateEventWithinGraceIsAddedToItsWindow() throws Exception {
        send("e1", T0.plusSeconds(5), 1, 1000);
        send("e2", T0.plusSeconds(70), 1, 1000);   // W1 adelanta el stream time a T0+70
        send("e3", T0.plusSeconds(50), 1, 1000);   // W0 sigue abierta: fin+gracia = T0+90 > T0+70
        send("e4", T0.plusSeconds(100), 1, 1000);  // cierra W0

        JsonNode w0 = json(agg.readValue());
        assertEquals(T0.toString(), w0.get("window_start").asText());
        assertEquals(2, w0.get("sales_count").asLong());
        assertEquals(0, metrics.late.get());
        assertEquals(4, metrics.processed.get());
        assertTrue(dlq.isEmpty());
    }

    @Test
    void lateEventAfterGraceIsDiscardedCountedAndSentToDlq() throws Exception {
        send("e1", T0.plusSeconds(5), 1, 1000);
        send("e2", T0.plusSeconds(100), 1, 1000);  // cierra W0
        send("e3", T0.plusSeconds(55), 1, 1000);   // W0 ya cerrada: tardío fuera de gracia
        send("e4", T0.plusSeconds(200), 1, 1000);  // cierra W1

        assertEquals(1, metrics.late.get());
        assertEquals(3, metrics.processed.get(), "e1, e2 y e4; el tardío e3 no cuenta");

        List<KeyValue<String, String>> dead = dlq.readKeyValuesToList();
        assertEquals(1, dead.size());
        assertEquals(STORE, dead.get(0).key, "la key original se pierde al reparticionar: es la tienda");
        JsonNode envelope = json(dead.get(0).value);
        assertEquals("EVENTO_TARDIO", envelope.get("reason").asText());
        assertEquals("raw", envelope.get("sourceTopic").asText());
        assertEquals("e3", json(envelope.get("originalValue").asText()).get("eventId").asText());

        List<KeyValue<String, String>> out = agg.readKeyValuesToList();
        assertEquals(2, out.size());
        assertEquals(T0.toString(), json(out.get(0).value).get("window_start").asText());
        assertEquals(1, json(out.get(0).value).get("sales_count").asLong(), "W0 no cambia por el tardío");
        assertEquals(T0.plusSeconds(60).toString(), json(out.get(1).value).get("window_start").asText());
    }
}
