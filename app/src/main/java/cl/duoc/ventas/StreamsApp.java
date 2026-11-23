package cl.duoc.ventas;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public final class StreamsApp {

    private static final Logger LOG = LoggerFactory.getLogger(StreamsApp.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String STATE_STORE = "ventas-por-tienda";
    private static final String DEDUP_STORE = "ventas-eventids-vistos";

    private record Validated(String original, SaleValidator.Result result) {
    }

    public static void main(String[] args) {
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        String rawTopic = env("RAW_TOPIC", "ventas-raw");
        String aggTopic = env("AGG_TOPIC", "ventas-agregadas");
        String dlqTopic = env("DLQ_TOPIC", "ventas-raw-dlq");
        String applicationId = env("GROUP_ID", "ventas-aggregator");
        long windowSeconds = Long.parseLong(env("WINDOW_SECONDS", "60"));
        long graceSeconds = Long.parseLong(env("WINDOW_GRACE_SECONDS", "30"));
        long dedupRetentionSeconds = Long.parseLong(env("DEDUP_RETENTION_SECONDS", "3600"));
        long maxFutureSkewSeconds = Long.parseLong(env("MAX_FUTURE_SKEW_SECONDS", "300"));

        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.STATE_DIR_CONFIG, env("STREAMS_STATE_DIR", "/tmp/kafka-streams"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var metrics = new BusinessMetrics();
        metrics.register();

        var streams = new KafkaStreams(
                buildTopology(rawTopic, aggTopic, dlqTopic, windowSeconds, graceSeconds,
                        dedupRetentionSeconds, Duration.ofSeconds(maxFutureSkewSeconds),
                        metrics, Clock.systemUTC()),
                props);
        // Última red: un fallo inesperado reemplaza el hilo (con log) en vez de dejar el consumidor detenido.
        streams.setUncaughtExceptionHandler(e -> {
            LOG.error("[STREAMS_ERROR] hilo de Streams falló; se reemplaza", e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        LOG.info("Kafka Streams app={} raw={} agg={} dlq={} ventana={}s gracia={}s dedup={}s futuro<={}s",
                applicationId, rawTopic, aggTopic, dlqTopic, windowSeconds, graceSeconds,
                dedupRetentionSeconds, maxFutureSkewSeconds);
        streams.start();
    }

    /**
     * raw -> validar (inválidos a DLQ) -> rekey por tienda -> repartition -> descartar tardíos y duplicados
     * (ambos a DLQ) -> ventana tumbling por eventTime -> suppress -> agregadas.
     */
    public static Topology buildTopology(
            String rawTopic, String aggTopic, String dlqTopic, long windowSeconds, long graceSeconds,
            long dedupRetentionSeconds, Duration maxFutureSkew, BusinessMetrics metrics, Clock clock) {
        return buildTopology(rawTopic, aggTopic, dlqTopic, windowSeconds, graceSeconds, dedupRetentionSeconds,
                maxFutureSkew, metrics, clock, new SaleValidator(clock, maxFutureSkew));
    }

    static Topology buildTopology(
            String rawTopic, String aggTopic, String dlqTopic, long windowSeconds, long graceSeconds,
            long dedupRetentionSeconds, Duration maxFutureSkew, BusinessMetrics metrics, Clock clock,
            SaleValidator validator) {
        StreamsBuilder builder = new StreamsBuilder();
        Duration window = Duration.ofSeconds(windowSeconds);
        Duration grace = Duration.ofSeconds(graceSeconds);
        Duration dedupRetention = Duration.ofSeconds(dedupRetentionSeconds);

        builder.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(DEDUP_STORE, dedupRetention, dedupRetention, false),
                Serdes.String(), Serdes.Long()));

        // 1. Validación: el timestamp del record es el eventTime del JSON (ver EventTimeExtractor).
        Map<String, KStream<String, Validated>> validated = builder
                .stream(rawTopic, Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(new EventTimeExtractor(clock, maxFutureSkew)))
                .mapValues(json -> new Validated(json, SaleValidator.validateSafely(validator, json)))
                .split(Named.as("validar-"))
                .branch((key, v) -> v.result().valid(), Branched.as("ok"))
                .defaultBranch(Branched.as("dlq"));

        validated.get("validar-dlq")
                .mapValues((key, v) -> {
                    var rejection = v.result().rejection();
                    metrics.invalid.incrementAndGet();
                    LOG.warn("[DLQ] topico={} key={} motivo={} detalle={} valor={} ({})",
                            rawTopic, key, rejection.code(), rejection.detail(), v.original(), metrics.summary());
                    return DiscardRecord.json(rawTopic, key, v.original(), rejection.code(),
                            rejection.detail(), clock.instant());
                })
                .to(dlqTopic, Produced.with(Serdes.String(), Serdes.String()));

        // 2. Tras el repartition por tienda el stream time es el mismo de la agregación.
        Map<String, KStream<String, DedupLateProcessor.Checked>> checked = validated.get("validar-ok")
                .map((key, v) -> {
                    Sale sale = v.result().sale();
                    return KeyValue.pair(sale.store(), sale);
                }, Named.as("rekey-por-tienda"))
                .repartition(Repartitioned.<String, Sale>as("por-tienda")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new JsonSerde<>(Sale.class)))
                .processValues(() -> new DedupLateProcessor(DEDUP_STORE, rawTopic, window, grace,
                        dedupRetention, clock, metrics), Named.as("tardios-y-duplicados"), DEDUP_STORE)
                .split(Named.as("dedup-"))
                .branch((key, c) -> c.dlqJson() != null, Branched.as("dup"))
                .defaultBranch(Branched.as("ok"));

        checked.get("dedup-dup")
                .mapValues(DedupLateProcessor.Checked::dlqJson)
                .to(dlqTopic, Produced.with(Serdes.String(), Serdes.String()));

        // 3. Ventana tumbling por tienda sobre eventTime; la gracia admite eventos fuera de orden.
        TimeWindows windows = TimeWindows.ofSizeAndGrace(window, grace);

        KTable<Windowed<String>, TicketAggregate> aggregate = checked.get("dedup-ok")
                .mapValues(DedupLateProcessor.Checked::sale)
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Sale.class)))
                .windowedBy(windows)
                .aggregate(
                        TicketAggregate::new,
                        (store, sale, totals) -> totals.add(sale),
                        Materialized.<String, TicketAggregate, WindowStore<Bytes, byte[]>>as(STATE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TicketAggregate.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        aggregate.toStream()
                .map((windowedKey, totals) -> KeyValue.pair(
                        windowedKey.key(), resultJson(windowedKey, totals)))
                .to(aggTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    private static String resultJson(Windowed<String> windowedKey, TicketAggregate totals) {
        long average = totals.count == 0 ? 0 : Math.round((double) totals.totalClp / totals.count);
        var result = new WindowResult(
                windowedKey.window().startTime().toString(),
                windowedKey.window().endTime().toString(),
                windowedKey.key(),
                totals.count,
                totals.totalClp,
                average);
        try {
            return MAPPER.writeValueAsString(result);
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo serializar el agregado", e);
        }
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
