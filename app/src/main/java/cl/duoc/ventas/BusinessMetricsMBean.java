package cl.duoc.ventas;

/** Contadores de negocio publicados por JMX; el JMX Prometheus javaagent los expone como ventas_*_total. */
public interface BusinessMetricsMBean {

    /** Ventas válidas, no tardías ni duplicadas, que entran a la agregación. */
    long getProcesadas();

    /** Eventos rechazados por validación (van a la DLQ). */
    long getInvalidas();

    /** Eventos con eventId repetido (van a la DLQ). */
    long getDuplicadas();

    /** Eventos fuera del período de gracia: no se agregan y van a la DLQ. */
    long getTardias();

    /** Total de registros enviados a la DLQ: inválidos + duplicados + tardíos. */
    long getEnviadasDlq();
}
