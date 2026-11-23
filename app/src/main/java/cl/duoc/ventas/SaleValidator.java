package cl.duoc.ventas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/** Parsea el JSON crudo a {@link Sale} y aplica las reglas de negocio. */
public class SaleValidator {

    public enum Code {
        JSON_MALFORMADO,
        CAMPO_FALTANTE,
        CAMPO_INVALIDO,
        EVENT_TIME_INVALIDO,
        TOTAL_INCONSISTENTE,
        EVENT_ID_DUPLICADO,
        EVENTO_TARDIO,
        ERROR_VALIDACION
    }

    public record Rejection(Code code, String detail) {
    }

    /** Exactamente uno de los dos campos es no nulo. */
    public record Result(Sale sale, Rejection rejection) {
        public boolean valid() {
            return sale != null;
        }
    }

    // Estricto: "5" o 5.7 no se aceptan como int, y no se toleran datos tras el JSON.
    static final ObjectMapper MAPPER = JsonMapper.builder()
            .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build();

    private final Clock clock;
    private final Duration maxFutureSkew;

    public SaleValidator(Clock clock, Duration maxFutureSkew) {
        this.clock = clock;
        this.maxFutureSkew = maxFutureSkew;
    }

    public Result validate(String json) {
        if (json == null) {
            return reject(Code.JSON_MALFORMADO, "valor nulo");
        }
        Sale sale;
        try {
            sale = MAPPER.readValue(json, Sale.class);
        } catch (JsonProcessingException e) {
            return reject(Code.JSON_MALFORMADO, e.getOriginalMessage());
        }
        if (sale == null) {
            return reject(Code.JSON_MALFORMADO, "el JSON es null");
        }

        String[][] required = {
                {"eventId", sale.eventId()}, {"eventTime", sale.eventTime()}, {"store", sale.store()},
                {"storeFormat", sale.storeFormat()}, {"city", sale.city()}, {"channel", sale.channel()},
                {"category", sale.category()}, {"product", sale.product()},
                {"customerId", sale.customerId()}, {"paymentMethod", sale.paymentMethod()}};
        for (String[] field : required) {
            if (field[1] == null || field[1].isBlank()) {
                return reject(Code.CAMPO_FALTANTE, field[0] + " ausente o vacío");
            }
        }
        Object[][] numbers = {
                {"quantity", sale.quantity()}, {"unitPriceClp", sale.unitPriceClp()},
                {"totalClp", sale.totalClp()}};
        for (Object[] field : numbers) {
            if (field[1] == null) {
                return reject(Code.CAMPO_FALTANTE, field[0] + " ausente");
            }
        }

        Instant eventTime = parseEventTime(sale.eventTime());
        if (eventTime == null) {
            return reject(Code.EVENT_TIME_INVALIDO, "eventTime no es ISO-8601: " + sale.eventTime());
        }
        if (eventTime.isAfter(clock.instant().plus(maxFutureSkew))) {
            return reject(Code.EVENT_TIME_INVALIDO,
                    "eventTime en el futuro (más de " + maxFutureSkew.toSeconds() + "s): " + sale.eventTime());
        }

        if (sale.quantity() <= 0) {
            return reject(Code.CAMPO_INVALIDO, "quantity debe ser > 0: " + sale.quantity());
        }
        if (sale.unitPriceClp() <= 0) {
            return reject(Code.CAMPO_INVALIDO, "unitPriceClp debe ser > 0: " + sale.unitPriceClp());
        }
        long expected = (long) sale.quantity() * sale.unitPriceClp();
        if (sale.totalClp() != expected) {
            return reject(Code.TOTAL_INCONSISTENTE,
                    "totalClp=" + sale.totalClp() + " != quantity*unitPriceClp=" + expected);
        }
        return new Result(sale, null);
    }

    static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("No se pudo serializar " + value.getClass().getSimpleName(), e);
        }
    }

    /** Instant del evento, o null si no es ISO-8601. */
    static Instant parseEventTime(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException | NullPointerException e) {
            return null;
        }
    }

    /** Como {@link #validate}, pero un fallo inesperado se convierte en rechazo para no detener el stream. */
    static Result validateSafely(SaleValidator validator, String json) {
        try {
            return validator.validate(json);
        } catch (RuntimeException e) {
            return reject(Code.ERROR_VALIDACION, e.toString());
        }
    }

    private static Result reject(Code code, String detail) {
        return new Result(null, new Rejection(code, detail));
    }
}
