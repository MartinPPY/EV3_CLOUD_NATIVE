package cl.duoc.ventas;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SaleValidatorTest {

    static final Instant NOW = Instant.parse("2026-01-01T12:00:00Z");

    private final SaleValidator validator = new SaleValidator(
            Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(300));

    static String json(String eventTime, Object quantity, Object unitPrice, Object total) {
        return """
                {"eventId":"e-1","eventTime":%s,"store":"Paris Online","storeFormat":"online","city":"Santiago",
                 "channel":"web","category":"ropa","product":"Polera","quantity":%s,"unitPriceClp":%s,
                 "totalClp":%s,"customerId":"cliente-0001","paymentMethod":"débito"}"""
                .formatted(eventTime, quantity, unitPrice, total);
    }

    private static String time(Instant instant) {
        return "\"" + instant + "\"";
    }

    private void assertRejected(SaleValidator.Code code, String json) {
        var result = validator.validate(json);
        assertTrue(!result.valid(), "debía rechazarse: " + json);
        assertEquals(code, result.rejection().code(), result.rejection().detail());
    }

    @Test
    void acceptsValidSale() {
        var result = validator.validate(json(time(NOW.minusSeconds(10)), 2, 10000, 20000));
        assertTrue(result.valid());
        assertNull(result.rejection());
        assertEquals("Paris Online", result.sale().store());
    }

    @Test
    void rejectsMalformedJson() {
        assertRejected(SaleValidator.Code.JSON_MALFORMADO, "{\"eventId\":\"e-1\",\"eventTi");
        assertRejected(SaleValidator.Code.JSON_MALFORMADO, "esto-no-es-json");
        assertRejected(SaleValidator.Code.JSON_MALFORMADO, "");
        assertRejected(SaleValidator.Code.JSON_MALFORMADO, "null");
        assertRejected(SaleValidator.Code.JSON_MALFORMADO,
                json(time(NOW), 1, 100, 100) + " basura");
    }

    @Test
    void rejectsScalarCoercion() {
        assertRejected(SaleValidator.Code.JSON_MALFORMADO, json(time(NOW), "\"5\"", 100, 500));
        assertRejected(SaleValidator.Code.JSON_MALFORMADO, json(time(NOW), 5.7, 100, 500));
    }

    @Test
    void rejectsMissingFields() {
        assertRejected(SaleValidator.Code.CAMPO_FALTANTE,
                json(time(NOW), 1, 100, 100).replace("\"eventId\":\"e-1\",", ""));
        assertRejected(SaleValidator.Code.CAMPO_FALTANTE,
                json(time(NOW), 1, 100, 100).replace("\"store\":\"Paris Online\"", "\"store\":\" \""));
        assertRejected(SaleValidator.Code.CAMPO_FALTANTE,
                json(time(NOW), 1, 100, 100).replace("\"quantity\":1,", ""));
    }

    @Test
    void rejectsInvalidNumbers() {
        assertRejected(SaleValidator.Code.CAMPO_INVALIDO, json(time(NOW), 0, 100, 0));
        assertRejected(SaleValidator.Code.CAMPO_INVALIDO, json(time(NOW), -1, 100, -100));
        assertRejected(SaleValidator.Code.CAMPO_INVALIDO, json(time(NOW), 1, 0, 0));
    }

    @Test
    void rejectsInvalidEventTime() {
        assertRejected(SaleValidator.Code.EVENT_TIME_INVALIDO, json("\"ayer\"", 1, 100, 100));
        assertRejected(SaleValidator.Code.EVENT_TIME_INVALIDO,
                json(time(NOW.plus(Duration.ofHours(1))), 1, 100, 100));
    }

    @Test
    void acceptsEventTimeWithinFutureSkew() {
        assertTrue(validator.validate(json(time(NOW.plusSeconds(60)), 1, 100, 100)).valid());
    }

    @Test
    void rejectsInconsistentTotal() {
        assertRejected(SaleValidator.Code.TOTAL_INCONSISTENTE, json(time(NOW), 3, 1000, 9999));
    }
}
