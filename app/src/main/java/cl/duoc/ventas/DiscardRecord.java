package cl.duoc.ventas;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/**
 * Sobre JSON que se escribe en la DLQ: conserva el valor original y explica el descarte.
 * Si el valor supera {@link #MAX_ORIGINAL_CHARS} se trunca y {@code originalChars} trae el largo real,
 * para que el sobre nunca exceda el máximo de mensaje del broker (si no, escribirlo detendría Streams).
 */
public record DiscardRecord(
        String sourceTopic,
        String key,
        String originalValue,
        String reason,
        String detail,
        String discardedAt,
        @JsonInclude(JsonInclude.Include.NON_NULL) Integer originalChars) {

    static final int MAX_ORIGINAL_CHARS = 64 * 1024;

    static String json(String sourceTopic, String key, String originalValue,
                       SaleValidator.Code reason, String detail, Instant at) {
        boolean truncate = originalValue != null && originalValue.length() > MAX_ORIGINAL_CHARS;
        return SaleValidator.toJson(new DiscardRecord(
                sourceTopic, key,
                truncate ? originalValue.substring(0, MAX_ORIGINAL_CHARS) : originalValue,
                reason.name(), detail, at.toString(),
                truncate ? originalValue.length() : null));
    }
}
