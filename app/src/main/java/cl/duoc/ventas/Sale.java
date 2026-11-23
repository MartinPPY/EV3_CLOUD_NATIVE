package cl.duoc.ventas;

// Los numéricos son Integer (no int) para distinguir "campo ausente" (null) de un 0 legítimo.
public record Sale(
        String eventId,
        String eventTime,
        String store,
        String storeFormat,
        String city,
        String channel,
        String category,
        String product,
        Integer quantity,
        Integer unitPriceClp,
        Integer totalClp,
        String customerId,
        String paymentMethod) {
}
