package cl.duoc.digitalfix.report.messaging.support;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;

/**
 * Carga en el MDC el contexto del registro que se esta procesando (topico,
 * particion, offset, eventId, traceId) para que cada linea de log lo incluya.
 * Uso: {@code try (var ctx = KafkaLogContext.open(...)) { ... }}.
 */
public final class KafkaLogContext implements AutoCloseable {

    private static final String[] KEYS = {"topic", "partition", "offset", "eventId", "traceId"};

    private KafkaLogContext() {}

    public static KafkaLogContext open(ConsumerRecord<?, ?> record, String eventId, String traceId) {
        MDC.put("topic", record.topic());
        MDC.put("partition", String.valueOf(record.partition()));
        MDC.put("offset", String.valueOf(record.offset()));
        putIfPresent("eventId", eventId);
        putIfPresent("traceId", traceId);
        return new KafkaLogContext();
    }

    private static void putIfPresent(String key, String value) {
        if (value != null && !value.isBlank()) {
            MDC.put(key, value);
        }
    }

    @Override
    public void close() {
        for (String key : KEYS) {
            MDC.remove(key);
        }
    }
}
