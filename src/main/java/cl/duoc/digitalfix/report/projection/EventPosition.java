package cl.duoc.digitalfix.report.projection;

/** Posicion de un evento en el log de Kafka (para trazarlo en Kafka UI). */
public record EventPosition(String topic, int partition, long offset) {}
