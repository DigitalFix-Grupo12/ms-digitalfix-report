package cl.duoc.digitalfix.report.projection;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Registro de idempotencia: cada eventId se aplica a la proyeccion una sola vez
 * (el consumo es at-least-once: tras un rebalanceo o un reintento puede repetirse).
 */
@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @Column(name = "event_id", length = 64)
    private String eventId;

    @Column(name = "work_order_id", nullable = false)
    private Long workOrderId;

    @Column(name = "event_type", nullable = false, length = 30)
    private String eventType;

    @Column(nullable = false, length = 120)
    private String topic;

    @Column(name = "kafka_partition", nullable = false)
    private int partition;

    @Column(name = "kafka_offset", nullable = false)
    private long offset;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedEvent() {}

    public ProcessedEvent(String eventId, Long workOrderId, String eventType, EventPosition position) {
        this.eventId = eventId;
        this.workOrderId = workOrderId;
        this.eventType = eventType;
        this.topic = position.topic();
        this.partition = position.partition();
        this.offset = position.offset();
        this.processedAt = Instant.now();
    }

    public String getEventId() { return eventId; }
    public Long getWorkOrderId() { return workOrderId; }
    public String getEventType() { return eventType; }
    public int getPartition() { return partition; }
    public long getOffset() { return offset; }
    public Instant getProcessedAt() { return processedAt; }
}
