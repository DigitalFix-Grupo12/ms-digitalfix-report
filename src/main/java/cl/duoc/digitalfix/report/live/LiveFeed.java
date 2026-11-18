package cl.duoc.digitalfix.report.live;

import cl.duoc.digitalfix.report.messaging.event.WorkOrderEvent;
import cl.duoc.digitalfix.report.projection.EventPosition;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Estado en memoria del consumo: ultimos eventos aplicados, alertas SLA
 * recientes y el ultimo offset procesado por particion. Lo lee el panel
 * "en vivo" del frontend para mostrar como reacciona el sistema a cada evento.
 */
@Component
public class LiveFeed {

    private static final int MAX_ITEMS = 20;

    public record LiveEvent(Instant receivedAt, Instant occurredAt, Long workOrderId, String eventType,
                            String status, String actor, String role, int partition, long offset) {}

    public record SlaAlert(Instant raisedAt, Long workOrderId, String status, Instant since, long minutes) {}

    private final Deque<LiveEvent> events = new ConcurrentLinkedDeque<>();
    private final Deque<SlaAlert> alerts = new ConcurrentLinkedDeque<>();
    private final Map<Integer, Long> lastOffsetByPartition = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicLong> countByPartition = new ConcurrentHashMap<>();
    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private volatile Instant lastEventAt;

    public void applied(WorkOrderEvent e, EventPosition p) {
        Instant now = Instant.now();
        events.addFirst(new LiveEvent(now, e.occurredAt(), e.workOrderId(), e.eventType().name(), e.status(),
                e.actor().username(), e.actor().role(), p.partition(), p.offset()));
        trim(events);
        lastOffsetByPartition.merge(p.partition(), p.offset(), Math::max);
        countByPartition.computeIfAbsent(p.partition(), k -> new AtomicLong()).incrementAndGet();
        applied.incrementAndGet();
        lastEventAt = now;
    }

    public void duplicate() {
        duplicates.incrementAndGet();
    }

    public void alert(SlaAlert alert) {
        alerts.addFirst(alert);
        trim(alerts);
    }

    public List<LiveEvent> recentEvents() {
        return new ArrayList<>(events);
    }

    public List<SlaAlert> recentAlerts() {
        return new ArrayList<>(alerts);
    }

    /** Eventos aplicados y ultimo offset por particion: muestra como se reparte el consumo. */
    public Map<String, Object> consumption() {
        Map<Integer, Map<String, Long>> partitions = new TreeMap<>();
        countByPartition.forEach((partition, count) -> partitions.put(partition,
                Map.of("eventos", count.get(), "ultimoOffset", lastOffsetByPartition.getOrDefault(partition, -1L))));
        return Map.of(
                "eventosAplicados", applied.get(),
                "duplicadosIgnorados", duplicates.get(),
                "ultimoEvento", lastEventAt == null ? "" : lastEventAt.toString(),
                "particiones", partitions);
    }

    private static <T> void trim(Deque<T> deque) {
        while (deque.size() > MAX_ITEMS) {
            deque.pollLast();
        }
    }
}
