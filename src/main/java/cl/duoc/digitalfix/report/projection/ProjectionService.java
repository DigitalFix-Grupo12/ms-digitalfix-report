package cl.duoc.digitalfix.report.projection;

import cl.duoc.digitalfix.report.live.LiveFeed;
import cl.duoc.digitalfix.report.messaging.event.WorkOrderEvent;
import cl.duoc.digitalfix.report.messaging.event.WorkOrderEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Aplica cada hecho de la orden al read model. Logica de dominio pura: no
 * conoce Kafka (recibe el evento ya validado y su posicion solo para trazarlo).
 *
 * <ul>
 *   <li>Idempotente: un eventId ya procesado se ignora.</li>
 *   <li>Tolerante al desorden: un evento mas antiguo que el ultimo aplicado no
 *       retrocede el estado (la clave = id de orden ya garantiza orden por particion,
 *       esto cubre re-procesos y la carga inicial).</li>
 *   <li>CLOSED fija closedAt = occurredAt: base del KPI de tiempo de resolucion.</li>
 *   <li>Todo cambio de estado limpia la alerta de SLA de la orden.</li>
 * </ul>
 */
@Service
public class ProjectionService {

    private static final Logger log = LoggerFactory.getLogger(ProjectionService.class);

    private final OrderProjectionRepository orders;
    private final ProcessedEventRepository processed;
    private final LiveFeed liveFeed;

    public ProjectionService(OrderProjectionRepository orders, ProcessedEventRepository processed, LiveFeed liveFeed) {
        this.orders = orders;
        this.processed = processed;
        this.liveFeed = liveFeed;
    }

    /** @return true si el evento cambio la proyeccion; false si era duplicado. */
    @Transactional
    public boolean apply(WorkOrderEvent e, EventPosition position) {
        if (processed.existsById(e.eventId())) {
            log.info("Evento duplicado ignorado (orden #{})", e.workOrderId());
            liveFeed.duplicate();
            return false;
        }

        OrderProjection order = orders.findById(e.workOrderId())
                .orElseGet(() -> new OrderProjection(e.workOrderId(), e.status(), e.occurredAt(), "KAFKA"));

        if (order.getLastChangeAt() != null && e.occurredAt().isBefore(order.getLastChangeAt())
                && e.eventType() != WorkOrderEventType.CREATED) {
            log.warn("Evento anterior al ultimo aplicado (orden #{}, {} < {}): no retrocede el estado",
                    e.workOrderId(), e.occurredAt(), order.getLastChangeAt());
        } else {
            order.setStatus(e.status());
            order.setLastChangeAt(e.occurredAt());
            order.setSlaAtRisk(false);
            if (e.eventType() == WorkOrderEventType.CLOSED) {
                order.setClosedAt(e.occurredAt());
            }
        }
        if (e.customer() != null) order.setCustomer(e.customer());
        if (e.technician() != null) order.setTechnician(e.technician());
        order.setLastEventId(e.eventId());
        order.setSource("KAFKA");

        orders.save(order);
        processed.save(new ProcessedEvent(e.eventId(), e.workOrderId(), e.eventType().name(), position));
        log.info("Proyeccion: orden #{} -> {} ({})", e.workOrderId(), order.getStatus(), e.eventType());

        afterCommit(() -> liveFeed.applied(e, position));
        return true;
    }

    private static void afterCommit(Runnable action) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    action.run();
                }
            });
        } else {
            action.run();
        }
    }
}
