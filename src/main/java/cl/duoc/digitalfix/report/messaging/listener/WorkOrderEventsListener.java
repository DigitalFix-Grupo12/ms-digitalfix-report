package cl.duoc.digitalfix.report.messaging.listener;

import cl.duoc.digitalfix.report.messaging.event.WorkOrderEvent;
import cl.duoc.digitalfix.report.messaging.support.EventValidator;
import cl.duoc.digitalfix.report.messaging.support.KafkaLogContext;
import cl.duoc.digitalfix.report.projection.EventPosition;
import cl.duoc.digitalfix.report.projection.ProjectionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor de {@code workorders.events} en el grupo {@code report-kpis}.
 * Solo adapta: valida el payload y delega en la proyeccion. Cada evento
 * actualiza el read model de inmediato, por eso los KPIs reflejan el cambio
 * en cuanto el BFF los vuelve a consultar (sin llamar a workorders).
 */
@Component
public class WorkOrderEventsListener {

    private final EventValidator validator;
    private final ProjectionService projection;

    public WorkOrderEventsListener(EventValidator validator, ProjectionService projection) {
        this.validator = validator;
        this.projection = projection;
    }

    @KafkaListener(
            id = "report-workorder-events",
            topics = "${digitalfix.kafka.topics.workorder-events}",
            groupId = "${digitalfix.kafka.consumer-group}",
            containerFactory = "workOrderEventListenerFactory",
            autoStartup = "${digitalfix.kafka.listeners-enabled:true}")
    public void onWorkOrderEvent(ConsumerRecord<String, WorkOrderEvent> record) {
        WorkOrderEvent event = record.value();
        try (var ignored = KafkaLogContext.open(record,
                event != null ? event.eventId() : null,
                event != null ? event.traceId() : null)) {
            validator.requireValid(event, WorkOrderEvent.class);
            projection.apply(event, new EventPosition(record.topic(), record.partition(), record.offset()));
        }
    }
}
