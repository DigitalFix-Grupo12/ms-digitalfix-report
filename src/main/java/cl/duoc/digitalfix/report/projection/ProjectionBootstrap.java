package cl.duoc.digitalfix.report.projection;

import cl.duoc.digitalfix.report.client.WorkOrdersClient;
import cl.duoc.digitalfix.report.client.WorkOrdersClient.WorkOrderView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

/**
 * Carga inicial (snapshot) del read model: al arrancar, las ordenes que existian
 * antes de que hubiera eventos (p. ej. datos de demo) se copian una vez desde
 * workorders. Desde ahi, solo los eventos de Kafka actualizan la proyeccion.
 * No pisa ordenes que ya llegaron por Kafka; si workorders no responde, se omite.
 */
@Component
public class ProjectionBootstrap {

    private static final Logger log = LoggerFactory.getLogger(ProjectionBootstrap.class);

    private final WorkOrdersClient workOrders;
    private final OrderProjectionRepository repository;
    private final TransactionTemplate tx;
    private final boolean enabled;

    public ProjectionBootstrap(WorkOrdersClient workOrders, OrderProjectionRepository repository,
                               TransactionTemplate tx,
                               @Value("${digitalfix.report.snapshot-on-startup:true}") boolean enabled) {
        this.workOrders = workOrders;
        this.repository = repository;
        this.tx = tx;
        this.enabled = enabled;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void loadSnapshot() {
        if (!enabled) {
            return;
        }
        List<WorkOrderView> views;
        try {
            views = workOrders.findAll();
        } catch (RuntimeException ex) {
            log.warn("Snapshot inicial omitido (workorders no disponible): {}", ex.getMessage());
            return;
        }
        Integer added = tx.execute(status -> {
            int count = 0;
            for (WorkOrderView v : views) {
                if (v.id() == null || v.createdAt() == null || repository.existsById(v.id())) continue;
                OrderProjection p = new OrderProjection(v.id(), v.status(), v.createdAt(), "SNAPSHOT");
                p.setClosedAt(v.closedAt());
                if (v.closedAt() != null) p.setLastChangeAt(v.closedAt());
                repository.save(p);
                count++;
            }
            return count;
        });
        log.info("Snapshot inicial: {} ordenes cargadas desde workorders (luego solo eventos Kafka)", added);
    }
}
