package cl.duoc.digitalfix.report.projection;

import cl.duoc.digitalfix.report.client.WorkOrdersClient.WorkOrderView;
import cl.duoc.digitalfix.report.service.OrderViews;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/** Fuente de datos de los KPIs: la proyeccion local alimentada por Kafka. */
@Component
public class JpaOrderViews implements OrderViews {

    private final OrderProjectionRepository repository;

    public JpaOrderViews(OrderProjectionRepository repository) {
        this.repository = repository;
    }

    @Override
    @Transactional(readOnly = true)
    public List<WorkOrderView> findAll() {
        return repository.findAll().stream()
                .map(o -> new WorkOrderView(o.getId(), o.getStatus(), o.getCreatedAt(), o.getClosedAt()))
                .toList();
    }
}
