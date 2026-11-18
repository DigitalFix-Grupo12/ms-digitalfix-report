package cl.duoc.digitalfix.report.live;

import cl.duoc.digitalfix.report.projection.OrderProjectionRepository;
import cl.duoc.digitalfix.report.service.KpiService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Vista "en vivo" para el panel del frontend (refresco cada pocos segundos):
 * KPIs recalculados desde la proyeccion, ultimos eventos consumidos, ordenes
 * con SLA en riesgo y como se esta repartiendo el consumo por particion.
 */
@Service
public class LiveService {

    private final KpiService kpis;
    private final LiveFeed feed;
    private final OrderProjectionRepository repository;
    private final SlaMonitor sla;
    private final String consumerGroup;
    private final String topic;

    public LiveService(KpiService kpis, LiveFeed feed, OrderProjectionRepository repository, SlaMonitor sla,
                       @Value("${digitalfix.kafka.consumer-group}") String consumerGroup,
                       @Value("${digitalfix.kafka.topics.workorder-events}") String topic) {
        this.kpis = kpis;
        this.feed = feed;
        this.repository = repository;
        this.sla = sla;
        this.consumerGroup = consumerGroup;
        this.topic = topic;
    }

    @Transactional(readOnly = true)
    public Map<String, Object> snapshot() {
        List<Map<String, Object>> enRiesgo = repository.findBySlaAtRiskTrueOrderByLastChangeAtAsc().stream()
                .map(o -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("ordenId", o.getId());
                    m.put("estado", o.getStatus());
                    m.put("desde", o.getLastChangeAt().toString());
                    m.put("tecnico", o.getTechnician() == null ? "" : o.getTechnician());
                    return m;
                })
                .toList();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("fuente", "kafka:" + topic);
        out.put("consumerGroup", consumerGroup);
        out.put("kpis", kpis.kpis("last24h"));
        out.put("eventosRecientes", feed.recentEvents());
        out.put("slaMinutos", sla.slaMinutes());
        out.put("ordenesEnRiesgo", enRiesgo);
        out.put("alertasRecientes", feed.recentAlerts());
        out.put("consumo", feed.consumption());
        return out;
    }
}
