package cl.duoc.digitalfix.report.live;

import cl.duoc.digitalfix.report.projection.OrderProjection;
import cl.duoc.digitalfix.report.projection.OrderProjectionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * Proceso derivado del streaming: revisa periodicamente la proyeccion y marca
 * "SLA en riesgo" las ordenes activas que no han cambiado de estado en
 * {@code digitalfix.report.sla-minutes}. El siguiente evento de esa orden
 * (cualquier avance) limpia la marca automaticamente (ver ProjectionService).
 */
@Component
public class SlaMonitor {

    private static final Logger log = LoggerFactory.getLogger(SlaMonitor.class);
    private static final Set<String> FINALES = Set.of("CERRADA", "CANCELADA");

    private final OrderProjectionRepository repository;
    private final LiveFeed liveFeed;
    private final Duration sla;

    public SlaMonitor(OrderProjectionRepository repository, LiveFeed liveFeed,
                      @Value("${digitalfix.report.sla-minutes:30}") long slaMinutes) {
        this.repository = repository;
        this.liveFeed = liveFeed;
        this.sla = Duration.ofMinutes(slaMinutes);
    }

    @Scheduled(fixedDelayString = "${digitalfix.report.sla-check-interval-ms:30000}", initialDelay = 10000)
    @Transactional
    public void check() {
        Instant now = Instant.now();
        List<OrderProjection> late = repository
                .findByStatusNotInAndLastChangeAtBeforeAndSlaAtRiskFalse(FINALES, now.minus(sla));
        for (OrderProjection o : late) {
            o.setSlaAtRisk(true);
            long minutes = Duration.between(o.getLastChangeAt(), now).toMinutes();
            log.warn("[SLA] orden #{} en riesgo: {} hace {} min (limite {} min)", o.getId(), o.getStatus(),
                    minutes, sla.toMinutes());
            liveFeed.alert(new LiveFeed.SlaAlert(now, o.getId(), o.getStatus(), o.getLastChangeAt(), minutes));
        }
    }

    public long slaMinutes() {
        return sla.toMinutes();
    }
}
