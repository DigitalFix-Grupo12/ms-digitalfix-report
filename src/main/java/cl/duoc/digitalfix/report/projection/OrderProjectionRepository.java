package cl.duoc.digitalfix.report.projection;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

public interface OrderProjectionRepository extends JpaRepository<OrderProjection, Long> {

    /** Ordenes activas sin cambios desde antes de 'limit' que aun no estan marcadas en riesgo. */
    List<OrderProjection> findByStatusNotInAndLastChangeAtBeforeAndSlaAtRiskFalse(Collection<String> finals, Instant limit);

    List<OrderProjection> findBySlaAtRiskTrueOrderByLastChangeAtAsc();
}
