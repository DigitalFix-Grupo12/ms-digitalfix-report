package cl.duoc.digitalfix.report;

import cl.duoc.digitalfix.report.client.WorkOrdersClient;
import cl.duoc.digitalfix.report.client.WorkOrdersClient.WorkOrderView;
import cl.duoc.digitalfix.report.service.KpiService;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Calculo de KPIs con el cliente de workorders simulado (sin red). */
class KpiServiceTest {

    private final WorkOrdersClient client = mock(WorkOrdersClient.class);
    private final KpiService service = new KpiService(client, "America/Santiago");

    @Test
    @SuppressWarnings("unchecked")
    void calculaKpisDesdeLasOrdenes() {
        Instant now = Instant.now();
        when(client.findAll()).thenReturn(List.of(
            new WorkOrderView(1L, "CREADA", now.minus(Duration.ofHours(1)), null),
            new WorkOrderView(2L, "EN_EJECUCION", now.minus(Duration.ofHours(3)), null),
            new WorkOrderView(3L, "CERRADA", now.minus(Duration.ofHours(5)), now.minus(Duration.ofHours(4))),
            new WorkOrderView(4L, "CANCELADA", now.minus(Duration.ofDays(3)), null)
        ));

        Map<String, Object> k = service.kpis("last24h");

        assertEquals(4, k.get("totalOrdenes"));
        assertEquals(3, k.get("ordenesEnRango"));
        assertEquals(2L, k.get("estadosActivos"));
        assertEquals(60L, k.get("tiempoResolucionPromedioMin"));
        List<Map<String, Object>> serie = (List<Map<String, Object>>) k.get("serie");
        assertEquals(6, serie.size());
        int total = serie.stream().mapToInt(p -> ((Number) p.get("ordenes")).intValue()).sum();
        assertEquals(3, total);
    }

    @Test
    @SuppressWarnings("unchecked")
    void serieSemanalTieneSieteDias() {
        when(client.findAll()).thenReturn(List.of());
        Map<String, Object> k = service.kpis("last7d");
        assertEquals(7, ((List<Map<String, Object>>) k.get("serie")).size());
        assertEquals(0L, k.get("tiempoResolucionPromedioMin"));
    }

    @Test
    void rangoInvalidoDevuelve400() {
        ResponseStatusException ex = assertThrows(ResponseStatusException.class, () -> service.kpis("ayer"));
        assertEquals(HttpStatus.BAD_REQUEST, ex.getStatusCode());
    }
}
