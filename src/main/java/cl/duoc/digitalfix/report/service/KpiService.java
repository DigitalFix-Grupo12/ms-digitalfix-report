package cl.duoc.digitalfix.report.service;

import cl.duoc.digitalfix.report.client.WorkOrdersClient;
import cl.duoc.digitalfix.report.client.WorkOrdersClient.WorkOrderView;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/** Calcula KPIs a partir de las ordenes reales de ms-digitalfix-workorders. */
@Service
public class KpiService {

    private static final Set<String> FINALES = Set.of("CERRADA", "CANCELADA");
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("dd-MM");

    private final WorkOrdersClient workOrders;
    private final ZoneId zone;
    private final Clock clock;

    public KpiService(WorkOrdersClient workOrders,
                      @Value("${digitalfix.report.zone:America/Santiago}") String zone) {
        this.workOrders = workOrders;
        this.zone = ZoneId.of(zone);
        this.clock = Clock.system(this.zone);
    }

    public Map<String, Object> kpis(String range) {
        int hours = switch (range) {
            case "last24h" -> 24;
            case "last7d" -> 24 * 7;
            default -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                "range inválido: use last24h o last7d");
        };

        List<WorkOrderView> all = workOrders.findAll();
        Instant now = clock.instant();
        Instant from = now.minus(Duration.ofHours(hours));

        List<WorkOrderView> enRango = all.stream()
            .filter(o -> o.createdAt() != null && !o.createdAt().isBefore(from))
            .toList();

        double ordenesPorHora = Math.round(enRango.size() * 10.0 / hours) / 10.0;

        long tiempoResolucionPromedioMin = Math.round(all.stream()
            .filter(o -> o.closedAt() != null && !o.closedAt().isBefore(from) && o.createdAt() != null)
            .mapToLong(o -> Duration.between(o.createdAt(), o.closedAt()).toMinutes())
            .average()
            .orElse(0));

        long estadosActivos = all.stream().filter(o -> !FINALES.contains(o.status())).count();

        Map<String, Long> porEstado = all.stream()
            .collect(Collectors.groupingBy(WorkOrderView::status, TreeMap::new, Collectors.counting()));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("range", range);
        out.put("generadoEn", now.toString());
        out.put("totalOrdenes", all.size());
        out.put("ordenesEnRango", enRango.size());
        out.put("ordenesPorHora", ordenesPorHora);
        out.put("tiempoResolucionPromedioMin", tiempoResolucionPromedioMin);
        out.put("estadosActivos", estadosActivos);
        out.put("porEstado", porEstado);
        out.put("serie", hours == 24 ? serieHoras(enRango, now) : serieDias(enRango, now));
        return out;
    }

    /** 6 franjas de 4 horas (hora local) de las ultimas 24h. */
    private List<Map<String, Object>> serieHoras(List<WorkOrderView> orders, Instant now) {
        int[] counts = new int[6];
        orders.forEach(o -> counts[o.createdAt().atZone(zone).getHour() / 4]++);
        List<Map<String, Object>> serie = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            serie.add(Map.of("label", "%02d-%02d".formatted(i * 4, i * 4 + 4), "ordenes", counts[i]));
        }
        return serie;
    }

    /** Un punto por dia (hora local) de los ultimos 7 dias. */
    private List<Map<String, Object>> serieDias(List<WorkOrderView> orders, Instant now) {
        LocalDate today = now.atZone(zone).toLocalDate();
        Map<LocalDate, Long> byDay = orders.stream()
            .collect(Collectors.groupingBy(o -> o.createdAt().atZone(zone).toLocalDate(), Collectors.counting()));
        List<Map<String, Object>> serie = new ArrayList<>();
        for (int i = 6; i >= 0; i--) {
            LocalDate d = today.minusDays(i);
            serie.add(Map.of("label", d.format(DAY), "ordenes", byDay.getOrDefault(d, 0L)));
        }
        return serie;
    }
}
