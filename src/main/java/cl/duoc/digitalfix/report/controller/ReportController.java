package cl.duoc.digitalfix.report.controller;

import cl.duoc.digitalfix.report.live.LiveService;
import cl.duoc.digitalfix.report.service.KpiService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * KPIs de la red. API interna: no valida JWT (solo el BFF le habla). Los datos
 * salen de la proyeccion alimentada por Kafka (workorders.events, grupo report-kpis).
 */
@RestController
@RequestMapping("/api/report")
public class ReportController {

    private final KpiService kpiService;
    private final LiveService liveService;

    public ReportController(KpiService kpiService, LiveService liveService) {
        this.kpiService = kpiService;
        this.liveService = liveService;
    }

    /** range = last24h (default) | last7d */
    @GetMapping("/kpis")
    public Map<String, Object> kpis(@RequestParam(defaultValue = "last24h") String range) {
        return kpiService.kpis(range);
    }

    /** Panel en vivo: KPIs + ultimos eventos consumidos + SLA en riesgo + consumo por particion. */
    @GetMapping("/live")
    public Map<String, Object> live() {
        return liveService.snapshot();
    }
}
