package cl.duoc.digitalfix.report.controller;

import cl.duoc.digitalfix.report.service.KpiService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * KPIs de la red. API interna: no valida JWT (solo el BFF le habla); los
 * datos se calculan en linea desde ms-digitalfix-workorders.
 */
@RestController
@RequestMapping("/api/report")
public class ReportController {

    private final KpiService kpiService;

    public ReportController(KpiService kpiService) {
        this.kpiService = kpiService;
    }

    /** range = last24h (default) | last7d */
    @GetMapping("/kpis")
    public Map<String, Object> kpis(@RequestParam(defaultValue = "last24h") String range) {
        return kpiService.kpis(range);
    }
}
