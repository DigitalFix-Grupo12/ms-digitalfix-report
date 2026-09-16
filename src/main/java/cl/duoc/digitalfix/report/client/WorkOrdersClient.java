package cl.duoc.digitalfix.report.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;

/**
 * Lee las ordenes desde ms-digitalfix-workorders (dueño de los datos).
 * Report no tiene copia propia: en la evaluacion de streaming pasara a
 * mantener una proyeccion alimentada por eventos Kafka.
 */
@Component
public class WorkOrdersClient {

    /** Proyeccion minima que report necesita (ignora el resto de campos). */
    public record WorkOrderView(Long id, String status, Instant createdAt, Instant closedAt) {}

    private final RestClient client;

    public WorkOrdersClient(RestClient.Builder builder,
                            @Value("${digitalfix.services.workorders-url}") String baseUrl) {
        SimpleClientHttpRequestFactory rf = new SimpleClientHttpRequestFactory();
        rf.setConnectTimeout(2000);
        rf.setReadTimeout(5000);
        this.client = builder.baseUrl(baseUrl).requestFactory(rf).build();
    }

    public List<WorkOrderView> findAll() {
        try {
            List<WorkOrderView> list = client.get()
                .uri("/api/workorders")
                .retrieve()
                .body(new ParameterizedTypeReference<List<WorkOrderView>>() {});
            return list == null ? List.of() : list;
        } catch (RestClientException ex) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                "ms-digitalfix-workorders no disponible", ex);
        }
    }
}
