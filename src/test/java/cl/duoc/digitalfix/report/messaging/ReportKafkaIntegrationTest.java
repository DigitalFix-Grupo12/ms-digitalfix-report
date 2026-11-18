package cl.duoc.digitalfix.report.messaging;

import cl.duoc.digitalfix.report.projection.OrderProjection;
import cl.duoc.digitalfix.report.projection.OrderProjectionRepository;
import cl.duoc.digitalfix.report.projection.ProcessedEventRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Punta a punta contra un broker Kafka embebido: los eventos de workorders.events
 * mantienen la proyeccion, los KPIs y el panel en vivo, y los defectuosos van a la DLT.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "digitalfix.report.snapshot-on-startup=false",
        "digitalfix.report.sla-check-interval-ms=300",
        "digitalfix.kafka.retry.initial-interval=100ms",
        "digitalfix.kafka.retry.max-interval=200ms",
        "digitalfix.kafka.retry.max-attempts=1"
})
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = {
        ReportKafkaIntegrationTest.TOPIC,
        ReportKafkaIntegrationTest.DLT
})
class ReportKafkaIntegrationTest {

    static final String TOPIC = "workorders.events";
    static final String DLT = "workorders.events.report-kpis.DLT";
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Autowired private EmbeddedKafkaBroker broker;
    @Autowired private OrderProjectionRepository orders;
    @Autowired private ProcessedEventRepository processed;
    @Autowired private MockMvc mvc;

    private Producer<String, String> producer;

    @BeforeEach
    void setUp() {
        producer = new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker),
                new StringSerializer(), new StringSerializer()).createProducer();
    }

    @AfterEach
    void tearDown() {
        producer.close();
    }

    @Test
    void cicloDeVidaActualizaProyeccionKpisYPanelEnVivo() throws Exception {
        // Truncado a milisegundos: la BD guarda microsegundos y Windows entrega mas precision
        Instant t0 = Instant.now().minus(Duration.ofMinutes(90)).truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        send(501, event(UUID.randomUUID(), "CREATED", 501, "CREADA", null, t0, ""));
        send(501, event(UUID.randomUUID(), "ASSIGNED", 501, "ASIGNADA", "CREADA", t0.plusSeconds(600),
                ",\"technician\":\"tec-9\""));
        send(501, event(UUID.randomUUID(), "CLOSED", 501, "CERRADA", "EN_EJECUCION", t0.plusSeconds(4200), ""));

        await().atMost(TIMEOUT).until(() -> orders.findById(501L)
                .map(o -> "CERRADA".equals(o.getStatus())).orElse(false));
        OrderProjection o = orders.findById(501L).orElseThrow();
        assertThat(o.getTechnician()).isEqualTo("tec-9");
        assertThat(o.getClosedAt()).isEqualTo(t0.plusSeconds(4200));
        assertThat(o.getSource()).isEqualTo("KAFKA");

        mvc.perform(get("/api/report/kpis"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.porEstado.CERRADA").value(org.hamcrest.Matchers.greaterThanOrEqualTo(1)));
        mvc.perform(get("/api/report/live"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.consumerGroup").value("report-kpis"))
                .andExpect(jsonPath("$.eventosRecientes[*].workOrderId", hasItem(501)))
                .andExpect(jsonPath("$.consumo.eventosAplicados").value(org.hamcrest.Matchers.greaterThanOrEqualTo(3)));
    }

    @Test
    void eventoRepetidoSeAplicaUnaSolaVez() {
        UUID id = UUID.randomUUID();
        String payload = event(id, "CREATED", 502, "CREADA", null, Instant.now(), "");
        send(502, payload);
        send(502, payload);
        UUID marker = UUID.randomUUID();
        send(502, event(marker, "CANCELLED", 502, "CANCELADA", "CREADA", Instant.now().plusSeconds(1), ""));

        await().atMost(TIMEOUT).until(() -> processed.existsById(marker.toString()));
        assertThat(processed.findAll()).filteredOn(p -> p.getEventId().equals(id.toString())).hasSize(1);
        assertThat(orders.findById(502L).orElseThrow().getStatus()).isEqualTo("CANCELADA");
    }

    @Test
    void ordenDetenidaQuedaEnRiesgoYSeLimpiaAlAvanzar() {
        Instant hace2h = Instant.now().minus(Duration.ofHours(2));
        send(503, event(UUID.randomUUID(), "CREATED", 503, "CREADA", null, hace2h, ""));
        await().atMost(TIMEOUT).until(() -> orders.findById(503L).map(OrderProjection::isSlaAtRisk).orElse(false));

        send(503, event(UUID.randomUUID(), "ASSIGNED", 503, "ASIGNADA", "CREADA", Instant.now(),
                ",\"technician\":\"tec-1\""));
        await().atMost(TIMEOUT).until(() -> orders.findById(503L)
                .map(o -> "ASIGNADA".equals(o.getStatus()) && !o.isSlaAtRisk()).orElse(false));
    }

    @Test
    void payloadInvalidoVaALaDltDelGrupoReportKpis() {
        String invalid = "{\"eventId\":\"x-1\",\"eventType\":\"CLOSED\",\"schemaVersion\":1,"
                + "\"occurredAt\":\"2026-11-18T12:00:00Z\",\"source\":\"test\",\"workOrderId\":504}";
        send(504, invalid);

        Map<String, Object> config = KafkaTestUtils.consumerProps("dlt-" + UUID.randomUUID(), "false", broker);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(config,
                new StringDeserializer(), new StringDeserializer()).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(consumer, DLT);
            ConsumerRecord<String, String> dlt = KafkaTestUtils.getSingleRecord(consumer, DLT, TIMEOUT);
            assertThat(dlt.key()).isEqualTo("504");
            assertThat(header(dlt, KafkaHeaders.DLT_EXCEPTION_FQCN)).endsWith("InvalidEventException");
            assertThat(header(dlt, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("actor");
        }
        assertThat(orders.existsById(504L)).isFalse();
    }

    private void send(long key, String value) {
        producer.send(new ProducerRecord<>(TOPIC, String.valueOf(key), value));
        producer.flush();
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static String event(UUID eventId, String type, long orderId, String status, String previous,
                                Instant occurredAt, String extra) {
        return """
                {"eventId":"%s","eventType":"%s","schemaVersion":1,"occurredAt":"%s","traceId":"t-%d",
                 "source":"ms-digitalfix-workorders","workOrderId":%d,
                 "actor":{"id":"sup","username":"supervisor.demo@test","role":"Supervisor"},
                 "status":"%s"%s,"customer":"cliente.demo@test"%s}
                """.formatted(eventId, type, occurredAt, orderId, orderId, status,
                previous == null ? "" : ",\"previousStatus\":\"" + previous + "\"", extra);
    }
}
