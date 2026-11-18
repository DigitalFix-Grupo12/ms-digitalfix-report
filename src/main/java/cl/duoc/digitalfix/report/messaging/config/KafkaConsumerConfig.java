package cl.duoc.digitalfix.report.messaging.config;

import cl.duoc.digitalfix.report.messaging.event.WorkOrderEvent;
import cl.duoc.digitalfix.report.messaging.support.InvalidEventException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Cliente Kafka de reporteria: consumer group {@code report-kpis} sobre workorders.events.
 *
 * <pre>
 *  workorders.events (3 particiones) --(report-kpis, 3 hilos)--> WorkOrderEventsListener --> ProjectionService
 *                                    \-(audit-timeline)--------> ms-digitalfix-audit   (fan-out: otro grupo)
 *
 *  JSON ilegible / payload invalido -> DLT sin reintentar
 *  Falla transitoria (BD)           -> 3 reintentos 1s/2s/4s -> workorders.events.report-kpis.DLT
 * </pre>
 *
 * Con 3 hilos cada uno toma una particion; si se levanta otra instancia del mismo
 * grupo, Kafka rebalancea (CooperativeStickyAssignor) y se reparten las particiones.
 */
@Configuration
@EnableConfigurationProperties(ReportKafkaProperties.class)
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    private final ReportKafkaProperties props;
    private final ObjectMapper objectMapper;

    public KafkaConsumerConfig(ReportKafkaProperties props, ObjectMapper objectMapper) {
        this.props = props;
        this.objectMapper = objectMapper;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, WorkOrderEvent> workOrderEventListenerFactory(
            ConsumerFactory<Object, Object> baseConsumerFactory, DefaultErrorHandler reportErrorHandler) {

        JsonDeserializer<WorkOrderEvent> json = new JsonDeserializer<>(WorkOrderEvent.class, objectMapper, false);
        ConsumerFactory<String, WorkOrderEvent> consumerFactory = new DefaultKafkaConsumerFactory<>(
                baseConsumerFactory.getConfigurationProperties(),
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(json));

        ConcurrentKafkaListenerContainerFactory<String, WorkOrderEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(props.concurrency());
        factory.setCommonErrorHandler(reportErrorHandler);
        // Offset confirmado despues de actualizar la proyeccion: at-least-once + idempotencia por eventId
        factory.getContainerProperties().setAckMode(AckMode.RECORD);
        factory.getContainerProperties().setDeliveryAttemptHeader(true);
        return factory;
    }

    @Bean
    public DefaultErrorHandler reportErrorHandler(DeadLetterPublishingRecoverer reportDltRecoverer) {
        ReportKafkaProperties.Retry retry = props.retry();
        ExponentialBackOff backOff = new ExponentialBackOff(retry.initialInterval().toMillis(), retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());
        backOff.setMaxAttempts(retry.maxAttempts());

        DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) -> {
            log.error("Enviando a DLT {}-{}@{} -> {}: {}", record.topic(), record.partition(), record.offset(),
                    props.dltFor(record.topic()), rootMessage(ex));
            reportDltRecoverer.accept(record, unwrap(ex));
        }, backOff);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        handler.setRetryListeners((record, ex, attempt) ->
                log.warn("Fallo procesando {}-{}@{} (intento {}): {}", record.topic(), record.partition(),
                        record.offset(), attempt, rootMessage(ex)));
        return handler;
    }

    @Bean
    public DeadLetterPublishingRecoverer reportDltRecoverer(KafkaTemplate<String, Object> dltJsonTemplate,
                                                            KafkaTemplate<String, byte[]> dltBytesTemplate) {
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, dltBytesTemplate);
        templates.put(Object.class, dltJsonTemplate);
        return new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new TopicPartition(props.dltFor(record.topic()), record.partition()));
    }

    @Bean
    public KafkaTemplate<String, Object> dltJsonTemplate(ProducerFactory<Object, Object> baseProducerFactory) {
        JsonSerializer<Object> serializer = new JsonSerializer<Object>(objectMapper).noTypeInfo();
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                baseProducerFactory.getConfigurationProperties(), new StringSerializer(), serializer));
    }

    @Bean
    public KafkaTemplate<String, byte[]> dltBytesTemplate(ProducerFactory<Object, Object> baseProducerFactory) {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                baseProducerFactory.getConfigurationProperties(), new StringSerializer(), new ByteArraySerializer()));
    }

    private static Exception unwrap(Exception ex) {
        if (ex instanceof ListenerExecutionFailedException && ex.getCause() instanceof Exception cause) {
            return cause;
        }
        return ex;
    }

    private static String rootMessage(Throwable ex) {
        Throwable root = ex;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }
}
