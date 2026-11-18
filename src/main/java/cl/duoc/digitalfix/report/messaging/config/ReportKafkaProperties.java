package cl.duoc.digitalfix.report.messaging.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Parametros de mensajeria Kafka de reporteria ({@code digitalfix.kafka.*}):
 * topico, consumer group, concurrencia, sufijo DLT y politica de reintentos.
 */
@Validated
@ConfigurationProperties(prefix = "digitalfix.kafka")
public record ReportKafkaProperties(
        boolean listenersEnabled,
        @NotBlank String consumerGroup,
        @Min(1) @Max(12) int concurrency,
        @Valid @NotNull Topics topics,
        @NotBlank String dltSuffix,
        @Valid @NotNull Retry retry) {

    public record Topics(@NotBlank String workorderEvents) {}

    public record Retry(
            @Min(0) @Max(10) int maxAttempts,
            @NotNull Duration initialInterval,
            @DecimalMin("1.0") double multiplier,
            @NotNull Duration maxInterval) {}

    /** DLT de este consumer group: {@code <topico>.<grupo>.DLT}. */
    public String dltFor(String topic) {
        return topic + "." + consumerGroup + dltSuffix;
    }
}
