package cl.duoc.digitalfix.report.messaging.event;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.time.Instant;

/**
 * Evento de dominio del topico {@code workorders.events} (clave = workOrderId, para
 * que todos los eventos de una orden caigan en la misma particion y se lean en orden).
 *
 * <p>Envelope: eventId (idempotencia), eventType, schemaVersion (evolucion del
 * contrato; los campos desconocidos se ignoran), occurredAt (fecha del hecho, no
 * de la lectura), traceId (correlacion con los logs del productor) y source.
 * Payload: la orden, quien actuo y el cambio de estado.</p>
 */
public record WorkOrderEvent(
        @NotBlank @Size(max = 64) String eventId,
        @NotNull WorkOrderEventType eventType,
        @Min(1) int schemaVersion,
        @NotNull Instant occurredAt,
        @Size(max = 64) String traceId,
        @NotBlank @Size(max = 100) String source,
        @NotNull @Positive Long workOrderId,
        @NotNull @Valid Actor actor,
        @Size(max = 30) String status,
        @Size(max = 30) String previousStatus,
        @Size(max = 100) String technician,
        @Size(max = 100) String customer,
        @Size(max = 100) String serviceCode) {

    /** Reglas por tipo: un cambio de estado trae ambos estados y una asignacion trae tecnico. */
    @JsonIgnore
    @AssertTrue(message = "campos obligatorios segun eventType: STATUS_CHANGED requiere status y previousStatus; ASSIGNED requiere technician")
    public boolean isConsistentWithType() {
        if (eventType == null) {
            return true; // lo reporta @NotNull
        }
        return switch (eventType) {
            case STATUS_CHANGED -> notBlank(status) && notBlank(previousStatus);
            case ASSIGNED -> notBlank(technician);
            default -> true;
        };
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }
}
