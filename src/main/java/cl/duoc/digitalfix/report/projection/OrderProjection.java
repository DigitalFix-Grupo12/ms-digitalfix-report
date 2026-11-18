package cl.duoc.digitalfix.report.projection;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.Set;

/**
 * Read model de una orden de trabajo, mantenido por los eventos de
 * workorders.events. Report no consulta a workorders para calcular KPIs: lee
 * esta tabla, que se actualiza en cuanto llega cada evento.
 */
@Entity
@Table(name = "order_projection", indexes = {
        @Index(name = "idx_proj_status", columnList = "status"),
        @Index(name = "idx_proj_created", columnList = "created_at")
})
public class OrderProjection {

    private static final Set<String> FINALES = Set.of("CERRADA", "CANCELADA");

    /** Id de la orden en ms-digitalfix-workorders (= clave del mensaje Kafka). */
    @Id
    private Long id;

    @Column(nullable = false, length = 30)
    private String status;

    @Column(length = 255)
    private String customer;

    @Column(length = 100)
    private String technician;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "closed_at")
    private Instant closedAt;

    /** Fecha del ultimo hecho aplicado: base del control de orden y del SLA. */
    @Column(name = "last_change_at", nullable = false)
    private Instant lastChangeAt;

    @Column(name = "last_event_id", length = 64)
    private String lastEventId;

    /** KAFKA o SNAPSHOT (carga inicial desde workorders). */
    @Column(nullable = false, length = 10)
    private String source;

    @Column(name = "sla_at_risk", nullable = false)
    private boolean slaAtRisk;

    protected OrderProjection() {}

    public OrderProjection(Long id, String status, Instant createdAt, String source) {
        this.id = id;
        this.status = status;
        this.createdAt = createdAt;
        this.lastChangeAt = createdAt;
        this.source = source;
    }

    public boolean isActive() {
        return !FINALES.contains(status);
    }

    public Long getId() { return id; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getCustomer() { return customer; }
    public void setCustomer(String customer) { this.customer = customer; }
    public String getTechnician() { return technician; }
    public void setTechnician(String technician) { this.technician = technician; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getClosedAt() { return closedAt; }
    public void setClosedAt(Instant closedAt) { this.closedAt = closedAt; }
    public Instant getLastChangeAt() { return lastChangeAt; }
    public void setLastChangeAt(Instant lastChangeAt) { this.lastChangeAt = lastChangeAt; }
    public String getLastEventId() { return lastEventId; }
    public void setLastEventId(String lastEventId) { this.lastEventId = lastEventId; }
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    public boolean isSlaAtRisk() { return slaAtRisk; }
    public void setSlaAtRisk(boolean slaAtRisk) { this.slaAtRisk = slaAtRisk; }
}
