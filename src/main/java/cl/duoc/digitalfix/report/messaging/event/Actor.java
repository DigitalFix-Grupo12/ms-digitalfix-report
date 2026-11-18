package cl.duoc.digitalfix.report.messaging.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Quien origino el evento (usuario autenticado en Entra ID o servicio interno).
 *
 * @param id       identificador estable del usuario (claim oid) o nombre del servicio
 * @param username UPN o correo; es lo que se muestra en el timeline
 * @param role     rol con el que actuo (Admin, Supervisor, Cliente, Auditor, SYSTEM)
 */
public record Actor(
        @NotBlank @Size(max = 100) String id,
        @NotBlank @Size(max = 255) String username,
        @Size(max = 30) String role) {}
