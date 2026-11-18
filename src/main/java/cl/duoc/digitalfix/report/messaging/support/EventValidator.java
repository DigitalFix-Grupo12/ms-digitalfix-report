package cl.duoc.digitalfix.report.messaging.support;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;

/** Valida el payload de un evento con Bean Validation antes de pasarlo al dominio. */
@Component
public class EventValidator {

    private final Validator validator;

    public EventValidator(Validator validator) {
        this.validator = validator;
    }

    /**
     * @throws InvalidEventException si el evento es nulo o viola alguna restriccion;
     *         el mensaje lista cada campo invalido (queda en el header de la DLT).
     */
    public <T> T requireValid(T event, Class<T> type) {
        if (event == null) {
            throw new InvalidEventException(type.getSimpleName() + " sin payload (valor nulo)");
        }
        Set<ConstraintViolation<T>> violations = validator.validate(event);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                    .sorted(Comparator.comparing(v -> v.getPropertyPath().toString()))
                    .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                    .collect(Collectors.joining("; "));
            throw new InvalidEventException(type.getSimpleName() + " invalido -> " + detail);
        }
        return event;
    }
}
