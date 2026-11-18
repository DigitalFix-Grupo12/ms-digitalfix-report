package cl.duoc.digitalfix.report.messaging.support;

/**
 * Payload que no cumple el contrato del evento. No es recuperable: reintentarlo
 * daria el mismo resultado, por eso el manejador de errores lo envia directo a la DLT.
 */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message) {
        super(message);
    }
}
