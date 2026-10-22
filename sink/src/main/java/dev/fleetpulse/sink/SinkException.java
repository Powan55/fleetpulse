package dev.fleetpulse.sink;

import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;

class SinkException extends RuntimeException {
    final HttpStatus status;
    final List<Map<String, String>> errors;

    SinkException(HttpStatus status, String message) {
        this(status, message, List.of());
    }

    SinkException(HttpStatus status, String message, List<Map<String, String>> errors) {
        super(message);
        this.status = status;
        this.errors = errors;
    }
}
