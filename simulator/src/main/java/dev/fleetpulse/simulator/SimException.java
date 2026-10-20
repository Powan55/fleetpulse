package dev.fleetpulse.simulator;

import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;

class SimException extends RuntimeException {
    final HttpStatus status;
    final List<Map<String, String>> errors;

    SimException(HttpStatus status, String message) {
        this(status, message, List.of());
    }

    SimException(HttpStatus status, String message, List<Map<String, String>> errors) {
        super(message);
        this.status = status;
        this.errors = errors;
    }
}
