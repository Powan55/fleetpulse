package dev.fleetpulse.api;

import java.util.List;
import org.springframework.http.HttpStatus;

public class ApiException extends RuntimeException {
    public record FieldError(String field, String message) {}

    final HttpStatus status;
    final List<FieldError> errors;

    ApiException(HttpStatus status, String message, List<FieldError> errors) {
        super(message);
        this.status = status;
        this.errors = errors;
    }

    static ApiException validation(List<FieldError> errors) {
        return new ApiException(HttpStatus.BAD_REQUEST, "Invalid request parameters", errors);
    }

    static ApiException unavailable(String message) {
        return new ApiException(HttpStatus.SERVICE_UNAVAILABLE, message, List.of());
    }
}
