package dev.fleetpulse.api;

import java.util.List;
import org.springframework.http.HttpStatus;

public class ApiException extends RuntimeException {
    public record FieldError(String field, String message) {}

    final HttpStatus status;
    final List<FieldError> errors;
    Dto.Alert alert;

    ApiException(HttpStatus status, String message, List<FieldError> errors) {
        super(message);
        this.status = status;
        this.errors = errors;
    }

    static ApiException validation(List<FieldError> errors) {
        return new ApiException(HttpStatus.BAD_REQUEST, "Invalid request parameters", errors);
    }

    static ApiException notFound(String message) {
        return new ApiException(HttpStatus.NOT_FOUND, message, List.of());
    }

    static ApiException unsupportedMedia(String message) {
        return new ApiException(HttpStatus.UNSUPPORTED_MEDIA_TYPE, message, List.of());
    }

    static ApiException resolved(Dto.Alert alert) {
        ApiException e = new ApiException(HttpStatus.CONFLICT, "Alert is already resolved", List.of());
        e.alert = alert;
        return e;
    }

    static ApiException unavailable(String message) {
        return new ApiException(HttpStatus.SERVICE_UNAVAILABLE, message, List.of());
    }
}
