package dev.fleetpulse.api;

import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@RestControllerAdvice
public class ProblemHandler extends ResponseEntityExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ProblemHandler.class);

    @ExceptionHandler(ApiException.class)
    ResponseEntity<Object> api(ApiException e, WebRequest req) {
        ProblemDetail p = ProblemDetail.forStatusAndDetail(e.status, e.getMessage());
        if (!e.errors.isEmpty()) p.setProperty("errors", e.errors);
        if (e.alert != null) p.setProperty("alert", e.alert);
        return handleExceptionInternal(e, p, new HttpHeaders(), e.status, req);
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<Object> other(Exception e, WebRequest req) {
        log.error("unhandled request error", e);
        return handleExceptionInternal(e, ProblemDetail.forStatus(HttpStatus.INTERNAL_SERVER_ERROR), new HttpHeaders(),
                HttpStatus.INTERNAL_SERVER_ERROR, req);
    }

    @Override
    protected ResponseEntity<Object> handleExceptionInternal(Exception ex, Object body, HttpHeaders headers,
                                                             HttpStatusCode status, WebRequest request) {
        if (body == null) body = ProblemDetail.forStatus(status);
        if (body instanceof ProblemDetail p) {
            p.setProperty("code", switch (status.value()) {
                case 400, 405, 411, 413 -> "VALIDATION_FAILED";
                case 404 -> "NOT_FOUND";
                case 409 -> "ALERT_RESOLVED";
                case 415 -> "UNSUPPORTED_MEDIA_TYPE";
                default -> "INTERNAL";
            });
            p.setType(URI.create("urn:fleetpulse:problem:" + p.getProperties().get("code").toString().toLowerCase().replace('_', '-')));
            if (p.getTitle() == null && status instanceof HttpStatus hs) p.setTitle(hs.getReasonPhrase());
        }
        return super.handleExceptionInternal(ex, body, headers, status, request);
    }
}
