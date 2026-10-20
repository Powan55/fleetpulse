package dev.fleetpulse.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.HandlerExceptionResolver;

@Component
class BodyLimit extends OncePerRequestFilter {
    static final long MAX_BODY = 64 * 1024;

    private final HandlerExceptionResolver resolver;

    BodyLimit(@Qualifier("handlerExceptionResolver") HandlerExceptionResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String m = req.getMethod();
        if ("POST".equals(m) || "PUT".equals(m) || "PATCH".equals(m)) {
            long len = req.getContentLengthLong();
            if (len > MAX_BODY) {
                resolver.resolveException(req, res, null,
                        new ApiException(HttpStatus.CONTENT_TOO_LARGE, "request body over 65536 bytes", List.of()));
                return;
            }
            if (len < 0 && req.getHeader("Transfer-Encoding") != null) {
                resolver.resolveException(req, res, null,
                        new ApiException(HttpStatus.LENGTH_REQUIRED, "Content-Length required", List.of()));
                return;
            }
        }
        chain.doFilter(req, res);
    }
}
