package dev.fleetpulse.sink;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
class BodyLimit implements WebMvcConfigurer {

    static final long MAX_BODY = 64 * 1024;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new HandlerInterceptor() {
            @Override
            public boolean preHandle(HttpServletRequest req, HttpServletResponse res, Object handler) {
                String m = req.getMethod();
                if (!"POST".equals(m) && !"PUT".equals(m)) {
                    return true;
                }
                long len = req.getContentLengthLong();
                if (len > MAX_BODY) {
                    throw new SinkException(HttpStatus.CONTENT_TOO_LARGE, "request body over 65536 bytes");
                }
                if (len < 0 && req.getHeader("Transfer-Encoding") != null) {
                    throw new SinkException(HttpStatus.LENGTH_REQUIRED, "Content-Length required");
                }
                return true;
            }
        }).addPathPatterns("/api/**", "/webhooks/**");
    }
}
