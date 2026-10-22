package dev.fleetpulse.sink;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@RestController
class SinkController {

    private static final String JSON = MediaType.APPLICATION_JSON_VALUE;

    record Mode(boolean outage, double errorRate, int delayMs) {}

    private final JsonMapper mapper = JsonMapper.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build();
    private final Store store;
    private final AtomicReference<Mode> mode = new AtomicReference<>(new Mode(false, 0, 0));

    SinkController(Store store) {
        this.store = store;
    }

    @PostMapping(value = "/webhooks/alerts", consumes = JSON)
    void receive(@RequestHeader(value = "Idempotency-Key", required = false) String key, @RequestBody byte[] body)
            throws InterruptedException {
        Mode m = mode.get();
        if (m.outage() || (m.errorRate() > 0 && ThreadLocalRandom.current().nextDouble() < m.errorRate())) {
            store.rejectInjected();
            throw new SinkException(HttpStatus.SERVICE_UNAVAILABLE, "injected failure");
        }
        JsonNode n;
        try {
            n = mapper.readTree(body);
        } catch (RuntimeException e) {
            store.rejectInvalid();
            throw new SinkException(HttpStatus.BAD_REQUEST, "body is not valid JSON");
        }
        List<String> errs = WebhookValidator.validate(n);
        if (errs.isEmpty() && (key == null || key.length() > 64 || !key.equals(n.get("idempotencyKey").stringValue()))) {
            errs = List.of("Idempotency-Key header must equal idempotencyKey");
        }
        if (!errs.isEmpty()) {
            store.rejectInvalid();
            throw new SinkException(HttpStatus.BAD_REQUEST, "invalid webhook",
                    errs.stream().map(s -> Map.of("field", "", "message", s)).toList());
        }
        store.record(key, n);
        if (m.delayMs() > 0) {
            Thread.sleep(m.delayMs());
        }
    }

    @GetMapping("/api/v1/mode")
    Mode getMode() {
        return mode.get();
    }

    @PutMapping(value = "/api/v1/mode", consumes = JSON)
    Mode putMode(@RequestBody byte[] body) {
        JsonNode n;
        try {
            n = mapper.readTree(body);
        } catch (RuntimeException e) {
            throw new SinkException(HttpStatus.BAD_REQUEST, "body is not valid JSON");
        }
        if (n == null || !n.isObject()) {
            throw new SinkException(HttpStatus.BAD_REQUEST, "body must be a JSON object");
        }
        List<Map<String, String>> errs = new java.util.ArrayList<>();
        for (String k : n.propertyNames()) {
            if (!List.of("outage", "errorRate", "delayMs").contains(k)) {
                errs.add(Map.of("field", k.length() > 64 ? k.substring(0, 64) + "..." : k, "message", "unknown field"));
            }
        }
        boolean outage = false;
        JsonNode o = n.get("outage");
        if (o != null) {
            if (o.isBoolean()) {
                outage = o.booleanValue();
            } else {
                errs.add(Map.of("field", "outage", "message", "must be a boolean"));
            }
        }
        double rate = 0;
        JsonNode r = n.get("errorRate");
        if (r != null) {
            if (r.isNumber() && r.doubleValue() >= 0 && r.doubleValue() <= 1) {
                rate = r.doubleValue();
            } else {
                errs.add(Map.of("field", "errorRate", "message", "must be a number between 0 and 1"));
            }
        }
        int delay = 0;
        JsonNode d = n.get("delayMs");
        if (d != null) {
            if (d.isIntegralNumber() && d.canConvertToInt() && d.intValue() >= 0 && d.intValue() <= 60000) {
                delay = d.intValue();
            } else {
                errs.add(Map.of("field", "delayMs", "message", "must be an integer between 0 and 60000"));
            }
        }
        if (!errs.isEmpty()) {
            throw new SinkException(HttpStatus.BAD_REQUEST, "invalid request", errs);
        }
        Mode m = new Mode(outage, rate, delay);
        mode.set(m);
        return m;
    }

    @GetMapping("/api/v1/deliveries/summary")
    Map<String, Long> summary() {
        return store.summary();
    }

    @GetMapping("/api/v1/deliveries/keys")
    List<Store.Key> keys() {
        return store.keys();
    }

    @GetMapping("/api/v1/deliveries")
    List<JsonNode> deliveries(@RequestParam(required = false) Long alertId,
                              @RequestParam(defaultValue = "100") int limit) {
        if ((alertId != null && alertId < 1) || limit < 1 || limit > 1000) {
            throw new SinkException(HttpStatus.BAD_REQUEST, "alertId must be >= 1 and limit between 1 and 1000");
        }
        return store.deliveries(alertId, limit);
    }

    @DeleteMapping("/api/v1/deliveries")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void reset() {
        store.reset();
    }
}
