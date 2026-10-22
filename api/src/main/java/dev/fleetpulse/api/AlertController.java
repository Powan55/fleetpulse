package dev.fleetpulse.api;

import dev.fleetpulse.api.Dto.Alert;
import dev.fleetpulse.api.Dto.AlertPage;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.InvalidMediaTypeException;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@RestController
@RequestMapping("/api/v1/alerts")
public class AlertController {
    private static final Logger log = LoggerFactory.getLogger(AlertController.class);
    private static final Pattern DEVICE_ID = Pattern.compile("^dev-[0-9]{4,6}$");
    private static final List<String> STATUSES = List.of("OPEN", "ACKED", "RESOLVED");
    private static final List<String> RULES = List.of("temperature_high", "battery_low", "error_burst", "device_offline", "device_failed");
    private static final JsonMapper JSON = JsonMapper.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();

    private final AlertRepository alerts;

    public AlertController(AlertRepository alerts) {
        this.alerts = alerts;
    }

    @GetMapping
    AlertPage list(@RequestParam(required = false) String status, @RequestParam(required = false) String deviceId,
                   @RequestParam(required = false) String ruleId, @RequestParam(defaultValue = "0") int page,
                   @RequestParam(defaultValue = "50") int size) {
        List<ApiException.FieldError> errors = new ArrayList<>();
        List<String> statuses = new ArrayList<>();
        if (status != null) {
            for (String s : status.split(",", -1)) {
                if (!STATUSES.contains(s)) {
                    errors.add(new ApiException.FieldError("status", "must be a comma-separated subset of " + STATUSES));
                    break;
                }
                if (!statuses.contains(s)) statuses.add(s);
            }
            if (statuses.size() > 3) errors.add(new ApiException.FieldError("status", "at most 3 values"));
        }
        if (deviceId != null && !DEVICE_ID.matcher(deviceId).matches()) errors.add(new ApiException.FieldError("deviceId", "must match ^dev-[0-9]{4,6}$"));
        if (ruleId != null && !RULES.contains(ruleId)) errors.add(new ApiException.FieldError("ruleId", "must be one of " + RULES));
        if (page < 0 || page > 10000) errors.add(new ApiException.FieldError("page", "must be between 0 and 10000"));
        if (size < 1 || size > 200) errors.add(new ApiException.FieldError("size", "must be between 1 and 200"));
        if (!errors.isEmpty()) throw ApiException.validation(errors);
        return alerts.list(statuses, deviceId, ruleId, page, size);
    }

    @GetMapping("/{alertId}")
    Alert get(@PathVariable String alertId) {
        return alerts.get(id(alertId)).orElseThrow(() -> ApiException.notFound("Alert not found"));
    }

    @PostMapping("/{alertId}/ack")
    Alert ack(@PathVariable String alertId, HttpServletRequest req) throws IOException {
        long id = id(alertId);
        String note = body(req);
        Alert a = alerts.ack(id);
        logNote("ack", id, note);
        return a;
    }

    @PostMapping("/{alertId}/resolve")
    Alert resolve(@PathVariable String alertId, HttpServletRequest req) throws IOException {
        long id = id(alertId);
        String note = body(req);
        Alert a = alerts.resolve(id);
        logNote("resolve", id, note);
        return a;
    }

    private static void logNote(String action, long id, String note) {
        if (note != null) log.info("alert {} {} note={}", id, action, JSON.writeValueAsString(note));
    }

    private static long id(String raw) {
        try {
            long id = Long.parseLong(raw);
            if (id >= 1) return id;
        } catch (NumberFormatException e) {
            // falls through
        }
        throw ApiException.validation(List.of(new ApiException.FieldError("alertId", "must be an integer >= 1")));
    }

    /** Returns the optional note. Missing body or non-JSON content type is 415. */
    private static String body(HttpServletRequest req) throws IOException {
        boolean json = false;
        try {
            String ct = req.getContentType();
            json = ct != null && MediaType.APPLICATION_JSON.equalsTypeAndSubtype(MediaType.parseMediaType(ct));
        } catch (InvalidMediaTypeException e) {
            // not json
        }
        if (!json) throw ApiException.unsupportedMedia("Content-Type must be application/json");
        byte[] raw = req.getInputStream().readNBytes((int) BodyLimit.MAX_BODY + 1);
        if (raw.length == 0) throw ApiException.unsupportedMedia("A JSON body is required, send {}");
        JsonNode root;
        try {
            root = JSON.readTree(raw);
        } catch (RuntimeException e) {
            throw bad("body is not valid JSON");
        }
        if (root == null || !root.isObject()) throw bad("body must be a JSON object");
        String note = null;
        for (String k : root.propertyNames()) {
            if (!k.equals("note")) throw bad("unknown property " + k.substring(0, Math.min(k.length(), 40)));
            JsonNode n = root.get(k);
            if (!n.isString() || n.stringValue().length() > 200) throw bad("note must be a string of at most 200 characters");
            note = n.stringValue();
        }
        return note;
    }

    private static ApiException bad(String msg) {
        return ApiException.validation(List.of(new ApiException.FieldError("body", msg)));
    }
}
