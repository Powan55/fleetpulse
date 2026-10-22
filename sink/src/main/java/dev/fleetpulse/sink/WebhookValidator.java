package dev.fleetpulse.sink;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import tools.jackson.databind.JsonNode;

final class WebhookValidator {

    private static final Set<String> FIELDS = Set.of("schemaVersion", "idempotencyKey", "alertId", "alertVersion",
            "transition", "status", "deviceId", "ruleId", "backfilled", "occurredAt", "openedAt", "ackedAt",
            "resolvedAt", "resolvedBy", "triggerEventId", "conditionStartedEventTime");
    private static final List<String> REQUIRED = List.of("schemaVersion", "idempotencyKey", "alertId", "alertVersion",
            "transition", "status", "deviceId", "ruleId", "backfilled", "occurredAt", "openedAt");
    private static final Pattern KEY = Pattern.compile("^alert-[0-9]+-(opened|acked|resolved)$");
    private static final Pattern DEVICE = Pattern.compile("^dev-[0-9]{4,6}$");
    private static final Pattern UUID = Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private WebhookValidator() {}

    static List<String> validate(JsonNode n) {
        List<String> e = new ArrayList<>();
        if (n == null || !n.isObject()) {
            return List.of("body must be a JSON object");
        }
        for (String k : n.propertyNames()) {
            if (!FIELDS.contains(k)) {
                e.add("unknown field");
                break;
            }
        }
        for (String r : REQUIRED) {
            if (!n.has(r)) {
                e.add(r + " is required");
            }
        }
        if (!e.isEmpty()) {
            return e;
        }
        if (!(n.get("schemaVersion").isIntegralNumber() && n.get("schemaVersion").asLong() == 1)) {
            e.add("schemaVersion must be 1");
        }
        text(n, "idempotencyKey", KEY, e);
        integer(n, "alertId", 1, Long.MAX_VALUE, e);
        integer(n, "alertVersion", 1, 3, e);
        oneOf(n, "transition", Set.of("opened", "acked", "resolved"), false, e);
        oneOf(n, "status", Set.of("OPEN", "ACKED", "RESOLVED"), false, e);
        text(n, "deviceId", DEVICE, e);
        oneOf(n, "ruleId", Set.of("temperature_high", "battery_low", "error_burst", "device_offline", "device_failed"), false, e);
        if (!n.get("backfilled").isBoolean()) {
            e.add("backfilled must be a boolean");
        }
        dateTime(n, "occurredAt", false, e);
        dateTime(n, "openedAt", false, e);
        dateTime(n, "ackedAt", true, e);
        dateTime(n, "resolvedAt", true, e);
        dateTime(n, "conditionStartedEventTime", true, e);
        oneOf(n, "resolvedBy", Set.of("auto", "operator"), true, e);
        JsonNode t = n.get("triggerEventId");
        if (t != null && !t.isNull() && !(t.isString() && UUID.matcher(t.stringValue()).matches())) {
            e.add("triggerEventId must be a uuid or null");
        }
        return e;
    }

    private static void text(JsonNode n, String f, Pattern p, List<String> e) {
        JsonNode v = n.get(f);
        if (!v.isString() || !p.matcher(v.stringValue()).matches()) {
            e.add(f + " has the wrong format");
        }
    }

    private static void integer(JsonNode n, String f, long min, long max, List<String> e) {
        JsonNode v = n.get(f);
        if (!v.isIntegralNumber() || !v.canConvertToLong() || v.longValue() < min || v.longValue() > max) {
            e.add(f + " must be an integer between " + min + " and " + max);
        }
    }

    private static void oneOf(JsonNode n, String f, Set<String> allowed, boolean nullable, List<String> e) {
        JsonNode v = n.get(f);
        if (v == null || (nullable && v.isNull())) {
            return;
        }
        if (!v.isString() || !allowed.contains(v.stringValue())) {
            e.add(f + " has an unsupported value");
        }
    }

    private static void dateTime(JsonNode n, String f, boolean nullable, List<String> e) {
        JsonNode v = n.get(f);
        if (v == null || (nullable && v.isNull())) {
            return;
        }
        try {
            if (!v.isString()) {
                throw new IllegalArgumentException();
            }
            OffsetDateTime.parse(v.stringValue());
        } catch (RuntimeException ex) {
            e.add(f + " must be a date-time");
        }
    }
}
