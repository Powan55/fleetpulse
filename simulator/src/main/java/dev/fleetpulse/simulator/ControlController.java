package dev.fleetpulse.simulator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@RestController
@RequestMapping("/api/v1")
class ControlController {

    private static final String JSON = MediaType.APPLICATION_JSON_VALUE;
    private static final Set<String> KINDS = Set.of("invalid_json", "missing_field", "bad_type", "oversize");

    private final JsonMapper mapper = JsonMapper.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build();
    private final Fleet fleet;

    ControlController(Fleet fleet) {
        this.fleet = fleet;
    }

    @GetMapping("/chaos")
    Chaos getChaos() {
        return fleet.chaos();
    }

    @PutMapping(value = "/chaos", consumes = JSON)
    Chaos putChaos(@RequestBody byte[] body) {
        Errors err = new Errors();
        JsonNode n = parse(body);
        err.onlyKeys(n, "", Set.of("duplicateRate", "dropRate", "reorderRate", "skewSets", "replayOnReconnect",
                "replayMaxRate"));
        double dup = err.rate(n, "duplicateRate");
        double drop = err.rate(n, "dropRate");
        double reorder = err.rate(n, "reorderRate");
        List<Chaos.Skew> skews = new ArrayList<>();
        JsonNode ss = n.get("skewSets");
        if (ss != null) {
            if (!ss.isArray() || ss.size() > 4) {
                err.add("skewSets", "must be an array of at most 4 items");
            } else {
                double sum = 0;
                for (int i = 0; i < ss.size(); i++) {
                    JsonNode s = ss.get(i);
                    String f = "skewSets[" + i + "]";
                    if (!s.isObject()) {
                        err.add(f, "must be an object");
                        continue;
                    }
                    err.onlyKeys(s, f, Set.of("fraction", "offsetMs"));
                    if (!s.has("fraction") || !s.has("offsetMs")) {
                        err.add(f, "fraction and offsetMs are required");
                    }
                    double fr = err.rate(s, "fraction", f + ".fraction");
                    long off = s.has("offsetMs") ? err.integer(s.get("offsetMs"), f + ".offsetMs", -3_600_000, 3_600_000) : 0;
                    sum += fr;
                    skews.add(new Chaos.Skew(fr, off));
                }
                if (sum > 1 + 1e-9) {
                    err.add("skewSets", "fractions must sum to at most 1");
                }
            }
        }
        boolean replay = true;
        JsonNode r = n.get("replayOnReconnect");
        if (r != null) {
            if (r.isBoolean()) {
                replay = r.booleanValue();
            } else {
                err.add("replayOnReconnect", "must be a boolean");
            }
        }
        Long max = null;
        JsonNode m = n.get("replayMaxRate");
        if (m != null && !m.isNull()) {
            max = err.integer(m, "replayMaxRate", 1, 2000);
        }
        err.check();
        Chaos c = new Chaos(dup, drop, reorder, List.copyOf(skews), replay, max);
        fleet.setChaos(c);
        return c;
    }

    @PostMapping(value = "/chaos/malformed", consumes = JSON)
    @ResponseStatus(HttpStatus.ACCEPTED)
    Map<String, Object> malformed(@RequestBody byte[] body) {
        Errors err = new Errors();
        JsonNode n = parse(body);
        err.onlyKeys(n, "", Set.of("count", "kind"));
        if (!n.has("count") || !n.has("kind")) {
            err.add("", "count and kind are required");
        }
        long count = n.has("count") ? err.integer(n.get("count"), "count", 1, 1000) : 0;
        JsonNode k = n.get("kind");
        String kind = k != null && k.isString() ? k.stringValue() : null;
        if (k != null && (kind == null || !KINDS.contains(kind))) {
            err.add("kind", "must be one of " + String.join(", ", new java.util.TreeSet<>(KINDS)));
        }
        err.check();
        return Map.of("sent", fleet.injectMalformed(kind, (int) count));
    }

    @GetMapping("/generation")
    Map<String, Boolean> getGeneration() {
        return Map.of("paused", fleet.paused());
    }

    @PutMapping(value = "/generation", consumes = JSON)
    Map<String, Boolean> putGeneration(@RequestBody byte[] body) {
        Errors err = new Errors();
        JsonNode n = parse(body);
        err.onlyKeys(n, "", Set.of("paused"));
        JsonNode p = n.get("paused");
        if (p == null || !p.isBoolean()) {
            err.add("paused", "required boolean");
        }
        err.check();
        fleet.setPaused(p.booleanValue());
        return Map.of("paused", fleet.paused());
    }

    @GetMapping("/counters")
    Map<String, Object> counters() {
        return fleet.counters.view(fleet.ledger.total());
    }

    @GetMapping("/ledger")
    Map<String, Object> ledger() {
        List<Map<String, Object>> devs = new ArrayList<>();
        for (int i = 0; i < fleet.deviceCount(); i++) {
            devs.add(device(i));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("seed", fleet.seed());
        out.put("epoch", fleet.epoch());
        out.put("asOf", Instant.now().toString());
        out.put("totalAccepted", fleet.ledger.total());
        out.put("devices", devs);
        return out;
    }

    @GetMapping("/ledger/devices/{deviceId}")
    Map<String, Object> ledgerDevice(@PathVariable String deviceId) {
        if (!deviceId.matches("dev-[0-9]{4,6}")) {
            throw new SimException(HttpStatus.BAD_REQUEST, "invalid device id");
        }
        int i = fleet.indexOf(deviceId);
        if (i < 0) {
            throw new SimException(HttpStatus.NOT_FOUND, "unknown device");
        }
        Map<String, Object> out = device(i);
        out.put("acceptedSeqRanges", fleet.ledger.ranges(i));
        return out;
    }

    private Map<String, Object> device(int i) {
        Ledger.Entry e = fleet.ledger.entry(i);
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("deviceId", fleet.deviceId(i));
        d.put("epoch", fleet.epoch());
        d.put("acceptedCount", e.count());
        d.put("maxAcceptedSeq", e.maxSeq());
        d.put("lastAckAt", e.lastAckAt() == null ? null : e.lastAckAt().toString());
        return d;
    }

    private JsonNode parse(byte[] body) {
        JsonNode n;
        try {
            n = mapper.readTree(body);
        } catch (RuntimeException e) {
            throw new SimException(HttpStatus.BAD_REQUEST, "body is not valid JSON");
        }
        if (n == null || !n.isObject()) {
            throw new SimException(HttpStatus.BAD_REQUEST, "body must be a JSON object");
        }
        return n;
    }

    private static final class Errors {
        private final List<Map<String, String>> list = new ArrayList<>();

        void add(String field, String message) {
            list.add(Map.of("field", field, "message", message));
        }

        void onlyKeys(JsonNode n, String path, Set<String> allowed) {
            for (String k : n.propertyNames()) {
                if (!allowed.contains(k)) {
                    String shown = k.length() > 64 ? k.substring(0, 64) + "..." : k;
                    add(path.isEmpty() ? shown : path + "." + shown, "unknown field");
                }
            }
        }

        double rate(JsonNode n, String key) {
            return rate(n, key, key);
        }

        double rate(JsonNode n, String key, String field) {
            JsonNode v = n.get(key);
            if (v == null) {
                return 0;
            }
            if (!v.isNumber() || v.doubleValue() < 0 || v.doubleValue() > 1) {
                add(field, "must be a number between 0 and 1");
                return 0;
            }
            return v.doubleValue();
        }

        long integer(JsonNode v, String field, long min, long max) {
            if (!v.isIntegralNumber() || !v.canConvertToLong() || v.longValue() < min || v.longValue() > max) {
                add(field, "must be an integer between " + min + " and " + max);
                return 0;
            }
            return v.longValue();
        }

        void check() {
            if (!list.isEmpty()) {
                throw new SimException(HttpStatus.BAD_REQUEST, "invalid request", list);
            }
        }
    }
}
