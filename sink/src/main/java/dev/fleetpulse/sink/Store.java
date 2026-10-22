package dev.fleetpulse.sink;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

@Component
class Store {

    record Key(String key, long count, Instant firstReceivedAt, Instant lastReceivedAt, long alertId, int alertVersion) {}

    private static final class Entry {
        final long alertId;
        final int alertVersion;
        final Instant first;
        Instant last;
        long count;

        Entry(long alertId, int alertVersion, Instant now) {
            this.alertId = alertId;
            this.alertVersion = alertVersion;
            this.first = now;
        }
    }

    private final int payloadLimit;
    private final Map<String, Entry> keys = new LinkedHashMap<>();
    private final ArrayDeque<JsonNode> payloads = new ArrayDeque<>();
    private long recorded;
    private long rejectedInjected;
    private long rejectedInvalid;

    Store(@Value("${sink.payload-limit:10000}") int payloadLimit) {
        this.payloadLimit = payloadLimit;
    }

    synchronized void record(String key, JsonNode body) {
        Instant now = Instant.now();
        Entry e = keys.computeIfAbsent(key, k -> new Entry(body.get("alertId").longValue(), body.get("alertVersion").intValue(), now));
        e.count++;
        e.last = now;
        recorded++;
        payloads.addFirst(body);
        while (payloads.size() > payloadLimit) {
            payloads.removeLast();
        }
    }

    synchronized void rejectInjected() {
        rejectedInjected++;
    }

    synchronized void rejectInvalid() {
        rejectedInvalid++;
    }

    synchronized void reset() {
        keys.clear();
        payloads.clear();
        recorded = 0;
        rejectedInjected = 0;
        rejectedInvalid = 0;
    }

    synchronized Map<String, Long> summary() {
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("recorded", recorded);
        m.put("uniqueKeys", (long) keys.size());
        m.put("duplicates", recorded - keys.size());
        m.put("rejectedInjected", rejectedInjected);
        m.put("rejectedInvalid", rejectedInvalid);
        return m;
    }

    synchronized List<Key> keys() {
        List<Key> out = new ArrayList<>(keys.size());
        keys.forEach((k, e) -> out.add(new Key(k, e.count, e.first, e.last, e.alertId, e.alertVersion)));
        return out;
    }

    synchronized List<JsonNode> deliveries(Long alertId, int limit) {
        List<JsonNode> out = new ArrayList<>();
        for (JsonNode p : payloads) {
            if (alertId == null || p.get("alertId").longValue() == alertId) {
                out.add(p);
                if (out.size() >= limit) {
                    break;
                }
            }
        }
        return out;
    }
}
