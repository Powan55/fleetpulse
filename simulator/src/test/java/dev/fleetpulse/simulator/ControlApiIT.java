package dev.fleetpulse.simulator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "sim.device-count=20",
            "sim.seed=42",
            "sim.epoch=1792130400000",
            "sim.heartbeat-interval=100ms",
            "sim.telemetry-interval=100ms",
            "sim.topic=sim-ctl"
        })
class ControlApiIT {

    private static final String TOPIC = "sim-ctl";
    private static final Pattern SEQ = Pattern.compile("\"seq\":(\\d+)");
    private static final Pattern TIME = Pattern.compile("\"event_time\":\"([^\"]+)\"");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.2");

    @DynamicPropertySource
    static void kafka(DynamicPropertyRegistry r) throws Exception {
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get();
        }
    }

    @Value("${local.server.port}")
    int port;

    private final HttpClient http = HttpClient.newHttpClient();
    private final JsonMapper json = JsonMapper.builder().build();

    private HttpResponse<String> call(String method, String path, String contentType, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1" + path))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (contentType != null) {
            b.header("Content-Type", contentType);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode ok(String method, String path, String body, int status) throws Exception {
        HttpResponse<String> r = call(method, path, "application/json", body);
        assertEquals(status, r.statusCode(), r.body());
        return json.readTree(r.body());
    }

    private void problem(HttpResponse<String> r, int status, String code) throws Exception {
        assertEquals(status, r.statusCode(), r.body());
        assertTrue(r.headers().firstValue("Content-Type").orElse("").startsWith("application/problem+json"), r.headers().toString());
        JsonNode p = json.readTree(r.body());
        assertEquals(status, p.get("status").intValue());
        assertEquals(code, p.get("code").stringValue());
        assertEquals("urn:fleetpulse:problem:" + code.toLowerCase().replace('_', '-'), p.get("type").stringValue());
        assertTrue(p.get("title").isString());
        assertFalse(r.body().contains("Exception") || r.body().contains("\tat "), r.body());
    }

    private void bad(String method, String path, String body) throws Exception {
        problem(call(method, path, "application/json", body), 400, "VALIDATION_FAILED");
    }

    @Test
    void badInputIsRejectedWithProblemJson() throws Exception {
        problem(call("PUT", "/chaos", "text/plain", "{}"), 415, "UNSUPPORTED_MEDIA_TYPE");
        problem(call("PUT", "/chaos", null, "{}"), 415, "UNSUPPORTED_MEDIA_TYPE");
        problem(call("POST", "/chaos/malformed", "text/plain", "{}"), 415, "UNSUPPORTED_MEDIA_TYPE");
        problem(call("PUT", "/generation", "text/plain", "{}"), 415, "UNSUPPORTED_MEDIA_TYPE");

        problem(call("PUT", "/chaos", "application/json", "x".repeat(70_000)), 413, "PAYLOAD_TOO_LARGE");
        bad("PUT", "/chaos", "{");
        bad("PUT", "/chaos", "");
        bad("PUT", "/chaos", "[]");
        bad("PUT", "/chaos", "{\"dropRate\":0.1,\"dropRate\":0.2}");
        bad("PUT", "/chaos", "{} {}");
        bad("PUT", "/chaos", "{\"dropRate\":1.5}");
        bad("PUT", "/chaos", "{\"dropRate\":-0.1}");
        bad("PUT", "/chaos", "{\"dropRate\":\"0.1\"}");
        bad("PUT", "/chaos", "{\"duplicateRate\":null}");
        bad("PUT", "/chaos", "{\"nope\":1}");
        bad("PUT", "/chaos", "{\"replayOnReconnect\":\"yes\"}");
        bad("PUT", "/chaos", "{\"replayMaxRate\":0}");
        bad("PUT", "/chaos", "{\"replayMaxRate\":2001}");
        bad("PUT", "/chaos", "{\"skewSets\":[{\"fraction\":0.6,\"offsetMs\":1},{\"fraction\":0.6,\"offsetMs\":2}]}");
        bad("PUT", "/chaos", "{\"skewSets\":[{\"fraction\":0.1}]}");
        bad("PUT", "/chaos", "{\"skewSets\":[{\"fraction\":0.1,\"offsetMs\":3600001}]}");
        bad("PUT", "/chaos", "{\"skewSets\":[{\"fraction\":0.1,\"offsetMs\":1.5}]}");
        bad("PUT", "/chaos", "{\"skewSets\":[{\"fraction\":0.1,\"offsetMs\":1,\"x\":1}]}");
        bad("PUT", "/chaos", "{\"skewSets\":[" + "{\"fraction\":0.1,\"offsetMs\":1},".repeat(4) + "{\"fraction\":0.1,\"offsetMs\":1}]}");

        bad("POST", "/chaos/malformed", "{}");
        bad("POST", "/chaos/malformed", "{\"count\":0,\"kind\":\"oversize\"}");
        bad("POST", "/chaos/malformed", "{\"count\":1001,\"kind\":\"oversize\"}");
        bad("POST", "/chaos/malformed", "{\"count\":1.5,\"kind\":\"oversize\"}");
        bad("POST", "/chaos/malformed", "{\"count\":1,\"kind\":\"other\"}");
        bad("POST", "/chaos/malformed", "{\"count\":1,\"kind\":5}");
        bad("POST", "/chaos/malformed", "{\"count\":1,\"kind\":\"oversize\",\"extra\":true}");

        bad("PUT", "/generation", "{}");
        bad("PUT", "/generation", "{\"paused\":\"true\"}");
        bad("PUT", "/generation", "{\"paused\":true,\"x\":1}");

        problem(call("GET", "/ledger/devices/dev-9999", null, null), 404, "NOT_FOUND");
        problem(call("GET", "/ledger/devices/dev-00001", null, null), 404, "NOT_FOUND");
        problem(call("GET", "/ledger/devices/nope", null, null), 400, "VALIDATION_FAILED");
        problem(call("GET", "/nothing", null, null), 404, "NOT_FOUND");

        JsonNode chaos = ok("GET", "/chaos", null, 200);
        assertEquals(0, chaos.get("dropRate").doubleValue());
    }

    private Map<String, List<Long>> readTopic(KafkaConsumer<String, String> c, long quietMs, List<ConsumerRecord<String, String>> all) {
        Map<String, List<Long>> bySeq = new HashMap<>();
        long quietSince = System.currentTimeMillis();
        while (System.currentTimeMillis() - quietSince < quietMs) {
            for (ConsumerRecord<String, String> rec : c.poll(Duration.ofMillis(300))) {
                quietSince = System.currentTimeMillis();
                all.add(rec);
                Matcher m = SEQ.matcher(rec.value());
                if (m.find()) {
                    bySeq.computeIfAbsent(rec.key(), k -> new ArrayList<>()).add(Long.parseLong(m.group(1)));
                }
            }
        }
        return bySeq;
    }

    private long endOffsets(KafkaConsumer<String, String> c) {
        List<TopicPartition> tps = c.partitionsFor(TOPIC).stream().map(p -> new TopicPartition(TOPIC, p.partition())).toList();
        return c.endOffsets(tps).values().stream().mapToLong(Long::longValue).sum();
    }

    @Test
    void chaosLedgerCountersPauseAndMalformedMatchTheTopic() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        JsonNode applied = ok("PUT", "/chaos", "{\"duplicateRate\":0.3,\"dropRate\":0.2,\"reorderRate\":0.2,"
                + "\"skewSets\":[{\"fraction\":0.5,\"offsetMs\":300000},{\"fraction\":0.5,\"offsetMs\":-300000}]}", 200);
        assertEquals(0.3, applied.get("duplicateRate").doubleValue());
        assertEquals(0.3, ok("GET", "/chaos", null, 200).get("duplicateRate").doubleValue());
        Thread.sleep(3000);

        assertEquals(true, ok("PUT", "/generation", "{\"paused\":true}", 200).get("paused").booleanValue());
        assertEquals(true, ok("GET", "/generation", null, 200).get("paused").booleanValue());

        JsonNode counters = ok("GET", "/counters", null, 200);
        long generated = counters.get("generated").longValue();
        long accepted = counters.get("accepted").longValue();
        long dropped = counters.get("dropped").longValue();
        assertEquals(0, counters.get("bufferedEvents").longValue(), counters.toString());
        assertEquals(0, counters.get("sendFailed").longValue(), counters.toString());
        assertEquals(generated, accepted + dropped + counters.get("bufferOverflow").longValue(), counters.toString());
        assertTrue(dropped > 0 && counters.get("injectedDups").longValue() > 0 && counters.get("reordered").longValue() > 0
                && counters.get("skewedEvents").longValue() > 0, counters.toString());

        JsonNode ledger = ok("GET", "/ledger", null, 200);
        assertEquals(accepted, ledger.get("totalAccepted").longValue());
        assertEquals(1792130400000L, ledger.get("epoch").longValue());
        assertEquals(20, ledger.get("devices").size());

        List<ConsumerRecord<String, String>> all = new ArrayList<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(cfg)) {
            c.subscribe(List.of(TOPIC));
            Map<String, List<Long>> bySeq = readTopic(c, 2500, all);
            assertEquals(counters.get("sent").longValue(), all.size());

            long ahead = 0;
            long behind = 0;
            for (ConsumerRecord<String, String> rec : all) {
                Matcher m = TIME.matcher(rec.value());
                assertTrue(m.find());
                long diff = Instant.parse(m.group(1)).toEpochMilli() - rec.timestamp();
                ahead += diff > 200_000 ? 1 : 0;
                behind += diff < -200_000 ? 1 : 0;
            }
            assertTrue(ahead > 0 && behind > 0, ahead + "/" + behind);

            long total = 0;
            boolean outOfOrder = false;
            for (JsonNode d : ledger.get("devices")) {
                String id = d.get("deviceId").stringValue();
                List<Long> seen = bySeq.getOrDefault(id, List.of());
                Set<Long> unique = new HashSet<>(seen);
                assertEquals(unique.size(), d.get("acceptedCount").longValue(), id);
                assertEquals(unique.stream().mapToLong(Long::longValue).max().orElse(0), d.get("maxAcceptedSeq").longValue(), id);
                total += unique.size();
                for (int i = 1; i < seen.size(); i++) {
                    outOfOrder |= seen.get(i) < seen.get(i - 1);
                }
                JsonNode detail = ok("GET", "/ledger/devices/" + id, null, 200);
                Set<Long> fromRanges = new HashSet<>();
                for (JsonNode r : detail.get("acceptedSeqRanges")) {
                    for (long s = r.get(0).longValue(); s <= r.get(1).longValue(); s++) {
                        fromRanges.add(s);
                    }
                }
                assertEquals(unique, fromRanges, id);
                assertEquals(d.get("acceptedCount").longValue(), detail.get("acceptedCount").longValue());
            }
            assertEquals(accepted, total);
            assertTrue(outOfOrder, "reorder produced no out-of-order seq on the topic");

            long before = endOffsets(c);
            Thread.sleep(1500);
            assertEquals(before, endOffsets(c), "records appeared while paused");

            for (String kind : List.of("invalid_json", "missing_field", "bad_type", "oversize")) {
                assertEquals(5, ok("POST", "/chaos/malformed", "{\"count\":5,\"kind\":\"" + kind + "\"}", 202).get("sent").intValue());
            }
            assertEquals(before + 20, endOffsets(c));
            JsonNode after = ok("GET", "/counters", null, 200);
            assertEquals(20, after.get("malformedInjected").longValue());
            assertEquals(accepted, after.get("accepted").longValue());
            assertEquals(accepted, ok("GET", "/ledger", null, 200).get("totalAccepted").longValue());

            ok("PUT", "/chaos", "{}", 200);
            ok("PUT", "/generation", "{\"paused\":false}", 200);
            Thread.sleep(1500);
            assertTrue(ok("GET", "/counters", null, 200).get("accepted").longValue() > accepted);
        }
    }
}
