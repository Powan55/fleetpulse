package dev.fleetpulse.simulator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
            "sim.device-count=10",
            "sim.seed=42",
            "sim.epoch=1792130400000",
            "sim.heartbeat-interval=100ms",
            "sim.telemetry-interval=100ms",
            "sim.buffer-capacity=10",
            "sim.topic=sim-off"
        })
class OfflineIT {

    private static final String TOPIC = "sim-off";
    private static final Pattern SEQ = Pattern.compile("\"seq\":(\\d+)");
    private static final Pattern ID = Pattern.compile("\"event_id\":\"([^\"]+)\"");
    private static final Pattern REPLAY = Pattern.compile("\"replay\":(true|false)");
    private static final Pattern TEMP = Pattern.compile("\"temperature_c\":([0-9.]+)");

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
        return r.body().isEmpty() ? null : json.readTree(r.body());
    }

    private void bad(String path, String body) throws Exception {
        HttpResponse<String> r = call("POST", path, "application/json", body);
        assertEquals(400, r.statusCode(), body + " -> " + r.body());
        assertTrue(r.headers().firstValue("Content-Type").orElse("").startsWith("application/problem+json"));
        assertEquals("VALIDATION_FAILED", json.readTree(r.body()).get("code").stringValue());
    }

    @Test
    void faultAndDisconnectInputIsValidated() throws Exception {
        String t = "\"target\":{\"deviceIds\":[\"dev-0001\"]}";
        assertEquals(415, call("POST", "/faults", "text/plain", "{}").statusCode());
        assertEquals(415, call("POST", "/disconnects", "text/plain", "{}").statusCode());
        assertEquals(413, call("POST", "/faults", "application/json", "x".repeat(70_000)).statusCode());
        bad("/faults", "{" + t + ",\"metric\":\"temperature\",\"pattern\":null}");
        bad("/faults", "{" + t + ",\"metric\":\"temperature\"}");
        bad("/faults", "{" + t + ",\"metric\":\"temperature\",\"level\":\"FAILED\",\"pattern\":{\"steps\":[{\"level\":\"OK\",\"samples\":1}]}}");
        bad("/faults", "{" + t + ",\"metric\":\"temperature\",\"level\":null}");
        bad("/faults", "{" + t + ",\"metric\":\"temperature\",\"level\":\"OK\"}");
        bad("/faults", "{" + t + ",\"metric\":\"temperature\",\"pattern\":{\"steps\":[]}}");
        bad("/faults", "{" + t + ",\"metric\":\"temperature\",\"pattern\":{\"steps\":[{\"level\":\"OK\",\"samples\":0}]}}");
        bad("/faults", "{" + t + ",\"metric\":\"temperature\",\"pattern\":{\"steps\":[{\"level\":\"OK\",\"samples\":101}]}}");
        bad("/faults", "{" + t + ",\"metric\":\"temperature\",\"pattern\":{\"steps\":[{\"level\":\"BAD\",\"samples\":1}]}}");
        bad("/faults", "{" + t + ",\"metric\":\"temperature\",\"pattern\":{\"steps\":[{\"level\":\"OK\"}]}}");
        bad("/faults", "{" + t + ",\"metric\":\"temperature\",\"pattern\":{\"steps\":[{\"level\":\"OK\",\"samples\":1,\"x\":1}]}}");
        bad("/faults", "{" + t + ",\"metric\":\"temperature\",\"pattern\":{\"steps\":["
                + "{\"level\":\"OK\",\"samples\":1},".repeat(10) + "{\"level\":\"OK\",\"samples\":1}]}}");
        bad("/faults", "{" + t + ",\"metric\":\"temperature\",\"pattern\":{\"steps\":[{\"level\":\"OK\",\"samples\":1}],\"x\":1}}");
        bad("/faults", "{" + t + ",\"metric\":\"speed\",\"level\":\"FAILED\"}");
        bad("/faults", "{" + t + ",\"level\":\"FAILED\"}");
        bad("/faults", "{\"metric\":\"errors\",\"level\":\"FAILED\"}");
        bad("/faults", "{\"target\":{},\"metric\":\"errors\",\"level\":\"FAILED\"}");
        bad("/faults", "{\"target\":{\"fraction\":0.5,\"deviceIds\":[\"dev-0001\"]},\"metric\":\"errors\",\"level\":\"FAILED\"}");
        bad("/faults", "{\"target\":{\"fraction\":0},\"metric\":\"errors\",\"level\":\"FAILED\"}");
        bad("/faults", "{\"target\":{\"fraction\":1.1},\"metric\":\"errors\",\"level\":\"FAILED\"}");
        bad("/faults", "{\"target\":{\"deviceIds\":[]},\"metric\":\"errors\",\"level\":\"FAILED\"}");
        bad("/faults", "{\"target\":{\"deviceIds\":[\"dev-9999\"]},\"metric\":\"errors\",\"level\":\"FAILED\"}");
        bad("/faults", "{\"target\":{\"deviceIds\":[5]},\"metric\":\"errors\",\"level\":\"FAILED\"}");
        bad("/faults", "{" + t + ",\"metric\":\"errors\",\"level\":\"FAILED\",\"durationMs\":999}");
        bad("/faults", "{" + t + ",\"metric\":\"errors\",\"level\":\"FAILED\",\"extra\":1}");
        bad("/disconnects", "{" + t + "}");
        bad("/disconnects", "{" + t + ",\"durationMs\":999}");
        bad("/disconnects", "{" + t + ",\"durationMs\":3600001}");
        bad("/disconnects", "{\"durationMs\":5000}");
        bad("/disconnects", "{" + t + ",\"durationMs\":5000,\"x\":1}");
        assertEquals(0, ok("GET", "/faults", null, 200).size());
        assertEquals(0, ok("GET", "/disconnects", null, 200).size());
        assertEquals(404, call("DELETE", "/faults/nope", null, null).statusCode());

        JsonNode f = ok("POST", "/faults", "{\"target\":{\"fraction\":0.3},\"metric\":\"battery\",\"pattern\":"
                + "{\"steps\":[{\"level\":\"DEGRADED\",\"samples\":3},{\"level\":\"OK\",\"samples\":5}]}}", 202);
        assertEquals(3, f.get("devices").intValue());
        assertTrue(f.get("level").isNull() && f.get("until").isNull());
        assertEquals(2, f.get("pattern").get("steps").size());
        assertEquals(1, ok("GET", "/faults", null, 200).size());
        assertEquals(204, call("DELETE", "/faults/" + f.get("faultId").stringValue(), null, null).statusCode());
        ok("POST", "/faults", "{" + t + ",\"metric\":\"errors\",\"level\":\"DEGRADED\",\"durationMs\":60000}", 202);
        assertEquals(204, call("DELETE", "/faults", null, null).statusCode());
        assertEquals(0, ok("GET", "/faults", null, 200).size());
    }

    @Test
    void disconnectedDevicesBufferOverflowThenReplayInOrder() throws Exception {
        ok("POST", "/faults", "{\"target\":{\"deviceIds\":[\"dev-0004\"]},\"metric\":\"temperature\",\"level\":\"FAILED\"}", 202);
        Thread.sleep(1500);
        JsonNode d = ok("POST", "/disconnects", "{\"target\":{\"deviceIds\":[\"dev-0001\",\"dev-0002\",\"dev-0003\"]},\"durationMs\":3000}", 202);
        assertEquals(3, d.get("devices").intValue());
        assertEquals(1, ok("GET", "/disconnects", null, 200).size());
        Thread.sleep(500);
        assertEquals(3, ok("GET", "/counters", null, 200).get("disconnectedDevices").intValue());
        Thread.sleep(6000);
        assertEquals(0, ok("GET", "/counters", null, 200).get("disconnectedDevices").intValue());
        assertEquals(0, ok("GET", "/disconnects", null, 200).size());

        ok("PUT", "/generation", "{\"paused\":true}", 200);
        JsonNode c = null;
        for (int k = 0; k < 150; k++) {
            c = ok("GET", "/counters", null, 200);
            if (c.get("bufferedEvents").longValue() == 0) {
                break;
            }
            Thread.sleep(100);
        }
        assertEquals(0, c.get("bufferedEvents").longValue(), c.toString());
        assertEquals(c.get("generated").longValue(), c.get("accepted").longValue() + c.get("dropped").longValue()
                + c.get("bufferOverflow").longValue(), c.toString());
        assertTrue(c.get("bufferOverflow").longValue() > 30, c.toString());

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Map<String, List<String>> byDevice = new HashMap<>();
        long total = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cfg)) {
            consumer.subscribe(List.of(TOPIC));
            long quiet = System.currentTimeMillis();
            while (System.currentTimeMillis() - quiet < 3000) {
                for (ConsumerRecord<String, String> rec : consumer.poll(Duration.ofMillis(300))) {
                    quiet = System.currentTimeMillis();
                    byDevice.computeIfAbsent(rec.key(), k -> new ArrayList<>()).add(rec.value());
                    total++;
                }
            }
        }
        assertEquals(c.get("sent").longValue(), total);
        long replayRecords = 0;
        for (Map.Entry<String, List<String>> e : byDevice.entrySet()) {
            String dev = e.getKey();
            boolean offline = List.of("dev-0001", "dev-0002", "dev-0003").contains(dev);
            long last = 0;
            boolean inReplay = false;
            boolean replayEnded = false;
            boolean gap = false;
            for (String v : e.getValue()) {
                long seq = Long.parseLong(find(SEQ, v));
                boolean replay = Boolean.parseBoolean(find(REPLAY, v));
                assertTrue(seq > last, dev + " order " + last + " then " + seq);
                gap |= seq != last + 1;
                last = seq;
                assertEquals(UUID.nameUUIDFromBytes(("42:" + dev + ":1792130400000:" + seq).getBytes(StandardCharsets.UTF_8)).toString(),
                        find(ID, v));
                if (replay) {
                    replayRecords++;
                    assertTrue(offline, dev + " replayed without disconnect");
                    assertFalse(replayEnded, dev + " replay block is not contiguous");
                    inReplay = true;
                } else if (inReplay) {
                    replayEnded = true;
                }
            }
            if (offline) {
                assertTrue(inReplay && gap, dev + " replay=" + inReplay + " gap=" + gap);
            } else {
                assertFalse(gap, dev + " live device has a gap");
            }
        }
        assertEquals(c.get("replayed").longValue(), replayRecords);

        List<String> hot = byDevice.get("dev-0004").stream().filter(v -> v.contains("temperature_c")).toList();
        for (String v : hot.subList(hot.size() - 5, hot.size())) {
            assertTrue(Double.parseDouble(find(TEMP, v)) >= 93, v);
        }
        for (int k = 0; k < 10; k++) {
            JsonNode led = ok("GET", "/ledger/devices/dev-%04d".formatted(k + 1), null, 200);
            long unique = byDevice.get("dev-%04d".formatted(k + 1)).size();
            assertEquals(unique, led.get("acceptedCount").longValue());
        }
        ok("DELETE", "/faults", null, 204);
    }

    private static String find(Pattern p, String s) {
        Matcher m = p.matcher(s);
        assertTrue(m.find(), s);
        return m.group(1);
    }
}
