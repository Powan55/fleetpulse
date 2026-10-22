package dev.fleetpulse.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "sink.payload-limit=5")
class SinkIT {

    private static final String SAMPLE = """
            {"schemaVersion":1,"idempotencyKey":"alert-%d-opened","alertId":%d,"alertVersion":1,"transition":"opened",
             "status":"OPEN","deviceId":"dev-0042","ruleId":"temperature_high","backfilled":false,
             "occurredAt":"2026-10-16T12:40:01.120Z","openedAt":"2026-10-16T12:40:01.020Z","ackedAt":null,
             "resolvedAt":null,"resolvedBy":null,"triggerEventId":"5d41402a-bc4b-3a76-b971-9d911017c592",
             "conditionStartedEventTime":null}""";

    @Value("${local.server.port}")
    int port;

    private final HttpClient http = HttpClient.newHttpClient();
    private final JsonMapper json = JsonMapper.builder().build();

    private HttpResponse<String> call(String method, String path, String ct, String key, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (ct != null) {
            b.header("Content-Type", ct);
        }
        if (key != null) {
            b.header("Idempotency-Key", key);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(int id, String body) throws Exception {
        return call("POST", "/webhooks/alerts", "application/json", "alert-" + id + "-opened", body);
    }

    private static String sample(int id) {
        return SAMPLE.formatted(id, id);
    }

    @BeforeEach
    void reset() throws Exception {
        assertEquals(204, call("DELETE", "/api/v1/deliveries", null, null, null).statusCode());
        assertEquals(200, call("PUT", "/api/v1/mode", "application/json", null, "{}").statusCode());
    }

    @Test
    void recordsKeysDuplicatesAndResets() throws Exception {
        assertEquals(200, post(2, sample(2)).statusCode());
        assertEquals(200, post(1, sample(1)).statusCode());
        assertEquals(200, post(2, sample(2)).statusCode());
        JsonNode keys = json.readTree(call("GET", "/api/v1/deliveries/keys", null, null, null).body());
        assertEquals(2, keys.size());
        assertEquals("alert-2-opened", keys.get(0).get("key").stringValue());
        assertEquals(2, keys.get(0).get("count").intValue());
        assertEquals(2, keys.get(0).get("alertId").intValue());
        assertEquals(1, keys.get(0).get("alertVersion").intValue());
        assertEquals("alert-1-opened", keys.get(1).get("key").stringValue());
        JsonNode s = json.readTree(call("GET", "/api/v1/deliveries/summary", null, null, null).body());
        assertEquals(3, s.get("recorded").longValue());
        assertEquals(2, s.get("uniqueKeys").longValue());
        assertEquals(1, s.get("duplicates").longValue());
        JsonNode one = json.readTree(call("GET", "/api/v1/deliveries?alertId=1", null, null, null).body());
        assertEquals(1, one.size());
        call("DELETE", "/api/v1/deliveries", null, null, null);
        assertEquals(0, json.readTree(call("GET", "/api/v1/deliveries/keys", null, null, null).body()).size());
        assertEquals(0, json.readTree(call("GET", "/api/v1/deliveries/summary", null, null, null).body()).get("recorded").longValue());
    }

    @Test
    void outageAndErrorRateReturn503WithoutRecording() throws Exception {
        assertEquals(200, call("PUT", "/api/v1/mode", "application/json", null, "{\"outage\":true}").statusCode());
        assertEquals(true, json.readTree(call("GET", "/api/v1/mode", null, null, null).body()).get("outage").booleanValue());
        assertEquals(503, post(1, sample(1)).statusCode());
        call("PUT", "/api/v1/mode", "application/json", null, "{\"errorRate\":1}");
        assertEquals(503, post(1, sample(1)).statusCode());
        call("PUT", "/api/v1/mode", "application/json", null, "{\"errorRate\":0}");
        JsonNode s = json.readTree(call("GET", "/api/v1/deliveries/summary", null, null, null).body());
        assertEquals(0, s.get("recorded").longValue());
        assertEquals(2, s.get("rejectedInjected").longValue());
        assertEquals(200, post(1, sample(1)).statusCode());
    }

    @Test
    void delayRecordsThenAnswersLate() throws Exception {
        call("PUT", "/api/v1/mode", "application/json", null, "{\"delayMs\":600}");
        long t0 = System.nanoTime();
        assertEquals(200, post(1, sample(1)).statusCode());
        assertTrue((System.nanoTime() - t0) / 1_000_000 >= 550);
        assertEquals(1, json.readTree(call("GET", "/api/v1/deliveries/summary", null, null, null).body()).get("recorded").longValue());
    }

    @Test
    void invalidPayloadsAreRejectedAndCounted() throws Exception {
        List<String> bad = new ArrayList<>(List.of(
                "{", "[]", "{}", sample(1).replace("\"schemaVersion\":1", "\"schemaVersion\":2"),
                sample(1).replace("\"alertVersion\":1", "\"alertVersion\":4"),
                sample(1).replace("\"status\":\"OPEN\"", "\"status\":\"NOPE\""),
                sample(1).replace("dev-0042", "device-42"),
                sample(1).replace("\"backfilled\":false", "\"backfilled\":\"no\""),
                sample(1).replace("2026-10-16T12:40:01.120Z", "yesterday"),
                sample(1).replace("\"resolvedBy\":null", "\"resolvedBy\":\"bot\""),
                sample(1).replace("}", ",\"extra\":1}")));
        for (String b : bad) {
            assertEquals(400, post(1, b).statusCode(), b);
        }
        assertEquals(400, call("POST", "/webhooks/alerts", "application/json", "alert-9-opened", sample(1)).statusCode());
        assertEquals(400, call("POST", "/webhooks/alerts", "application/json", null, sample(1)).statusCode());
        assertEquals(415, call("POST", "/webhooks/alerts", "text/plain", "alert-1-opened", sample(1)).statusCode());
        JsonNode s = json.readTree(call("GET", "/api/v1/deliveries/summary", null, null, null).body());
        assertEquals(bad.size() + 2, s.get("rejectedInvalid").longValue());
        assertEquals(0, s.get("recorded").longValue());
    }

    @Test
    void validatorAgreesWithContractSchema() throws Exception {
        String schemaText = Files.readString(Path.of("..", "contracts", "webhook.schema.json"));
        Schema schema = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
                .getSchema(schemaText, InputFormat.JSON);
        List<String> cases = List.of(sample(1),
                sample(1).replace("\"transition\":\"opened\"", "\"transition\":\"closed\""),
                sample(1).replace("\"alertId\":1", "\"alertId\":0"),
                sample(1).replace("alert-1-opened", "alert-x-opened"),
                sample(1).replace("\"ruleId\":\"temperature_high\"", "\"ruleId\":\"other\""),
                sample(1).replace("\"backfilled\":false,", ""),
                sample(1).replace("\"conditionStartedEventTime\":null", "\"conditionStartedEventTime\":5"));
        for (String c : cases) {
            boolean schemaOk = schema.validate(c, InputFormat.JSON).isEmpty();
            boolean mine = WebhookValidator.validate(json.readTree(c)).isEmpty();
            assertEquals(schemaOk, mine, c);
        }
    }

    @Test
    void payloadListIsCappedKeysAreNot() throws Exception {
        for (int i = 1; i <= 8; i++) {
            assertEquals(200, post(i, sample(i)).statusCode());
        }
        JsonNode all = json.readTree(call("GET", "/api/v1/deliveries?limit=1000", null, null, null).body());
        assertEquals(5, all.size());
        assertEquals(8, all.get(0).get("alertId").intValue());
        assertEquals(8, json.readTree(call("GET", "/api/v1/deliveries/keys", null, null, null).body()).size());
    }

    @Test
    void controlInputIsValidated() throws Exception {
        for (String b : new String[] {"{\"outage\":\"yes\"}", "{\"errorRate\":2}", "{\"delayMs\":60001}", "{\"delayMs\":1.5}",
                "{\"x\":1}", "[]", "{"}) {
            assertEquals(400, call("PUT", "/api/v1/mode", "application/json", null, b).statusCode(), b);
        }
        assertEquals(415, call("PUT", "/api/v1/mode", "text/plain", null, "{}").statusCode());
        assertEquals(413, call("PUT", "/api/v1/mode", "application/json", null, "{\"outage\":" + " ".repeat(70000) + "false}").statusCode());
        assertEquals(400, call("GET", "/api/v1/deliveries?limit=0", null, null, null).statusCode());
        assertEquals(200, call("GET", "/actuator/health/readiness", null, null, null).statusCode());
        assertEquals(404, call("GET", "/actuator/env", null, null, null).statusCode());
    }
}
