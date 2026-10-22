package dev.fleetpulse.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.MeterRegistry;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "fp.sse.heartbeat-interval=1s")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class FleetApiIT {
    private static final String API_PASSWORD = "api-test-pw";
    private static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:18.6");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @LocalServerPort int port;
    @Autowired MeterRegistry meters;

    static {
        PG.start();
        Flyway.configure().dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
                .locations("filesystem:../db/migrations")
                .placeholders(Map.of("app_password", "app-test-pw", "api_password", API_PASSWORD, "ro_password", "ro-test-pw"))
                .load().migrate();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", PG::getJdbcUrl);
        r.add("spring.datasource.username", () -> "fleetpulse_api");
        r.add("spring.datasource.password", () -> API_PASSWORD);
    }

    @BeforeAll
    static void seed() throws Exception {
        String[] states = {"HEALTHY", "DEGRADED", "FAILED", "OFFLINE", "RECOVERING"};
        try (Connection c = admin(); PreparedStatement ps = c.prepareStatement(
                "INSERT INTO device(device_id, state, last_epoch, last_seq, last_seen_at) VALUES (?, ?, 1, 1, now())")) {
            for (int i = 1; i <= 250; i++) {
                ps.setString(1, "dev-%04d".formatted(i));
                ps.setString(2, states[i % 5]);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    @BeforeEach
    void listening() throws Exception {
        waitForListener();
    }

    @AfterAll
    static void stopDb() {
        PG.stop();
    }

    static Connection admin() throws SQLException {
        return DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    }

    static String scalar(String sql) throws SQLException {
        try (Connection c = admin(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    static void exec(String sql) throws SQLException {
        try (Connection c = admin(); Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    static void waitForListener() throws Exception {
        for (int i = 0; i < 100; i++) {
            if (!scalar("SELECT count(*) FROM pg_stat_activity WHERE usename='fleetpulse_api' AND query LIKE 'LISTEN%'").equals("0")) return;
            Thread.sleep(100);
        }
        throw new AssertionError("listener never connected");
    }

    static void notifyDevices(String... ids) throws SQLException {
        exec("SELECT pg_notify('fleetpulse_changes', '{\"t\":\"d\",\"ids\":[\"" + String.join("\",\"", ids) + "\"]}')");
    }

    HttpResponse<String> get(String path) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void summaryMatchesRows() throws Exception {
        var res = get("/api/v1/fleet/summary");
        assertEquals(200, res.statusCode());
        ContractSchemas.assertApi("FleetSummary", res.body());
        JsonNode body = JSON.readTree(res.body());
        assertEquals(Long.parseLong(scalar("SELECT count(*) FROM device")), body.get("total").asLong());
        for (String s : DeviceRepository.STATES) {
            assertEquals(Long.parseLong(scalar("SELECT count(*) FROM device WHERE state='" + s + "'")), body.get("byState").get(s).asLong(), s);
        }
    }

    @Test
    void pagingIsStableAndFilterWorks() throws Exception {
        List<String> ids = new ArrayList<>();
        for (int p = 0; p < 3; p++) {
            var res = get("/api/v1/devices?size=100&page=" + p);
            assertEquals(200, res.statusCode());
            ContractSchemas.assertApi("DevicePage", res.body());
            JsonNode body = JSON.readTree(res.body());
            assertEquals(Long.parseLong(scalar("SELECT count(*) FROM device")), body.get("totalItems").asLong());
            body.get("items").forEach(i -> ids.add(i.get("deviceId").asString()));
            if (p == 0) assertEquals(res.body().replaceAll("\"asOf\"[^,}]*", ""), get("/api/v1/devices?size=100&page=0").body().replaceAll("\"asOf\"[^,}]*", ""));
        }
        assertTrue(ids.size() >= 250);
        assertEquals(ids.stream().sorted().toList(), ids);
        assertEquals(ids.size(), ids.stream().distinct().count());

        var res = get("/api/v1/devices?state=OFFLINE&size=1000");
        ContractSchemas.assertApi("DevicePage", res.body());
        JsonNode body = JSON.readTree(res.body());
        assertEquals(Long.parseLong(scalar("SELECT count(*) FROM device WHERE state='OFFLINE'")), body.get("totalItems").asLong());
        body.get("items").forEach(i -> assertEquals("OFFLINE", i.get("state").asString()));
        assertEquals(200, get("/api/v1/devices").statusCode());
        assertEquals(200, get("/api/v1/devices?size=1000").statusCode());
    }

    @Test
    void badParamsGetProblemJson() throws Exception {
        for (String q : List.of("size=0", "size=1001", "page=-1", "page=10001", "state=BOGUS", "page=abc")) {
            var res = get("/api/v1/devices?" + q);
            assertEquals(400, res.statusCode(), q);
            assertTrue(res.headers().firstValue("content-type").orElse("").startsWith("application/problem+json"), q);
            ContractSchemas.assertApi("Problem", res.body());
            assertEquals("VALIDATION_FAILED", JSON.readTree(res.body()).get("code").asString());
        }
        var res = get("/api/v1/nope");
        assertEquals(404, res.statusCode());
        ContractSchemas.assertApi("Problem", res.body());
        assertTrue(!res.body().contains("Exception") && !res.body().contains("\tat "));
    }

    static void seedDetail() throws SQLException {
        exec("INSERT INTO device(device_id, state, last_epoch, last_seq, last_seen_at, last_event_time) VALUES "
                + "('dev-7001','DEGRADED',2,1,now(),now()), ('dev-7002','HEALTHY',1,2,now(),NULL)");
        exec("INSERT INTO telemetry(device_id, epoch, seq, event_id, event_time, received_at, temperature_c, battery_pct, error_count, replay) "
                + "SELECT 'dev-7001', 1, g, gen_random_uuid(), now(), now(), 20 + g % 10, 80.5, g % 3, g % 2 = 0 FROM generate_series(1, 460) g");
        exec("INSERT INTO telemetry VALUES ('dev-7001', 2, 1, gen_random_uuid(), now(), now(), 30, 70, 0, false)");
        exec("INSERT INTO telemetry VALUES ('dev-7002', 1, 1, gen_random_uuid(), now(), now() - interval '1 hour', 30, 70, 0, false), "
                + "('dev-7002', 1, 2, gen_random_uuid(), now(), now(), 31, 71, 1, false)");
        exec("INSERT INTO state_transition(device_id, from_state, to_state, cause, event_id, epoch, seq, event_time) "
                + "SELECT 'dev-7001', 'HEALTHY', 'DEGRADED', 'telemetry', gen_random_uuid(), 1, g, now() FROM generate_series(1, 59) g");
        exec("INSERT INTO state_transition(device_id, from_state, to_state, cause) VALUES ('dev-7001', 'DEGRADED', 'OFFLINE', 'offline_sweep')");
    }

    @Test
    void deviceDetailMatchesContract() throws Exception {
        seedDetail();
        var res = get("/api/v1/devices/dev-7001");
        assertEquals(200, res.statusCode());
        ContractSchemas.assertApi("DeviceDetail", res.body());
        JsonNode body = JSON.readTree(res.body());
        assertEquals("DEGRADED", body.get("state").asString());
        assertEquals(2, body.get("lastEpoch").asLong());
        assertEquals(0, body.get("alerts").size());
        JsonNode tel = body.get("recentTelemetry");
        assertEquals(450, tel.size());
        assertEquals(2, tel.get(0).get("epoch").asLong());
        assertEquals(460, tel.get(1).get("seq").asLong());
        assertEquals(1, tel.get(1).get("epoch").asLong());
        JsonNode hist = body.get("stateHistory");
        assertEquals(50, hist.size());
        assertEquals("offline_sweep", hist.get(0).get("cause").asString());
        assertTrue(hist.get(0).get("id").asLong() > hist.get(1).get("id").asLong());

        JsonNode small = JSON.readTree(get("/api/v1/devices/dev-7001?telemetryLimit=3&historyLimit=200").body());
        assertEquals(3, small.get("recentTelemetry").size());
        assertEquals(60, small.get("stateHistory").size());

        JsonNode windowed = JSON.readTree(get("/api/v1/devices/dev-7002").body());
        assertEquals(1, windowed.get("recentTelemetry").size());
        assertEquals(2, windowed.get("recentTelemetry").get(0).get("seq").asLong());
        assertTrue(windowed.get("lastEventTime").isNull());
        assertEquals(0, windowed.get("stateHistory").size());
    }

    @Test
    void deviceDetailRejectsBadInputAndUnknown() throws Exception {
        for (String p : List.of("dev-7001?telemetryLimit=0", "dev-7001?telemetryLimit=451", "dev-7001?historyLimit=0",
                "dev-7001?historyLimit=201", "dev-7001?historyLimit=abc", "bogus", "dev-12", "dev-1234567")) {
            var res = get("/api/v1/devices/" + p);
            assertEquals(400, res.statusCode(), p);
            assertTrue(res.headers().firstValue("content-type").orElse("").startsWith("application/problem+json"), p);
            ContractSchemas.assertApi("Problem", res.body());
            JsonNode b = JSON.readTree(res.body());
            assertEquals("VALIDATION_FAILED", b.get("code").asString());
            assertEquals("urn:fleetpulse:problem:validation-failed", b.get("type").asString());
        }
        var res = get("/api/v1/devices/dev-8888");
        assertEquals(404, res.statusCode());
        assertTrue(res.headers().firstValue("content-type").orElse("").startsWith("application/problem+json"));
        ContractSchemas.assertApi("Problem", res.body());
        JsonNode b = JSON.readTree(res.body());
        assertEquals("NOT_FOUND", b.get("code").asString());
        assertEquals("urn:fleetpulse:problem:not-found", b.get("type").asString());
    }

    void assertProblem(HttpResponse<String> res, int status, String code) throws Exception {
        assertEquals(status, res.statusCode());
        assertTrue(res.headers().firstValue("content-type").orElse("").startsWith("application/problem+json"));
        ContractSchemas.assertApi("Problem", res.body());
        JsonNode b = JSON.readTree(res.body());
        assertEquals(code, b.get("code").asString());
        assertEquals("urn:fleetpulse:problem:" + code.toLowerCase().replace('_', '-'), b.get("type").asString());
        assertTrue(!res.body().contains("Exception") && !res.body().contains("\tat "));
    }

    HttpResponse<String> send(String method, String path, HttpRequest.BodyPublisher body) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).method(method, body).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void methodBodySizeAndLengthErrorsAreProblems() throws Exception {
        assertProblem(send("POST", "/api/v1/devices", HttpRequest.BodyPublishers.ofString("{}")), 405, "VALIDATION_FAILED");
        assertProblem(send("DELETE", "/api/v1/fleet/summary", HttpRequest.BodyPublishers.noBody()), 405, "VALIDATION_FAILED");
        assertProblem(send("POST", "/api/v1/devices", HttpRequest.BodyPublishers.ofString("x".repeat(65537))), 413, "VALIDATION_FAILED");
        assertEquals(405, send("POST", "/api/v1/devices", HttpRequest.BodyPublishers.ofString("x".repeat(65536))).statusCode());
        var chunked = HttpRequest.BodyPublishers.fromPublisher(HttpRequest.BodyPublishers.ofString("{}"));
        assertProblem(send("POST", "/api/v1/devices", chunked), 411, "VALIDATION_FAILED");
    }

    @Test
    void serverErrorHidesInternals() throws Exception {
        exec("REVOKE SELECT ON state_transition FROM fleetpulse_api");
        try {
            var res = get("/api/v1/devices/dev-0001");
            assertEquals(500, res.statusCode());
            ContractSchemas.assertApi("Problem", res.body());
            assertEquals("INTERNAL", JSON.readTree(res.body()).get("code").asString());
            assertTrue(!res.body().contains("state_transition") && !res.body().contains("SQL") && !res.body().contains("\tat "));
        } finally {
            exec("GRANT SELECT ON state_transition TO fleetpulse_api");
        }
    }

    @Test
    void apiRoleCannotWriteDevices() throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "fleetpulse_api", API_PASSWORD); Statement s = c.createStatement()) {
            assertThrows(SQLException.class, () -> s.executeUpdate("UPDATE device SET state='FAILED'"));
            assertThrows(SQLException.class, () -> s.executeUpdate("DELETE FROM device"));
        }
    }

    @Test
    void healthExposedOnly() throws Exception {
        assertEquals(200, get("/actuator/health/liveness").statusCode());
        var ready = get("/actuator/health/readiness");
        assertEquals(200, ready.statusCode());
        assertTrue(ready.body().contains("UP"));
        assertEquals(404, get("/actuator/env").statusCode());
    }

    @Test
    void streamSendsHelloThenDevicesAfterNotify() throws Exception {
        try (Sse sse = new Sse()) {
            assertTrue(sse.raw.poll(5, TimeUnit.SECONDS).startsWith("retry:2000"));
            Event hello = sse.next("hello");
            ContractSchemas.assertSse("hello", hello.data);

            exec("INSERT INTO device(device_id, state, last_epoch, last_seq, last_seen_at) VALUES ('dev-9001','FAILED',1,1,now())");
            notifyDevices("dev-9001", "dev-0001");
            Event devices = sse.next("devices");
            ContractSchemas.assertSse("devices", devices.data);
            JsonNode items = JSON.readTree(devices.data).get("items");
            assertEquals(2, items.size());
            assertEquals("dev-0001", items.get(0).get("deviceId").asString());
            assertEquals("dev-9001", items.get(1).get("deviceId").asString());
            assertEquals("FAILED", items.get(1).get("state").asString());
            assertEquals(1, items.get(1).get("stateVersion").asInt());

            Event summary = sse.next("fleet.summary");
            ContractSchemas.assertSse("fleetSummary", summary.data);
            ContractSchemas.assertApi("FleetSummary", summary.data);
            assertTrue(sse.sawComment("hb"), "heartbeat comment");
        }
    }

    @Test
    void listenerSurvivesJunkAndReconnectsWithResync() throws Exception {
        double before = meters.counter("fleetpulse.api.notify.rejected").count();
        try (Sse sse = new Sse()) {
            sse.next("hello");
            exec("SELECT pg_notify('fleetpulse_changes', 'garbage')");
            exec("SELECT pg_notify('fleetpulse_changes', '{\"t\":\"d\",\"ids\":[\"x\"]}')");
            exec("INSERT INTO device(device_id, state, last_epoch, last_seq, last_seen_at) VALUES ('dev-9002','HEALTHY',1,1,now())");
            notifyDevices("dev-9002");
            assertTrue(JSON.readTree(sse.next("devices").data).get("items").toString().contains("dev-9002"));
            assertEquals(before + 2, meters.counter("fleetpulse.api.notify.rejected").count());

            exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='fleetpulse_api' AND query LIKE 'LISTEN%'");
            Event resync = sse.next("resync");
            ContractSchemas.assertSse("resync", resync.data);
            waitForListener();
            notifyDevices("dev-9002");
            assertNotNull(sse.next("devices"));
        }
    }

    static int nextDev = 5000;

    static synchronized long newAlert(String status) throws SQLException {
        String dev = "dev-%04d".formatted(nextDev++);
        String extra = status.equals("RESOLVED") ? ", resolved_at, resolved_by" : "";
        String vals = status.equals("RESOLVED") ? ", now(), 'auto'" : "";
        return Long.parseLong(scalar("INSERT INTO alert(device_id, rule_id, status" + extra + ") VALUES ('" + dev
                + "','temperature_high','" + status + "'" + vals + ") RETURNING alert_id"));
    }

    HttpResponse<String> post(String path, String contentType, String body) throws Exception {
        var b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).POST(HttpRequest.BodyPublishers.ofString(body));
        if (contentType != null) b.header("Content-Type", contentType);
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    HttpResponse<String> postJson(String path, String body) throws Exception {
        return post(path, "application/json", body);
    }

    static long outboxRows(long alertId) throws SQLException {
        return Long.parseLong(scalar("SELECT count(*) FROM outbox WHERE alert_id=" + alertId));
    }

    @Test
    void alertListGetAndFilters() throws Exception {
        long a = newAlert("OPEN");
        long b = newAlert("ACKED");
        long c = newAlert("RESOLVED");
        var res = get("/api/v1/alerts/" + a);
        assertEquals(200, res.statusCode());
        ContractSchemas.assertApi("Alert", res.body());
        JsonNode one = JSON.readTree(res.body());
        assertEquals("OPEN", one.get("status").asString());
        assertEquals(1, one.get("version").asInt());

        res = get("/api/v1/alerts?size=200");
        assertEquals(200, res.statusCode());
        ContractSchemas.assertApi("AlertPage", res.body());
        JsonNode page = JSON.readTree(res.body());
        assertEquals(Long.parseLong(scalar("SELECT count(*) FROM alert")), page.get("totalItems").asLong());
        List<Long> ids = new ArrayList<>();
        page.get("items").forEach(i -> ids.add(i.get("alertId").asLong()));
        assertEquals(ids.stream().sorted(java.util.Comparator.reverseOrder()).toList(), ids);

        res = get("/api/v1/alerts?status=OPEN,ACKED&size=200");
        ContractSchemas.assertApi("AlertPage", res.body());
        JsonNode open = JSON.readTree(res.body());
        assertEquals(Long.parseLong(scalar("SELECT count(*) FROM alert WHERE status IN ('OPEN','ACKED')")), open.get("totalItems").asLong());
        open.get("items").forEach(i -> assertTrue(!i.get("status").asString().equals("RESOLVED")));
        assertTrue(open.get("items").toString().contains("\"alertId\":" + b));
        assertTrue(!open.get("items").toString().contains("\"alertId\":" + c));

        String dev = scalar("SELECT device_id FROM alert WHERE alert_id=" + a);
        res = get("/api/v1/alerts?deviceId=" + dev + "&ruleId=temperature_high&status=OPEN");
        assertEquals(1, JSON.readTree(res.body()).get("totalItems").asInt());
        assertEquals(0, JSON.readTree(get("/api/v1/alerts?ruleId=battery_low").body()).get("totalItems").asInt());
        assertEquals(1, JSON.readTree(get("/api/v1/alerts?size=1&page=0").body()).get("items").size());
    }

    @Test
    void alertBadParamsAndUnknownIds() throws Exception {
        for (String q : List.of("status=BOGUS", "status=", "status=OPEN,", "deviceId=x", "ruleId=nope",
                "size=0", "size=201", "page=-1", "page=10001")) {
            var res = get("/api/v1/alerts?" + q);
            assertEquals(400, res.statusCode(), q);
            assertTrue(res.headers().firstValue("content-type").orElse("").startsWith("application/problem+json"), q);
            ContractSchemas.assertApi("Problem", res.body());
        }
        for (String id : List.of("abc", "0", "-3", "99999999999999999999")) {
            assertEquals(400, get("/api/v1/alerts/" + id).statusCode(), id);
            assertEquals(400, postJson("/api/v1/alerts/" + id + "/ack", "{}").statusCode(), id);
        }
        for (String path : List.of("/api/v1/alerts/987654", "/api/v1/alerts/987654/ack", "/api/v1/alerts/987654/resolve")) {
            var res = path.endsWith("987654") ? get(path) : postJson(path, "{}");
            assertEquals(404, res.statusCode(), path);
            ContractSchemas.assertApi("Problem", res.body());
            assertEquals("NOT_FOUND", JSON.readTree(res.body()).get("code").asString());
        }
    }

    @Test
    void ackTwiceMakesOneTransitionAndOneOutboxRow() throws Exception {
        long id = newAlert("OPEN");
        var res = postJson("/api/v1/alerts/" + id + "/ack", "{\"note\":\"on it\"}");
        assertEquals(200, res.statusCode());
        ContractSchemas.assertApi("Alert", res.body());
        JsonNode first = JSON.readTree(res.body());
        assertEquals("ACKED", first.get("status").asString());
        assertEquals(2, first.get("version").asInt());
        assertNotNull(first.get("ackedAt").asString());

        var again = postJson("/api/v1/alerts/" + id + "/ack", "{}");
        assertEquals(200, again.statusCode());
        assertEquals(res.body(), again.body());
        assertEquals(1, outboxRows(id));
        assertEquals("alert-" + id + "-acked", scalar("SELECT idempotency_key FROM outbox WHERE alert_id=" + id));
        assertEquals("2", scalar("SELECT alert_version FROM outbox WHERE alert_id=" + id));
        assertEquals("PENDING", scalar("SELECT status FROM outbox WHERE alert_id=" + id));
        String payload = scalar("SELECT payload::text FROM outbox WHERE alert_id=" + id);
        ContractSchemas.assertWebhook(payload);
        JsonNode p = JSON.readTree(payload);
        assertEquals("acked", p.get("transition").asString());
        assertEquals("ACKED", p.get("status").asString());
        assertEquals(first.get("ackedAt").asString(), p.get("ackedAt").asString());
        assertEquals(first.get("updatedAt").asString(), p.get("occurredAt").asString());
    }

    @Test
    void manualResolveWritesOneOutboxRowAndRefusesSecondCall() throws Exception {
        long open = newAlert("OPEN");
        var res = postJson("/api/v1/alerts/" + open + "/resolve", "{}");
        assertEquals(200, res.statusCode());
        ContractSchemas.assertApi("Alert", res.body());
        JsonNode r = JSON.readTree(res.body());
        assertEquals("RESOLVED", r.get("status").asString());
        assertEquals("operator", r.get("resolvedBy").asString());
        assertEquals(2, r.get("version").asInt());
        assertEquals(1, outboxRows(open));
        ContractSchemas.assertWebhook(scalar("SELECT payload::text FROM outbox WHERE alert_id=" + open));
        assertEquals("alert-" + open + "-resolved", scalar("SELECT idempotency_key FROM outbox WHERE alert_id=" + open));

        for (String action : List.of("resolve", "ack")) {
            var conflict = postJson("/api/v1/alerts/" + open + "/" + action, "{}");
            assertEquals(409, conflict.statusCode(), action);
            assertTrue(conflict.headers().firstValue("content-type").orElse("").startsWith("application/problem+json"));
            ContractSchemas.assertApi("Problem", conflict.body());
            JsonNode prob = JSON.readTree(conflict.body());
            assertEquals("ALERT_RESOLVED", prob.get("code").asString());
            assertEquals("RESOLVED", prob.get("alert").get("status").asString());
            assertEquals(2, prob.get("alert").get("version").asInt());
        }
        assertEquals(1, outboxRows(open));
        assertEquals("2", scalar("SELECT version FROM alert WHERE alert_id=" + open));

        long acked = newAlert("OPEN");
        postJson("/api/v1/alerts/" + acked + "/ack", "{}");
        var res2 = postJson("/api/v1/alerts/" + acked + "/resolve", "{}");
        assertEquals(3, JSON.readTree(res2.body()).get("version").asInt());
        assertEquals("2", scalar("SELECT count(*) FROM outbox WHERE alert_id=" + acked));
        assertEquals("3", scalar("SELECT max(alert_version) FROM outbox WHERE alert_id=" + acked));
    }

    @Test
    void postsNeedAJsonBody() throws Exception {
        long id = newAlert("OPEN");
        String path = "/api/v1/alerts/" + id + "/ack";
        for (var req : List.of(
                post(path, "application/x-www-form-urlencoded", ""),
                post(path, "application/x-www-form-urlencoded", "a=b"),
                post(path, "text/plain", "{}"),
                post(path, null, "{}"),
                post(path, "application/json", ""))) {
            assertEquals(415, req.statusCode());
            assertTrue(req.headers().firstValue("content-type").orElse("").startsWith("application/problem+json"));
            ContractSchemas.assertApi("Problem", req.body());
            assertEquals("UNSUPPORTED_MEDIA_TYPE", JSON.readTree(req.body()).get("code").asString());
        }
        assertEquals(415, post("/api/v1/alerts/" + id + "/resolve", "application/x-www-form-urlencoded", "").statusCode());
        for (String bad : List.of("[]", "\"x\"", "null", "{", "{} {}", "{\"extra\":1}", "{\"note\":5}", "{\"note\":\"" + "x".repeat(201) + "\"}",
                "{\"note\":\"a\",\"note\":\"b\"}")) {
            var res = postJson(path, bad);
            assertEquals(400, res.statusCode(), bad);
            ContractSchemas.assertApi("Problem", res.body());
        }
        assertEquals(413, postJson(path, "{\"note\":\"" + "x".repeat(70000) + "\"}").statusCode());
        assertEquals("OPEN", scalar("SELECT status FROM alert WHERE alert_id=" + id));
        assertEquals(0, outboxRows(id));
        assertEquals(200, postJson(path, "{\"note\":\"" + "x".repeat(200) + "\"}").statusCode());
    }

    @Test
    void ackIsAtomicWithOutboxInsert() throws Exception {
        long id = newAlert("OPEN");
        exec("REVOKE INSERT ON outbox FROM fleetpulse_api");
        try {
            var res = postJson("/api/v1/alerts/" + id + "/ack", "{}");
            assertEquals(500, res.statusCode());
            assertTrue(!res.body().contains("outbox") && !res.body().contains("\tat "));
        } finally {
            exec("GRANT INSERT ON outbox TO fleetpulse_api");
        }
        assertEquals("OPEN", scalar("SELECT status FROM alert WHERE alert_id=" + id));
        assertEquals("1", scalar("SELECT version FROM alert WHERE alert_id=" + id));
        assertEquals(200, postJson("/api/v1/alerts/" + id + "/ack", "{}").statusCode());
    }

    @Test
    void concurrentAcksMakeOneTransition() throws Exception {
        long id = newAlert("OPEN");
        List<java.util.concurrent.Future<Integer>> fs = new ArrayList<>();
        try (var pool = java.util.concurrent.Executors.newFixedThreadPool(8)) {
            for (int i = 0; i < 8; i++) fs.add(pool.submit(() -> postJson("/api/v1/alerts/" + id + "/ack", "{}").statusCode()));
            for (var f : fs) assertEquals(200, f.get());
        }
        assertEquals(1, outboxRows(id));
        assertEquals("2", scalar("SELECT version FROM alert WHERE alert_id=" + id));
    }

    @Test
    void apiRoleCannotTouchAlertIdentityColumns() throws Exception {
        long id = newAlert("OPEN");
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "fleetpulse_api", API_PASSWORD); Statement s = c.createStatement()) {
            assertThrows(SQLException.class, () -> s.execute("UPDATE alert SET device_id='dev-0001' WHERE alert_id=" + id));
            assertThrows(SQLException.class, () -> s.execute("DELETE FROM alert WHERE alert_id=" + id));
            assertThrows(SQLException.class, () -> s.execute("UPDATE outbox SET status='DEAD'"));
        }
    }

    @Test
    void deviceDetailListsItsAlerts() throws Exception {
        long id = newAlert("OPEN");
        String dev = scalar("SELECT device_id FROM alert WHERE alert_id=" + id);
        exec("INSERT INTO device(device_id, state, last_epoch, last_seq, last_seen_at) VALUES ('" + dev + "','FAILED',1,1,now())");
        exec("INSERT INTO alert(device_id, rule_id, status, backfilled, resolved_at, resolved_by) VALUES ('" + dev
                + "','battery_low','RESOLVED', true, now(), 'auto')");
        var res = get("/api/v1/devices/" + dev);
        assertEquals(200, res.statusCode());
        ContractSchemas.assertApi("DeviceDetail", res.body());
        JsonNode alerts = JSON.readTree(res.body()).get("alerts");
        assertEquals(2, alerts.size());
        assertEquals("battery_low", alerts.get(0).get("ruleId").asString());
        assertTrue(alerts.get(0).get("backfilled").asBoolean());
        assertEquals(id, alerts.get(1).get("alertId").asLong());
    }

    @Test
    void streamSendsAlertChangesWithIncreasingVersions() throws Exception {
        long id = newAlert("OPEN");
        try (Sse sse = new Sse()) {
            sse.next("hello");
            notifyAlerts(id);
            JsonNode opened = alertEvent(sse, id);
            assertEquals(1, opened.get("version").asInt());

            assertEquals(200, postJson("/api/v1/alerts/" + id + "/ack", "{}").statusCode());
            JsonNode acked = alertEvent(sse, id);
            assertEquals("ACKED", acked.get("status").asString());
            assertEquals(2, acked.get("version").asInt());

            assertEquals(200, postJson("/api/v1/alerts/" + id + "/resolve", "{}").statusCode());
            JsonNode resolved = alertEvent(sse, id);
            assertEquals("RESOLVED", resolved.get("status").asString());
            assertEquals(3, resolved.get("version").asInt());
        }
    }

    static void notifyAlerts(long... ids) throws SQLException {
        exec("SELECT pg_notify('fleetpulse_changes', '{\"t\":\"a\",\"ids\":[" + java.util.Arrays.stream(ids).mapToObj(Long::toString)
                .collect(java.util.stream.Collectors.joining(",")) + "]}')");
    }

    JsonNode alertEvent(Sse sse, long id) throws Exception {
        for (int i = 0; i < 20; i++) {
            Event e = sse.next("alerts");
            ContractSchemas.assertSse("alerts", e.data);
            for (JsonNode item : JSON.readTree(e.data).get("items")) {
                ContractSchemas.assertApi("Alert", item.toString());
                if (item.get("alertId").asLong() == id) return item;
            }
        }
        throw new AssertionError("no alerts event for " + id);
    }

    record Event(String name, String data) {}

    final class Sse implements AutoCloseable {
        final BlockingQueue<String> raw = new LinkedBlockingQueue<>();
        final BlockingQueue<Event> events = new LinkedBlockingQueue<>();
        final List<String> comments = new java.util.concurrent.CopyOnWriteArrayList<>();
        final Thread reader;
        final java.io.InputStream in;

        Sse() throws Exception {
            var res = HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/stream"))
                    .header("Accept", "text/event-stream").build(), HttpResponse.BodyHandlers.ofInputStream());
            assertEquals(200, res.statusCode());
            assertTrue(res.headers().firstValue("content-type").orElse("").startsWith("text/event-stream"));
            in = res.body();
            reader = Thread.ofVirtual().start(() -> {
                try (var br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String name = null;
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.startsWith(":")) comments.add(line);
                        else if (line.startsWith("retry:")) raw.add(line);
                        else if (line.startsWith("event:")) name = line.substring(6).trim();
                        else if (line.startsWith("data:") && name != null) {
                            events.add(new Event(name, line.substring(5).trim()));
                            name = null;
                        }
                    }
                } catch (Exception ignored) {
                    // closed by test
                }
            });
        }

        Event next(String name) throws InterruptedException {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < end) {
                Event e = events.poll(200, TimeUnit.MILLISECONDS);
                if (e != null && e.name.equals(name)) return e;
            }
            throw new AssertionError("no " + name + " event");
        }

        boolean sawComment(String text) throws InterruptedException {
            for (int i = 0; i < 40; i++) {
                if (comments.contains(": " + text)) return true;
                Thread.sleep(100);
            }
            return false;
        }

        @Override
        public void close() throws Exception {
            in.close();
        }
    }
}
