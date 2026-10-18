package com.fleetpulse.api;

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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "fp.sse.heartbeat-interval=1s")
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
