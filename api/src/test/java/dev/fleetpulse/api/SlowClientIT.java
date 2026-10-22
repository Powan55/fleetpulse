package dev.fleetpulse.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SlowClientIT {
    private static final String API_PASSWORD = "api-test-pw";
    private static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:18.6");

    @LocalServerPort int port;
    @Autowired StreamHub hub;

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

    @AfterAll
    static void stopDb() {
        PG.stop();
    }

    static Connection admin() throws SQLException {
        return DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    }

    @Test
    void clientThatStopsReadingIsDroppedAndOthersKeepReceiving() throws Exception {
        try (Connection c = admin(); PreparedStatement ps = c.prepareStatement(
                "INSERT INTO device(device_id, state, last_epoch, last_seq, last_seen_at) VALUES (?, 'HEALTHY', 1, 1, now())")) {
            for (int i = 1; i <= 200 * 5; i++) {
                ps.setString(1, "dev-%04d".formatted(i));
                ps.addBatch();
            }
            ps.executeBatch();
        }

        try (Socket slow = new Socket()) {
            slow.setReceiveBufferSize(1024);
            slow.connect(new InetSocketAddress("localhost", port));
            slow.getOutputStream().write(("GET /api/v1/stream HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            slow.getOutputStream().flush();

            var res = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/stream"))
                    .header("Accept", "text/event-stream").build(), HttpResponse.BodyHandlers.ofInputStream());
            AtomicInteger deviceEvents = new AtomicInteger();
            Thread.ofVirtual().start(() -> {
                try (var br = new BufferedReader(new InputStreamReader(res.body(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) if (line.equals("event: devices") || line.equals("event:devices")) deviceEvents.incrementAndGet();
                } catch (Exception ignored) {
                    // closed by test
                }
            });
            for (int i = 0; i < 100 && hub.clientCount() < 2; i++) Thread.sleep(100);
            assertEquals(2, hub.clientCount());

            long end = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(90);
            try (Connection c = admin(); Statement s = c.createStatement()) {
                while (hub.clientCount() > 1 && System.nanoTime() < end) {
                    for (int k = 0; k < 5; k++) {
                        String batch = String.join("\",\"", java.util.stream.IntStream.rangeClosed(k * 200 + 1, k * 200 + 200)
                                .mapToObj(i -> "dev-%04d".formatted(i)).toList());
                        s.execute("SELECT pg_notify('fleetpulse_changes', '{\"t\":\"d\",\"ids\":[\"" + batch + "\"]}')");
                    }
                    Thread.sleep(300);
                }
            }
            assertEquals(1, hub.clientCount(), "slow client not dropped");
            int before = deviceEvents.get();
            try (Connection c = admin(); Statement s = c.createStatement()) {
                s.execute("SELECT pg_notify('fleetpulse_changes', '{\"t\":\"d\",\"ids\":[\"dev-0001\"]}')");
            }
            for (int i = 0; i < 50 && deviceEvents.get() == before; i++) Thread.sleep(100);
            assertTrue(deviceEvents.get() > before, "healthy client stopped receiving");
            res.body().close();
        }
    }
}
