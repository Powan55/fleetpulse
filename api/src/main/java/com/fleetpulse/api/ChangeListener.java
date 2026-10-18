package com.fleetpulse.api;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Component
public class ChangeListener {
    private static final Logger log = LoggerFactory.getLogger(ChangeListener.class);
    private static final Pattern DEVICE_ID = Pattern.compile("^dev-[0-9]{4,6}$");
    private static final Pattern CHANNEL = Pattern.compile("^[a-z_][a-z0-9_]{0,62}$");
    private static final int MAX_PAYLOAD = 8000;
    private static final int MAX_IDS = 200;
    private static final JsonMapper JSON = JsonMapper.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build();

    // devices == null: dropped. Alert notifications parse fine but M1 has no alert stream.
    record Parsed(List<String> devices) {}

    private final StreamHub hub;
    private final String url;
    private final String user;
    private final String password;
    private final String channel;
    private final Counter rejected;
    private final Counter reconnects;
    private volatile boolean running = true;
    private Thread thread;

    public ChangeListener(StreamHub hub, MeterRegistry meters,
                          @Value("${spring.datasource.url}") String url,
                          @Value("${spring.datasource.username}") String user,
                          @Value("${spring.datasource.password}") String password,
                          @Value("${fp.notify-channel}") String channel) {
        if (!CHANNEL.matcher(channel).matches()) throw new IllegalArgumentException("bad notify channel");
        this.hub = hub;
        this.url = url;
        this.user = user;
        this.password = password;
        this.channel = channel;
        this.rejected = meters.counter("fleetpulse.api.notify.rejected");
        this.reconnects = meters.counter("fleetpulse.api.notify.reconnects");
    }

    static Parsed parse(String payload) {
        try {
            if (payload == null || payload.length() > MAX_PAYLOAD) return null;
            JsonNode root = JSON.readTree(payload);
            JsonNode ids = root.get("ids");
            if (!root.isObject() || root.size() != 2 || ids == null || !ids.isArray() || ids.size() == 0 || ids.size() > MAX_IDS) return null;
            String t = root.path("t").asString("");
            List<String> out = new ArrayList<>();
            for (JsonNode id : ids) {
                if (t.equals("d") && id.isString() && DEVICE_ID.matcher(id.asString()).matches()) out.add(id.asString());
                else if (t.equals("a") && id.isIntegralNumber() && id.canConvertToLong() && id.asLong() >= 1) continue;
                else return null;
            }
            return new Parsed(out);
        } catch (RuntimeException e) {
            return null;
        }
    }

    @PostConstruct
    void start() {
        thread = Thread.ofPlatform().name("pg-listen").daemon().start(this::loop);
    }

    @PreDestroy
    void stop() {
        running = false;
        thread.interrupt();
    }

    private void loop() {
        boolean lost = false;
        long backoff = 500;
        while (running) {
            try (Connection c = DriverManager.getConnection(url, user, password)) {
                try (Statement s = c.createStatement()) {
                    s.execute("LISTEN " + channel);
                }
                PGConnection pg = c.unwrap(PGConnection.class);
                if (lost) {
                    hub.resync();
                    lost = false;
                }
                backoff = 500;
                while (running) {
                    PGNotification[] ns = pg.getNotifications(500);
                    if (ns == null) continue;
                    for (PGNotification n : ns) {
                        Parsed p = parse(n.getParameter());
                        if (p == null) {
                            rejected.increment();
                            log.warn("dropped notification, {} chars", n.getParameter() == null ? 0 : n.getParameter().length());
                        } else if (!p.devices().isEmpty()) {
                            hub.deviceChanged(p.devices());
                        }
                    }
                }
            } catch (Exception e) {
                if (!running) return;
                lost = true;
                reconnects.increment();
                log.warn("notify listener lost, retrying in {} ms: {}", backoff, e.toString());
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    return;
                }
                backoff = Math.min(backoff * 2, 10_000);
            }
        }
    }
}
