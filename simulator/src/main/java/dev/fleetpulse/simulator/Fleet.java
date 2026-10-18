package dev.fleetpulse.simulator;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
class Fleet implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Fleet.class);
    private static final long TICK_MS = 20;

    private final SimProperties props;
    private final KafkaTemplate<String, String> kafka;
    private final Clock clock;
    private final long epoch;
    private final long hbNanos;
    private final long telNanos;
    private final List<Device> devices = new ArrayList<>();
    private final long[] nextHb;
    private final long[] nextTel;
    private final AtomicLong sendFailed = new AtomicLong();
    private ScheduledExecutorService exec;

    Fleet(SimProperties props, KafkaTemplate<String, String> kafka) {
        this.props = props;
        this.kafka = kafka;
        this.clock = Clock.systemUTC();
        this.epoch = props.epoch() != null ? props.epoch() : clock.millis();
        this.hbNanos = props.heartbeatInterval().toNanos();
        this.telNanos = props.telemetryInterval().toNanos();
        for (int i = 1; i <= props.deviceCount(); i++) {
            devices.add(new Device(i, props.seed(), epoch, hbNanos, telNanos));
        }
        nextHb = new long[devices.size()];
        nextTel = new long[devices.size()];
    }

    @Override
    public void start() {
        long t0 = System.nanoTime();
        for (int i = 0; i < devices.size(); i++) {
            nextHb[i] = t0 + devices.get(i).heartbeatPhaseNanos;
            nextTel[i] = t0 + devices.get(i).telemetryPhaseNanos;
        }
        exec = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "fleet"));
        exec.scheduleAtFixedRate(this::tick, 0, TICK_MS, TimeUnit.MILLISECONDS);
        exec.scheduleAtFixedRate(this::reportFailures, 10, 10, TimeUnit.SECONDS);
        log.info("fleet started: devices={} epoch={} seed={}", devices.size(), epoch, props.seed());
    }

    @Override
    public void stop() {
        if (exec != null) {
            exec.shutdownNow();
            exec = null;
        }
    }

    @Override
    public boolean isRunning() {
        return exec != null;
    }

    private void tick() {
        try {
            long now = System.nanoTime();
            for (int i = 0; i < devices.size(); i++) {
                while (nextHb[i] <= now) {
                    send(devices.get(i), "heartbeat");
                    nextHb[i] += hbNanos;
                }
                while (nextTel[i] <= now) {
                    send(devices.get(i), "telemetry");
                    nextTel[i] += telNanos;
                }
            }
        } catch (RuntimeException e) {
            sendFailed.incrementAndGet();
        }
    }

    private void send(Device d, String type) {
        Device.Event e = d.next(type, clock.instant());
        kafka.send(props.topic(), e.deviceId(), e.json()).whenComplete((r, ex) -> {
            if (ex != null) {
                sendFailed.incrementAndGet();
            }
        });
    }

    private long reported;

    private void reportFailures() {
        long n = sendFailed.get();
        if (n != reported) {
            log.warn("send failures so far: {}", n);
            reported = n;
        }
    }
}
