package dev.fleetpulse.simulator;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
    private static final long PAUSE_WAIT_MS = 30_000;

    private final SimProperties props;
    private final KafkaTemplate<String, String> kafka;
    private final Clock clock;
    private final long epoch;
    private final long hbNanos;
    private final long telNanos;
    private final List<Device> devices = new ArrayList<>();
    private final long[] nextHb;
    private final long[] nextTel;
    private final Device.Event[] held;
    private final long[] heldAt;
    private final long[] maxSent;
    private final long holdNanos;
    private final List<Integer> skewOrder = new ArrayList<>();
    private final Object lock = new Object();
    private final AtomicLong inflight = new AtomicLong();
    private final AtomicLong malformedSeq = new AtomicLong();
    final Counters counters = new Counters();
    final Ledger ledger;
    private volatile Chaos chaos = Chaos.OFF;
    private volatile long[] skewMs;
    private volatile boolean paused;
    private boolean reschedule;
    private long lastTick;
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
            skewOrder.add(i - 1);
        }
        Collections.shuffle(skewOrder, new Random(props.seed() ^ 0x5EEDL));
        nextHb = new long[devices.size()];
        nextTel = new long[devices.size()];
        held = new Device.Event[devices.size()];
        heldAt = new long[devices.size()];
        maxSent = new long[devices.size()];
        holdNanos = props.reorderHold().toNanos();
        skewMs = new long[devices.size()];
        ledger = new Ledger(devices.size());
    }

    @Override
    public void start() {
        init(System.nanoTime());
        exec = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "fleet"));
        exec.scheduleAtFixedRate(() -> tick(System.nanoTime()), 0, TICK_MS, TimeUnit.MILLISECONDS);
        exec.scheduleAtFixedRate(this::reportFailures, 10, 10, TimeUnit.SECONDS);
        log.info("fleet started: devices={} epoch={} seed={}", devices.size(), epoch, props.seed());
    }

    void init(long t0) {
        schedule(t0);
    }

    private void schedule(long t0) {
        for (int i = 0; i < devices.size(); i++) {
            nextHb[i] = t0 + devices.get(i).heartbeatPhaseNanos;
            nextTel[i] = t0 + devices.get(i).telemetryPhaseNanos;
        }
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

    long epoch() {
        return epoch;
    }

    long seed() {
        return props.seed();
    }

    int deviceCount() {
        return devices.size();
    }

    String deviceId(int index) {
        return devices.get(index).id;
    }

    /** Zero-based index of a canonical device id, or -1. */
    int indexOf(String id) {
        if (id == null || !id.matches("dev-[0-9]{4,6}")) {
            return -1;
        }
        int n = Integer.parseInt(id.substring(4));
        return n >= 1 && n <= devices.size() && devices.get(n - 1).id.equals(id) ? n - 1 : -1;
    }

    Chaos chaos() {
        return chaos;
    }

    void setChaos(Chaos c) {
        long[] skew = new long[devices.size()];
        int at = 0;
        for (Chaos.Skew s : c.skewSets()) {
            int n = (int) Math.round(s.fraction() * devices.size());
            for (int k = 0; k < n && at < skewOrder.size(); k++) {
                skew[skewOrder.get(at++)] = s.offsetMs();
            }
        }
        synchronized (lock) {
            skewMs = skew;
            chaos = c;
        }
    }

    boolean paused() {
        return paused;
    }

    /** Pausing returns once every send is acked or failed, at most 30 s. */
    void setPaused(boolean p) {
        synchronized (lock) {
            if (p == paused) {
                return;
            }
            paused = p;
            if (!p) {
                reschedule = true;
                return;
            }
            for (int i = 0; i < held.length; i++) {
                release(i);
            }
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(PAUSE_WAIT_MS);
        while (inflight.get() > 0 && System.nanoTime() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    void tick(long now) {
        try {
            synchronized (lock) {
                if (paused) {
                    return;
                }
                if (reschedule) {
                    long shift = now - lastTick;
                    for (int i = 0; i < nextHb.length; i++) {
                        nextHb[i] += shift;
                        nextTel[i] += shift;
                    }
                    reschedule = false;
                }
                lastTick = now;
                for (int i = 0; i < devices.size(); i++) {
                    if (held[i] != null && now - heldAt[i] >= holdNanos) {
                        release(i);
                    }
                    // Earlier nominal time first (heartbeat on ties), so the type of each seq depends only on the seed.
                    while (Math.min(nextHb[i], nextTel[i]) <= now) {
                        if (nextHb[i] <= nextTel[i]) {
                            generate(i, "heartbeat", now);
                            nextHb[i] += hbNanos;
                        } else {
                            generate(i, "telemetry", now);
                            nextTel[i] += telNanos;
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            counters.sendFailed.incrementAndGet();
        }
    }

    // Draw order per device is fixed (drop, reorder, then dup per transmit) so a seed replays the same decisions.
    private void generate(int i, String type, long now) {
        Device d = devices.get(i);
        Chaos ch = chaos;
        long skew = skewMs[i];
        Device.Event e = d.next(type, clock.instant(), skew);
        counters.generated.incrementAndGet();
        if (skew != 0) {
            counters.skewedEvents.incrementAndGet();
        }
        double drop = d.chaos.nextDouble();
        double reorder = d.chaos.nextDouble();
        if (drop < ch.dropRate()) {
            counters.dropped.incrementAndGet();
            return;
        }
        counters.buffered.incrementAndGet();
        if (held[i] == null && reorder < ch.reorderRate()) {
            held[i] = e;
            heldAt[i] = now;
            return;
        }
        transmit(i, e);
    }

    /** Sends the held event; it counts as reordered when a later seq already went out. */
    private void release(int i) {
        Device.Event e = held[i];
        if (e == null) {
            return;
        }
        held[i] = null;
        if (maxSent[i] > e.seq()) {
            counters.reordered.incrementAndGet();
        }
        transmit(i, e);
    }

    private void transmit(int i, Device.Event e) {
        maxSent[i] = Math.max(maxSent[i], e.seq());
        send(i, e, true);
        if (devices.get(i).chaos.nextDouble() < chaos.duplicateRate()) {
            counters.injectedDups.incrementAndGet();
            send(i, e, false);
        }
    }

    private void send(int i, Device.Event e, boolean primary) {
        inflight.incrementAndGet();
        counters.sent.incrementAndGet();
        try {
            kafka.send(props.topic(), e.deviceId(), e.json()).whenComplete((r, ex) -> done(i, e, primary, ex));
        } catch (RuntimeException ex) {
            done(i, e, primary, ex);
        }
    }

    private void done(int i, Device.Event e, boolean primary, Throwable ex) {
        if (ex == null) {
            ledger.add(i, e.seq(), clock.instant());
        } else {
            counters.sendFailed.incrementAndGet();
        }
        if (primary) {
            counters.buffered.decrementAndGet();
        }
        inflight.decrementAndGet();
    }

    /** Sends count malformed records of one kind; returns how many Kafka acked. */
    int injectMalformed(String kind, int count) {
        List<CompletableFuture<?>> fs = new ArrayList<>();
        for (int k = 0; k < count; k++) {
            long n = malformedSeq.getAndIncrement();
            String key = devices.get((int) (n % devices.size())).id;
            String id = UUID.nameUUIDFromBytes(("malformed:" + n).getBytes(StandardCharsets.UTF_8)).toString();
            fs.add(kafka.send(props.topic(), key, malformed(kind, key, id)));
        }
        int ok = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        for (CompletableFuture<?> f : fs) {
            try {
                f.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                ok++;
            } catch (Exception e) {
                counters.sendFailed.incrementAndGet();
            }
        }
        counters.malformedInjected.addAndGet(ok);
        return ok;
    }

    private String malformed(String kind, String key, String id) {
        String head = "{\"schema_version\":1,\"event_id\":\"" + id + "\",\"device_id\":\"" + key + "\",\"epoch\":" + epoch;
        String tail = ",\"event_time\":\"2026-01-01T00:00:00.000Z\",\"type\":\"heartbeat\",\"replay\":false";
        return switch (kind) {
            case "invalid_json" -> head + ",\"seq\":1";
            case "missing_field" -> head + ",\"seq\":1" + tail + "}";
            case "bad_type" -> head + ",\"seq\":\"one\"" + tail + ",\"payload\":{}}";
            case "oversize" -> head + ",\"seq\":1" + tail + ",\"payload\":{\"pad\":\"" + "x".repeat(1100) + "\"}}";
            default -> throw new IllegalArgumentException(kind);
        };
    }

    private long reported;

    private void reportFailures() {
        long n = counters.sendFailed.get();
        if (n != reported) {
            log.warn("send failures so far: {}", n);
            reported = n;
        }
    }
}
