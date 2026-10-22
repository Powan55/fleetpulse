package dev.fleetpulse.simulator;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
class Fleet implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Fleet.class);
    private static final long TICK_MS = 20;
    private static final long PAUSE_WAIT_MS = 30_000;
    private static final long RETRY_NANOS = 1_000_000_000L;
    private static final int MAX_ACTIVE = 1000;

    record Disc(String id, int[] devices, long untilNanos, Instant until) {}

    private record Failed(int device, Device.Event event) {}

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
    private final List<Integer> pickOrder = new ArrayList<>();
    private final int capacity;
    private final double maxRate;
    private final List<TreeMap<Long, Device.Event>> queue = new ArrayList<>();
    private final long[] discUntil;
    private final long[] retryAfter;
    private final boolean[] wasDisc;
    private final ConcurrentLinkedQueue<Failed> failed = new ConcurrentLinkedQueue<>();
    private final List<Fault> faults = new ArrayList<>();
    private final Map<String, Disc> discs = new LinkedHashMap<>();
    private volatile long nowNanos;
    private long lastPump;
    private double bucket;
    private double replayTokens;
    private int rr;
    private int queued;
    private long faultSeq;
    private long discSeq;
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
        pickOrder.addAll(skewOrder);
        Collections.shuffle(pickOrder, new Random(props.seed() ^ 0xFA17L));
        capacity = props.bufferCapacity();
        maxRate = props.maxSendRate();
        discUntil = new long[devices.size()];
        retryAfter = new long[devices.size()];
        wasDisc = new boolean[devices.size()];
        for (int i = 0; i < devices.size(); i++) {
            queue.add(new TreeMap<>());
        }
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
        nowNanos = t0;
        lastPump = t0;
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
                nowNanos = now;
                double dt = Math.max(0, now - lastPump) / 1e9;
                lastPump = now;
                requeueFailures(now);
                expire(now);
                reconnects(now);
                pump(now, dt);
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
        Device.Event e = d.next(type, clock.instant(), skew, "telemetry".equals(type) ? faultLevels(i) : null);
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
        if (held[i] == null && reorder < ch.reorderRate() && !disconnected(i, now)) {
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
        if (disconnected(i, nowNanos) || !queue.get(i).isEmpty()) {
            enqueue(i, e);
            return;
        }
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
        bucket--;
        try {
            kafka.send(props.topic(), e.deviceId(), e.json()).whenComplete((r, ex) -> done(i, e, primary, ex));
        } catch (RuntimeException ex) {
            done(i, e, primary, ex);
        }
    }

    private void done(int i, Device.Event e, boolean primary, Throwable ex) {
        if (ex == null) {
            ledger.add(i, e.seq(), clock.instant());
            if (primary) {
                counters.buffered.decrementAndGet();
            }
        } else {
            counters.sendFailed.incrementAndGet();
            if (primary) {
                failed.add(new Failed(i, e));
            }
        }
        inflight.decrementAndGet();
    }

    private boolean disconnected(int i, long now) {
        return discUntil[i] > now;
    }

    /** Drop-oldest bounded queue; a dropped event is never sent, so it never reaches the ledger. */
    private void enqueue(int i, Device.Event e) {
        TreeMap<Long, Device.Event> q = queue.get(i);
        q.put(e.seq(), e);
        queued++;
        if (q.size() > capacity) {
            q.pollFirstEntry();
            queued--;
            counters.bufferOverflow.incrementAndGet();
            counters.buffered.decrementAndGet();
        }
    }

    private void requeueFailures(long now) {
        for (Failed f; (f = failed.poll()) != null; ) {
            retryAfter[f.device()] = now + RETRY_NANOS;
            enqueue(f.device(), f.event());
        }
    }

    private void expire(long now) {
        faults.removeIf(f -> f.untilNanos != null && now >= f.untilNanos);
        discs.values().removeIf(d -> now >= d.untilNanos());
    }

    private void reconnects(long now) {
        for (int i = 0; i < devices.size(); i++) {
            if (disconnected(i, now)) {
                wasDisc[i] = true;
            } else if (wasDisc[i]) {
                wasDisc[i] = false;
                if (!chaos.replayOnReconnect()) {
                    TreeMap<Long, Device.Event> q = queue.get(i);
                    counters.bufferOverflow.addAndGet(q.size());
                    counters.buffered.addAndGet(-q.size());
                    queued -= q.size();
                    q.clear();
                }
            }
        }
    }

    /** One queued event per device per turn, seq order within a device, at the leftover send budget. */
    private void pump(long now, double dt) {
        bucket = Math.min(Math.max(1, maxRate * 0.1), bucket + maxRate * dt);
        Long cap = chaos.replayMaxRate();
        if (cap != null) {
            replayTokens = Math.min(Math.max(1, cap * 0.1), replayTokens + cap * dt);
        }
        int n = devices.size();
        int start = rr;
        boolean progress = queued > 0;
        while (progress && budget(cap)) {
            progress = false;
            for (int k = 0; k < n && queued > 0 && budget(cap); k++) {
                int i = (start + k) % n;
                TreeMap<Long, Device.Event> q = queue.get(i);
                if (q.isEmpty() || disconnected(i, now) || retryAfter[i] > now) {
                    continue;
                }
                Device.Event e = q.pollFirstEntry().getValue();
                queued--;
                counters.replayed.incrementAndGet();
                maxSent[i] = Math.max(maxSent[i], e.seq());
                replayTokens--;
                send(i, e.asReplay(), true);
                rr = (i + 1) % n;
                progress = true;
            }
        }
    }

    private boolean budget(Long cap) {
        return bucket >= 1 && (cap == null || replayTokens >= 1);
    }

    private Fault.Level[] faultLevels(int i) {
        if (faults.isEmpty()) {
            return null;
        }
        Fault.Level[] lv = new Fault.Level[3];
        for (Fault f : faults) {
            if (java.util.Arrays.binarySearch(f.devices, i) >= 0) {
                Fault.Level l = f.next(i);
                int m = metricIndex(f.metric);
                if (lv[m] == null) {
                    lv[m] = l;
                }
            }
        }
        return lv;
    }

    private static int metricIndex(String metric) {
        return switch (metric) {
            case "temperature" -> 0;
            case "battery" -> 1;
            default -> 2;
        };
    }

    /** Deterministic from the seed: the same fraction always picks the same devices. */
    int[] pick(double fraction) {
        int n = Math.max(1, (int) Math.round(fraction * devices.size()));
        return pickOrder.stream().limit(n).mapToInt(Integer::intValue).sorted().toArray();
    }

    Fault addFault(String metric, Fault.Level level, List<Fault.Step> steps, Long durationMs, int[] devs) {
        synchronized (lock) {
            if (faults.size() >= MAX_ACTIVE) {
                throw new SimException(HttpStatus.CONFLICT, "too many active faults");
            }
            Fault f = new Fault("fault-" + (++faultSeq), metric, level, steps, devs, devices.size(),
                    durationMs == null ? null : nowNanos + durationMs * 1_000_000L,
                    durationMs == null ? null : clock.instant().plusMillis(durationMs));
            faults.add(f);
            return f;
        }
    }

    List<Fault> faults() {
        synchronized (lock) {
            return List.copyOf(faults);
        }
    }

    boolean removeFault(String id) {
        synchronized (lock) {
            return faults.removeIf(f -> f.id.equals(id));
        }
    }

    void clearFaults() {
        synchronized (lock) {
            faults.clear();
        }
    }

    Disc startDisconnect(int[] devs, long durationMs) {
        synchronized (lock) {
            if (discs.size() >= MAX_ACTIVE) {
                throw new SimException(HttpStatus.CONFLICT, "too many active disconnects");
            }
            long until = nowNanos + durationMs * 1_000_000L;
            for (int i : devs) {
                discUntil[i] = Math.max(discUntil[i], until);
            }
            Disc d = new Disc("disconnect-" + (++discSeq), devs, until, clock.instant().plusMillis(durationMs));
            discs.put(d.id(), d);
            return d;
        }
    }

    List<Disc> disconnects() {
        synchronized (lock) {
            return List.copyOf(discs.values());
        }
    }

    void reconnectAll() {
        synchronized (lock) {
            java.util.Arrays.fill(discUntil, 0);
            discs.clear();
        }
    }

    int disconnectedDevices() {
        int c = 0;
        long now = nowNanos;
        for (long u : discUntil) {
            if (u > now) {
                c++;
            }
        }
        return c;
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
