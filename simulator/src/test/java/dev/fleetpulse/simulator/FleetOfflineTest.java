package dev.fleetpulse.simulator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class FleetOfflineTest {

    private static final long SEC = 1_000_000_000L;
    private static final Pattern SEQ = Pattern.compile("\"seq\":(\\d+)");
    private static final Pattern ID = Pattern.compile("\"event_id\":\"([^\"]+)\"");
    private static final Pattern REPLAY = Pattern.compile("\"replay\":(true|false)");
    private static final Pattern TEMP = Pattern.compile("\"temperature_c\":([0-9.]+)");

    private final FleetTest base = new FleetTest();

    private static String find(Pattern p, String s) {
        Matcher m = p.matcher(s);
        assertTrue(m.find(), s);
        return m.group(1);
    }

    private static void run(Fleet f, long fromMs, long toMs, long stepMs) {
        for (long t = fromMs; t <= toMs; t += stepMs) {
            f.tick(t * SEC / 1000);
        }
    }

    private static int[] all(Fleet f) {
        return IntStream.range(0, f.deviceCount()).toArray();
    }

    private List<FleetTest.Sent> forDevice(String id) {
        return base.sent().stream().filter(s -> s.key().equals(id)).toList();
    }

    @Test
    void patternYieldsItsStepsInOrderAndRepeats() {
        Fault f = new Fault("x", "temperature", null,
                List.of(new Fault.Step(Fault.Level.DEGRADED, 3), new Fault.Step(Fault.Level.OK, 2), new Fault.Step(Fault.Level.FAILED, 1)),
                new int[] {0}, 1, null, null);
        List<Fault.Level> got = new ArrayList<>();
        for (int k = 0; k < 12; k++) {
            got.add(f.next(0));
        }
        Fault.Level d = Fault.Level.DEGRADED;
        Fault.Level o = Fault.Level.OK;
        Fault.Level x = Fault.Level.FAILED;
        assertEquals(List.of(d, d, d, o, o, x, d, d, d, o, o, x), got);
    }

    @Test
    void faultValuesFollowThePatternAndStayDeterministic() {
        List<List<String>> runs = new ArrayList<>();
        for (int r = 0; r < 2; r++) {
            FleetTest t = new FleetTest();
            Fleet f = t.fleet(3, 5, 400);
            f.addFault("temperature", null,
                    List.of(new Fault.Step(Fault.Level.DEGRADED, 3), new Fault.Step(Fault.Level.OK, 2)), null, new int[] {1});
            run(f, 1000, 40_000, 1000);
            List<String> mine = t.sent().stream().filter(s -> s.key().equals("dev-0002")).map(FleetTest.Sent::value).toList();
            runs.add(t.sent().stream().map(s -> s.key() + s.value().replaceAll("\"event_time\":\"[^\"]+\"", "")).toList());
            List<String> temps = mine.stream().filter(v -> v.contains("temperature_c")).map(v -> find(TEMP, v)).toList();
            assertTrue(temps.size() >= 10, "samples " + temps.size());
            for (int k = 0; k < temps.size(); k++) {
                double v = Double.parseDouble(temps.get(k));
                if (k % 5 < 3) {
                    assertTrue(v >= 73 && v <= 77, k + " " + v);
                } else {
                    assertTrue(v >= 20 && v <= 50, k + " " + v);
                }
            }
            for (String v : t.sent().stream().filter(s -> !s.key().equals("dev-0002")).map(FleetTest.Sent::value).toList()) {
                if (v.contains("temperature_c")) {
                    assertTrue(Double.parseDouble(find(TEMP, v)) <= 50);
                }
            }
        }
        assertEquals(runs.get(0), runs.get(1));
    }

    @Test
    void faultsDoNotChangeOtherMetricsOrEventIds() {
        FleetTest a = new FleetTest();
        Fleet fa = a.fleet(2, 9, 400);
        run(fa, 1000, 20_000, 1000);
        FleetTest b = new FleetTest();
        Fleet fb = b.fleet(2, 9, 400);
        fb.addFault("battery", Fault.Level.FAILED, null, null, new int[] {0, 1});
        run(fb, 1000, 20_000, 1000);
        assertEquals(a.sent().size(), b.sent().size());
        for (int i = 0; i < a.sent().size(); i++) {
            String x = a.sent().get(i).value();
            String y = b.sent().get(i).value();
            assertEquals(find(ID, x), find(ID, y));
            if (x.contains("temperature_c")) {
                assertEquals(find(TEMP, x), find(TEMP, y));
                assertTrue(y.contains("\"battery_pct\":2.") || y.contains("\"battery_pct\":1.") || y.contains("\"battery_pct\":3."), y);
            }
        }
    }

    @Test
    void timedFaultExpires() {
        Fleet f = base.fleet(2, 5, 400);
        f.addFault("errors", Fault.Level.FAILED, null, 5000L, new int[] {0});
        assertEquals(1, f.faults().size());
        run(f, 1000, 6000, 1000);
        assertEquals(0, f.faults().size());
    }

    @Test
    void disconnectedDevicesSendNothingThenReplayInOrderBeforeLive() {
        Fleet f = base.fleet(5, 42, 400);
        f.setChaos(new Chaos(0, 0, 0, List.of(), true, null));
        run(f, 1000, 3000, 1000);
        int before = base.sent().size();
        int[] pre = new int[5];
        for (int i = 0; i < 5; i++) {
            pre[i] = forDevice(f.deviceId(i)).size();
        }
        f.startDisconnect(all(f), 20_000);
        assertEquals(5, f.disconnectedDevices());
        run(f, 4000, 20_000, 1000);
        assertEquals(before, base.sent().size());
        assertTrue((long) f.counters.view(0, 0).get("bufferedEvents") > 50);

        run(f, 23_000, 40_000, 100);
        assertEquals(0, f.disconnectedDevices());
        long replayed = 0;
        for (int i = 0; i < 5; i++) {
            String id = f.deviceId(i);
            long lastSeq = pre[i];
            boolean live = false;
            for (FleetTest.Sent s : forDevice(id).subList(pre[i], forDevice(id).size())) {
                long seq = Long.parseLong(find(SEQ, s.value()));
                boolean isReplay = Boolean.parseBoolean(find(REPLAY, s.value()));
                assertTrue(seq > lastSeq, id + " seq order " + lastSeq + " then " + seq);
                lastSeq = seq;
                assertEquals(UUID.nameUUIDFromBytes(("42:" + id + ":1000:" + seq).getBytes(StandardCharsets.UTF_8)).toString(),
                        find(ID, s.value()));
                if (isReplay) {
                    replayed++;
                    assertFalse(live, id + " replay after live");
                } else {
                    live = true;
                }
            }
            assertEquals(lastSeq, f.ledger.entry(i).count(), id + " no gaps");
        }
        assertTrue(replayed > 50);
        assertEquals(replayed, (long) f.counters.view(0, 0).get("replayed"));
    }

    @Test
    void replayIsRoundRobinAndRateCapped() {
        Fleet f = base.fleet(10, 42, 400);
        f.setChaos(new Chaos(0, 0, 0, List.of(), true, 50L));
        f.startDisconnect(all(f), 10_000);
        run(f, 1000, 10_000, 1000);
        base.sent().clear();
        run(f, 11_000, 11_000, 1000);
        run(f, 11_100, 12_000, 100);
        List<FleetTest.Sent> first = base.sent();
        assertTrue(first.size() <= 80, "capped at about 50 per second, got " + first.size());
        assertTrue(first.size() >= 10);
        List<String> turn = first.subList(0, 10).stream().map(FleetTest.Sent::key).distinct().toList();
        assertEquals(10, turn.size(), "first turn visits every device once");
    }

    @Test
    void overflowDropsOldestAndNeverReachesTheLedger() {
        Fleet f = base.fleet(3, 1, 10);
        f.setChaos(new Chaos(0, 0, 0, List.of(), true, null));
        f.startDisconnect(all(f), 30_000);
        run(f, 1000, 29_000, 1000);
        Map<String, Object> c = f.counters.view(0, 0);
        long generated = (long) c.get("generated");
        assertEquals(generated - 30, (long) c.get("bufferOverflow"));
        assertEquals(30L, (long) c.get("bufferedEvents"));
        f.setPaused(true);
        run(f, 31_000, 40_000, 100);
        for (int i = 0; i < 3; i++) {
            assertEquals(10, f.ledger.entry(i).count(), f.deviceId(i));
            long[] r = f.ledger.ranges(i).get(f.ledger.ranges(i).size() - 1);
            assertTrue(r[1] - r[0] >= 9);
        }
        f.setPaused(true);
        c = f.counters.view(f.ledger.total(), 0);
        assertEquals((long) c.get("generated"), (long) c.get("accepted") + (long) c.get("dropped")
                + (long) c.get("bufferOverflow") + (long) c.get("bufferedEvents"), c.toString());
        assertEquals((long) c.get("sent"), (long) c.get("accepted") + (long) c.get("injectedDups"));
    }

    @Test
    void discardOnReconnectCountsAsOverflow() {
        Fleet f = base.fleet(2, 42, 400);
        f.setChaos(new Chaos(0, 0, 0, List.of(), false, null));
        f.startDisconnect(all(f), 5000);
        run(f, 1000, 5000, 1000);
        run(f, 6000, 8000, 1000);
        Map<String, Object> c = f.counters.view(0, 0);
        assertTrue((long) c.get("bufferOverflow") >= 8, c.toString());
        assertEquals(0L, (long) c.get("replayed"));
        assertTrue(base.sent().stream().noneMatch(s -> s.value().contains("\"replay\":true")));
    }

    @Test
    void failedSendsGoBackToTheQueueAndAreResentWithTheSameId() {
        Fleet f = base.fleet(2, 42, 400);
        base.failSends(true);
        run(f, 1000, 3000, 1000);
        assertTrue(base.sent().isEmpty());
        assertTrue(f.counters.sendFailed.get() > 0);
        base.failSends(false);
        run(f, 5000, 12_000, 500);
        for (int i = 0; i < 2; i++) {
            List<FleetTest.Sent> mine = forDevice(f.deviceId(i));
            long last = 0;
            for (FleetTest.Sent s : mine) {
                long seq = Long.parseLong(find(SEQ, s.value()));
                assertEquals(last + 1, seq, f.deviceId(i));
                last = seq;
            }
            assertEquals(last, f.ledger.entry(i).count());
        }
        assertTrue(base.sent().stream().anyMatch(s -> s.value().contains("\"replay\":true")));
    }

    @Test
    void fractionPicksAreStableAndCapsReturnConflict() {
        Fleet f = base.fleet(100, 42, 400);
        assertEquals(10, f.pick(0.1).length);
        org.junit.jupiter.api.Assertions.assertArrayEquals(f.pick(0.1), f.pick(0.1));
        for (int k = 0; k < 1000; k++) {
            f.startDisconnect(new int[] {0}, 1000);
        }
        try {
            f.startDisconnect(new int[] {0}, 1000);
            org.junit.jupiter.api.Assertions.fail("expected conflict");
        } catch (SimException e) {
            assertEquals(409, e.status.value());
        }
    }
}
