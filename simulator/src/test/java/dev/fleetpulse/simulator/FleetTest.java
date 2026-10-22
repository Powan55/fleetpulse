package dev.fleetpulse.simulator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

class FleetTest {

    private static final long SEC = 1_000_000_000L;
    private static final Pattern SEQ = Pattern.compile("\"seq\":(\\d+)");
    private static final Pattern ID = Pattern.compile("\"event_id\":\"([^\"]+)\"");
    private static final Pattern TIME = Pattern.compile("\"event_time\":\"([^\"]+)\"");

    record Sent(String key, String value) {}

    private final List<Sent> sent = new ArrayList<>();

    private boolean failSends;

    List<Sent> sent() {
        return sent;
    }

    void failSends(boolean on) {
        failSends = on;
    }

    private Fleet fleet(int devices, long seed) {
        return fleet(devices, seed, 400);
    }

    @SuppressWarnings("unchecked")
    Fleet fleet(int devices, long seed, int cap) {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenAnswer(inv -> {
            if (failSends) {
                return CompletableFuture.failedFuture(new RuntimeException("down"));
            }
            sent.add(new Sent(inv.getArgument(1), inv.getArgument(2)));
            return CompletableFuture.completedFuture(null);
        });
        Fleet f = new Fleet(new SimProperties("t", devices, seed, 1000L, Duration.ofSeconds(2), Duration.ofSeconds(2), Duration.ofSeconds(1), cap, 2000),
                kafka);
        f.init(0);
        return f;
    }

    private static String find(Pattern p, String s) {
        Matcher m = p.matcher(s);
        assertTrue(m.find(), s);
        return m.group(1);
    }

    private static final Chaos MIXED = new Chaos(0.3, 0.2, 0.25, List.of(), true, null);

    @Test
    void sameSeedAndEpochRepeatEventsAndChaosDecisions() {
        List<String> runs = new ArrayList<>();
        for (int r = 0; r < 2; r++) {
            sent.clear();
            Fleet f = fleet(30, 7);
            f.setChaos(MIXED);
            f.tick(20 * SEC);
            f.tick(25 * SEC);
            runs.add(sent.stream().map(s -> s.key() + find(ID, s.value())
                    + find(Pattern.compile("\"payload\":(.*)\\}$"), s.value())).toList().toString());
        }
        assertEquals(runs.get(0), runs.get(1));
        assertTrue(runs.get(0).length() > 1000);
    }

    @Test
    void typeEventIdAndPayloadPerSeqIgnoreTimerJitter() {
        Fleet a = fleet(25, 7);
        a.tick(40 * SEC);
        Map<String, String> first = bySeq(sent);
        sent.clear();
        Fleet b = fleet(25, 7);
        long t = 0;
        java.util.Random jitter = new java.util.Random(99);
        while (t < 40 * SEC) {
            t = Math.min(40 * SEC, t + 1 + (long) (jitter.nextDouble() * 700_000_000L));
            b.tick(t);
        }
        Map<String, String> second = bySeq(sent);
        assertTrue(first.size() > 900);
        assertEquals(first, second);
    }

    private static Map<String, String> bySeq(List<Sent> sent) {
        Map<String, String> m = new HashMap<>();
        for (Sent s : sent) {
            String body = s.value().replaceFirst("\"event_time\":\"[^\"]*\",", "");
            m.put(s.key() + "#" + find(SEQ, s.value()), body);
        }
        return m;
    }

    @Test
    void differentSeedChangesEvents() {
        Fleet a = fleet(3, 1);
        a.tick(5 * SEC);
        String first = sent.get(0).value();
        sent.clear();
        Fleet b = fleet(3, 2);
        b.tick(5 * SEC);
        assertNotEquals(find(ID, first), find(ID, sent.get(0).value()));
    }

    @Test
    void counterEquationHoldsAtQuiescenceAndLedgerMatchesSends() {
        Fleet f = fleet(40, 42);
        f.setChaos(MIXED);
        f.tick(30 * SEC);
        f.setPaused(true);
        Map<String, Object> c = f.counters.view(f.ledger.total(), 0);
        long generated = (long) c.get("generated");
        long accepted = (long) c.get("accepted");
        assertEquals(generated, accepted + (long) c.get("dropped") + (long) c.get("bufferOverflow")
                + (long) c.get("bufferedEvents"), c.toString());
        assertEquals(0L, (long) c.get("bufferedEvents"));
        assertTrue((long) c.get("dropped") > 0 && (long) c.get("injectedDups") > 0 && (long) c.get("reordered") > 0, c.toString());
        assertEquals((long) c.get("sent"), accepted + (long) c.get("injectedDups"));
        assertEquals(sent.size(), (long) c.get("sent"));

        Map<String, Set<Long>> bySeq = new HashMap<>();
        for (Sent s : sent) {
            bySeq.computeIfAbsent(s.key(), k -> new HashSet<>()).add(Long.parseLong(find(SEQ, s.value())));
        }
        for (int i = 0; i < f.deviceCount(); i++) {
            Set<Long> seqs = bySeq.getOrDefault(f.deviceId(i), Set.of());
            assertEquals(seqs.size(), f.ledger.entry(i).count(), f.deviceId(i));
            assertEquals(seqs.stream().mapToLong(Long::longValue).max().orElse(0), f.ledger.entry(i).maxSeq());
            long covered = f.ledger.ranges(i).stream().mapToLong(r -> r[1] - r[0] + 1).sum();
            assertEquals(seqs.size(), covered);
        }
    }

    @Test
    void dropOneRemovesEverythingAndKeepsGaps() {
        Fleet f = fleet(5, 42);
        f.setChaos(new Chaos(0, 1, 0, List.of(), true, null));
        f.tick(10 * SEC);
        assertTrue(sent.isEmpty());
        assertEquals(0, f.ledger.total());
        assertEquals(f.counters.generated.get(), f.counters.dropped.get());
        f.setChaos(Chaos.OFF);
        f.tick(12 * SEC);
        assertFalse(sent.isEmpty());
        assertTrue(Long.parseLong(find(SEQ, sent.get(0).value())) > 1);
    }

    @Test
    void reorderSendsHeldEventAfterNextOne() {
        Fleet f = fleet(1, 42);
        f.setChaos(new Chaos(0, 0, 1, List.of(), true, null));
        f.tick(10 * SEC);
        f.tick(10 * SEC + SEC / 2);
        int beforeRelease = sent.size();
        f.tick(12 * SEC);
        List<Long> seqs = sent.stream().map(s -> Long.parseLong(find(SEQ, s.value()))).toList();
        assertTrue(beforeRelease < seqs.size());
        assertTrue(seqs.get(beforeRelease) < seqs.get(beforeRelease - 1), "held event not released after hold: " + seqs);
        assertTrue(f.counters.reordered.get() > 0);
        boolean outOfOrder = false;
        for (int i = 1; i < seqs.size(); i++) {
            outOfOrder |= seqs.get(i) < seqs.get(i - 1);
        }
        assertTrue(outOfOrder, seqs.toString());
        assertEquals(new HashSet<>(seqs).size(), seqs.size());
    }

    @Test
    void skewChangesEventTimeOnly() {
        Fleet plain = fleet(10, 42);
        plain.tick(5 * SEC);
        List<Sent> base = new ArrayList<>(sent);
        sent.clear();
        Fleet skewed = fleet(10, 42);
        skewed.setChaos(new Chaos(0, 0, 0, List.of(new Chaos.Skew(0.5, 300_000), new Chaos.Skew(0.5, -300_000)), true, null));
        skewed.tick(5 * SEC);
        assertEquals(base.size(), sent.size());
        int ahead = 0;
        int behind = 0;
        for (int i = 0; i < base.size(); i++) {
            assertEquals(find(ID, base.get(i).value()), find(ID, sent.get(i).value()));
            long diff = Instant.parse(find(TIME, sent.get(i).value())).toEpochMilli()
                    - Instant.parse(find(TIME, base.get(i).value())).toEpochMilli();
            assertEquals(300_000, Math.abs(Math.round(diff / 1000.0) * 1000), 2000);
            if (diff > 0) {
                ahead++;
            } else {
                behind++;
            }
        }
        assertTrue(ahead > 0 && behind > 0);
        assertEquals(base.size(), skewed.counters.skewedEvents.get());
    }

    @Test
    void pauseStopsGenerationAndResumeDoesNotBurst() {
        Fleet f = fleet(10, 42);
        f.tick(4 * SEC);
        f.setPaused(true);
        int before = sent.size();
        f.tick(100 * SEC);
        assertEquals(before, sent.size());
        f.setPaused(false);
        f.tick(102 * SEC);
        assertEquals(before, sent.size());
        f.tick(104 * SEC);
        assertTrue(sent.size() - before <= 10 * 2, "burst after resume: " + (sent.size() - before));
        assertTrue(sent.size() > before);
    }

    @Test
    void malformedRecordsCoverFourKindsAndSkipLedger() {
        Fleet f = fleet(10, 42);
        for (String kind : List.of("invalid_json", "missing_field", "bad_type", "oversize")) {
            sent.clear();
            assertEquals(3, f.injectMalformed(kind, 3));
            assertEquals(3, sent.size());
            for (Sent s : sent) {
                assertTrue(s.key().matches("dev-[0-9]{4}"));
                switch (kind) {
                    case "invalid_json" -> assertFalse(s.value().endsWith("}"));
                    case "missing_field" -> assertFalse(s.value().contains("payload"));
                    case "bad_type" -> assertTrue(s.value().contains("\"seq\":\"one\""));
                    default -> assertTrue(s.value().length() > 1024);
                }
            }
        }
        assertEquals(12, f.counters.malformedInjected.get());
        assertEquals(0, f.ledger.total());
    }
}
