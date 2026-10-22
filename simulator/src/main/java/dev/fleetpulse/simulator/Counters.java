package dev.fleetpulse.simulator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * generated: unique events created (each seq once), whether later dropped or not.
 * sent: send attempts handed to the producer, duplicates included.
 * accepted: unique events acked by Kafka (ledger total).
 * At quiescence (generation paused, nothing in flight, no send failures):
 * generated == accepted + dropped + bufferOverflow + bufferedEvents.
 * bufferedEvents: generated, not dropped, not yet acked (in flight or held back for reorder).
 */
final class Counters {
    final AtomicLong generated = new AtomicLong();
    final AtomicLong sent = new AtomicLong();
    final AtomicLong injectedDups = new AtomicLong();
    final AtomicLong dropped = new AtomicLong();
    final AtomicLong reordered = new AtomicLong();
    final AtomicLong skewedEvents = new AtomicLong();
    final AtomicLong replayed = new AtomicLong();
    final AtomicLong bufferOverflow = new AtomicLong();
    final AtomicLong sendFailed = new AtomicLong();
    final AtomicLong malformedInjected = new AtomicLong();
    final AtomicLong buffered = new AtomicLong();

    Map<String, Object> view(long accepted, int disconnected) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("generated", generated.get());
        m.put("accepted", accepted);
        m.put("sent", sent.get());
        m.put("injectedDups", injectedDups.get());
        m.put("dropped", dropped.get());
        m.put("reordered", reordered.get());
        m.put("skewedEvents", skewedEvents.get());
        m.put("replayed", replayed.get());
        m.put("bufferOverflow", bufferOverflow.get());
        m.put("sendFailed", sendFailed.get());
        m.put("malformedInjected", malformedInjected.get());
        m.put("disconnectedDevices", disconnected);
        m.put("bufferedEvents", buffered.get());
        return m;
    }
}
