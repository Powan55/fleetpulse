package dev.fleetpulse.simulator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Per device record of producer-acked seq numbers for the current epoch. */
final class Ledger {

    record Entry(long count, long maxSeq, Instant lastAckAt) {}

    private final BitSet[] seen;
    private final long[] count;
    private final long[] max;
    private final Instant[] last;
    private final AtomicLong total = new AtomicLong();

    Ledger(int devices) {
        seen = new BitSet[devices];
        count = new long[devices];
        max = new long[devices];
        last = new Instant[devices];
        for (int i = 0; i < devices; i++) {
            seen[i] = new BitSet();
        }
    }

    /** Returns true when seq was not accepted before. */
    boolean add(int device, long seq, Instant at) {
        synchronized (seen[device]) {
            last[device] = at;
            if (seen[device].get((int) seq)) {
                return false;
            }
            seen[device].set((int) seq);
            count[device]++;
            max[device] = Math.max(max[device], seq);
        }
        total.incrementAndGet();
        return true;
    }

    long total() {
        return total.get();
    }

    Entry entry(int device) {
        synchronized (seen[device]) {
            return new Entry(count[device], max[device], last[device]);
        }
    }

    /** Inclusive [from, to] runs, ascending. */
    List<long[]> ranges(int device) {
        List<long[]> out = new ArrayList<>();
        synchronized (seen[device]) {
            BitSet b = seen[device];
            for (int from = b.nextSetBit(0); from >= 0; ) {
                int end = b.nextClearBit(from);
                out.add(new long[] {from, end - 1});
                from = b.nextSetBit(end);
            }
        }
        return out;
    }
}
