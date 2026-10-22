package dev.fleetpulse.processor;

/** Pure device state and metric-rule streak logic. Counts samples, never reads a clock. */
final class StateMachine {

    record Dev(String state, int bad, int fail, int ok) {
        static Dev fresh() { return new Dev("HEALTHY", 0, 0, 0); }
    }

    /** t = consecutive breaching samples, f = consecutive clear samples while open, open = alert id, start = event time micros of first breach. */
    record Streak(int t, int f, Long open, Long start) {
        static final Streak NONE = new Streak(0, 0, null, null);
    }

    enum Act { NONE, OPEN, RESOLVE }

    record Step(Streak streak, Act act) {}

    private static final int CAP = 1000;

    private StateMachine() {}

    /** OFFLINE leaves on any in-order event; the event itself is not counted as a sample. */
    static Dev onEvent(Dev d) {
        return "OFFLINE".equals(d.state()) ? new Dev("RECOVERING", 0, 0, 0) : d;
    }

    static Dev onSample(Rules r, Dev d, int severity) {
        if ("OFFLINE".equals(d.state())) return onEvent(d);
        int bad = severity >= Rules.DEGRADED ? Math.min(d.bad() + 1, CAP) : 0;
        int fail = severity == Rules.FAILED ? Math.min(d.fail() + 1, CAP) : 0;
        int ok = severity == Rules.OK ? Math.min(d.ok() + 1, CAP) : 0;
        String s = d.state();
        int enter = r.hysteresis().enterSamples(), exit = r.hysteresis().exitSamples(), rec = r.hysteresis().recoverSamples();
        boolean up = s.equals("HEALTHY") || s.equals("DEGRADED") || s.equals("RECOVERING");
        if (up && fail >= enter) return new Dev("FAILED", bad, fail, ok);
        if ((s.equals("HEALTHY") || s.equals("RECOVERING")) && bad >= enter) return new Dev("DEGRADED", bad, fail, ok);
        if (s.equals("DEGRADED") && ok >= exit) return new Dev("HEALTHY", bad, fail, ok);
        if (s.equals("FAILED") && ok >= exit) return new Dev("RECOVERING", 0, 0, 0);
        if (s.equals("RECOVERING") && ok >= rec) return new Dev("HEALTHY", bad, fail, ok);
        return new Dev(s, bad, fail, ok);
    }

    /** The caller fills in Streak.open after an OPEN is written. */
    static Step metric(Rules r, Streak s, boolean breach, long eventMicros) {
        int t, f;
        Long start;
        if (breach) {
            t = Math.min(s.t() + 1, CAP);
            f = 0;
            start = s.t() == 0 ? Long.valueOf(eventMicros) : s.start();
        } else {
            t = 0;
            f = s.open() != null ? Math.min(s.f() + 1, CAP) : 0;
            start = null;
        }
        Long open = s.open();
        if (open == null && t >= r.hysteresis().enterSamples()) return new Step(new Streak(t, f, null, start), Act.OPEN);
        if (open != null && f >= r.hysteresis().exitSamples()) return new Step(Streak.NONE, Act.RESOLVE);
        return new Step(new Streak(t, f, open, start), Act.NONE);
    }
}
