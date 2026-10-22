package dev.fleetpulse.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import dev.fleetpulse.processor.StateMachine.Act;
import dev.fleetpulse.processor.StateMachine.Dev;
import dev.fleetpulse.processor.StateMachine.Streak;

class StateMachineTest {

    static final Rules R = load();

    static Rules load() {
        try {
            return Rules.parse(Files.readString(Path.of("../contracts/rules.yaml")));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static Dev run(Dev d, int sev, int n) {
        for (int i = 0; i < n; i++) d = StateMachine.onSample(R, d, sev);
        return d;
    }

    @Test
    void severityFromRulesFile() {
        assertThat(R.severity(41, 87, 0)).isEqualTo(Rules.OK);
        assertThat(R.severity(70, 87, 0)).isEqualTo(Rules.DEGRADED);
        assertThat(R.severity(69.9, 20, 0)).isEqualTo(Rules.DEGRADED);
        assertThat(R.severity(41, 87, 5)).isEqualTo(Rules.DEGRADED);
        assertThat(R.severity(85, 87, 0)).isEqualTo(Rules.FAILED);
        assertThat(R.severity(41, 5, 0)).isEqualTo(Rules.FAILED);
        assertThat(R.severity(41, 87, 20)).isEqualTo(Rules.FAILED);
        assertThat(R.severity(90, 3, 99)).isEqualTo(Rules.FAILED);
    }

    @Test
    void worsenNeedsThreeInARow() {
        Dev d = run(Dev.fresh(), Rules.DEGRADED, 2);
        assertThat(d.state()).isEqualTo("HEALTHY");
        d = StateMachine.onSample(R, d, Rules.OK);
        d = run(d, Rules.DEGRADED, 2);
        assertThat(d.state()).isEqualTo("HEALTHY");
        assertThat(run(d, Rules.DEGRADED, 1).state()).isEqualTo("DEGRADED");
        assertThat(run(Dev.fresh(), Rules.FAILED, 3).state()).isEqualTo("FAILED");
        assertThat(run(Dev.fresh(), Rules.FAILED, 2).state()).isEqualTo("HEALTHY");
    }

    @Test
    void failedWinsOverDegradedWhenBothMatch() {
        assertThat(run(Dev.fresh(), Rules.FAILED, 3).state()).isEqualTo("FAILED");
        assertThat(run(run(Dev.fresh(), Rules.DEGRADED, 2), Rules.FAILED, 3).state()).isEqualTo("FAILED");
    }

    @Test
    void improveNeedsFiveOk() {
        Dev deg = run(Dev.fresh(), Rules.DEGRADED, 3);
        assertThat(run(deg, Rules.OK, 4).state()).isEqualTo("DEGRADED");
        assertThat(run(deg, Rules.OK, 5).state()).isEqualTo("HEALTHY");
        Dev failed = run(Dev.fresh(), Rules.FAILED, 3);
        Dev rec = run(failed, Rules.OK, 5);
        assertThat(rec).isEqualTo(new Dev("RECOVERING", 0, 0, 0));
        assertThat(run(rec, Rules.OK, 4).state()).isEqualTo("RECOVERING");
        assertThat(run(rec, Rules.OK, 5).state()).isEqualTo("HEALTHY");
        assertThat(run(failed, Rules.OK, 10).state()).isEqualTo("HEALTHY");
    }

    @Test
    void degradedDoesNotImproveFromFailed() {
        Dev failed = run(Dev.fresh(), Rules.FAILED, 3);
        assertThat(run(failed, Rules.DEGRADED, 10).state()).isEqualTo("FAILED");
    }

    @Test
    void recoveringFallsBackAfterThree() {
        Dev rec = run(run(Dev.fresh(), Rules.FAILED, 3), Rules.OK, 5);
        assertThat(run(rec, Rules.DEGRADED, 3).state()).isEqualTo("DEGRADED");
        assertThat(run(rec, Rules.FAILED, 3).state()).isEqualTo("FAILED");
        assertThat(run(rec, Rules.DEGRADED, 2).state()).isEqualTo("RECOVERING");
    }

    @Test
    void offlineLeavesOnAnyEventAndResetsCounters() {
        Dev off = new Dev("OFFLINE", 0, 0, 0);
        assertThat(StateMachine.onEvent(off)).isEqualTo(new Dev("RECOVERING", 0, 0, 0));
        assertThat(StateMachine.onSample(R, off, Rules.OK)).isEqualTo(new Dev("RECOVERING", 0, 0, 1));
        assertThat(StateMachine.onSample(R, off, Rules.DEGRADED)).isEqualTo(new Dev("RECOVERING", 1, 0, 0));
        assertThat(StateMachine.onSample(R, off, Rules.FAILED)).isEqualTo(new Dev("RECOVERING", 1, 1, 0));
        assertThat(StateMachine.onEvent(Dev.fresh())).isEqualTo(Dev.fresh());
        assertThat(run(StateMachine.onEvent(off), Rules.OK, 5).state()).isEqualTo("HEALTHY");
        assertThat(run(StateMachine.onEvent(off), Rules.OK, 4).state()).isEqualTo("RECOVERING");
        Dev trig = StateMachine.onSample(R, off, Rules.OK);
        assertThat(run(trig, Rules.OK, 3).state()).isEqualTo("RECOVERING");
        assertThat(run(trig, Rules.OK, 4).state()).isEqualTo("HEALTHY");
    }

    // The densest loop: RECOVERING -> DEGRADED -> FAILED -> RECOVERING is at most 3 transitions per 22 samples.
    @Test
    void flappingLoopTransitionsAreBounded() {
        Dev d = new Dev("RECOVERING", 0, 0, 0);
        int transitions = 0, samples = 0;
        int[] pattern = {Rules.DEGRADED, Rules.DEGRADED, Rules.DEGRADED, Rules.FAILED, Rules.FAILED, Rules.FAILED};
        for (int i = 0; i < 6; i++) {
            Dev n = StateMachine.onSample(R, d, pattern[i]);
            if (!n.state().equals(d.state())) transitions++;
            d = n;
            samples++;
        }
        for (int i = 0; i < 5; i++) {
            Dev n = StateMachine.onSample(R, d, Rules.OK);
            if (!n.state().equals(d.state())) transitions++;
            d = n;
            samples++;
        }
        assertThat(d.state()).isEqualTo("RECOVERING");
        assertThat(transitions).isEqualTo(3);
        assertThat(samples).isLessThanOrEqualTo(22);
    }

    @Test
    void metricRuleOpensAfterThreeAndResolvesAfterFive() {
        Streak s = Streak.NONE;
        for (int i = 1; i <= 2; i++) {
            var st = StateMachine.metric(R, s, true, 1000L * i);
            assertThat(st.act()).isEqualTo(Act.NONE);
            s = st.streak();
        }
        var open = StateMachine.metric(R, s, true, 3000);
        assertThat(open.act()).isEqualTo(Act.OPEN);
        assertThat(open.streak().start()).isEqualTo(1000L);
        s = new Streak(open.streak().t(), 0, 7L, open.streak().start());
        for (int i = 0; i < 4; i++) {
            var st = StateMachine.metric(R, s, false, 0);
            assertThat(st.act()).isEqualTo(Act.NONE);
            s = st.streak();
        }
        assertThat(StateMachine.metric(R, s, true, 0).streak().f()).isZero();
        assertThat(StateMachine.metric(R, s, false, 0).act()).isEqualTo(Act.RESOLVE);
    }

    @Test
    void brokenBreachStreakStartsOver() {
        Streak s = StateMachine.metric(R, Streak.NONE, true, 1).streak();
        s = StateMachine.metric(R, s, true, 2).streak();
        s = StateMachine.metric(R, s, false, 3).streak();
        assertThat(s).isEqualTo(Streak.NONE);
    }

    @Test
    void rulesFileMistakesFailStartup() throws Exception {
        String ok = Files.readString(Path.of("../contracts/rules.yaml"));
        assertThatThrownBy(() -> Rules.parse(ok.replace("version: 1", "version: 2"))).isNotNull();
        assertThatThrownBy(() -> Rules.parse(ok.replace("enter_samples: 3", "enter_samples: 3\n  bogus: 1"))).isNotNull();
        assertThatThrownBy(() -> Rules.parse(ok.replace("op: gte                 # gte | lte", "op: eq"))).isNotNull();
        assertThatThrownBy(() -> Rules.parse(ok.replace("state: OFFLINE", "state: GONE"))).isNotNull();
        assertThatThrownBy(() -> Rules.parse("version: 1\nhysteresis: {}")).isNotNull();
        assertThat(R.reopenAfter().toSeconds()).isEqualTo(6);
    }
}
