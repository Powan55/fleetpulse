package dev.fleetpulse.processor;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** Stalls one partition so the sweep's lag gate can be exercised. Only exists when fp.test-hooks.enabled=true. */
@RestController
@RequestMapping("/test/v1/partitions")
@ConditionalOnProperty(name = "fp.test-hooks.enabled", havingValue = "true")
class TestHooks {

    record State(String topic, int partition, boolean pauseRequested, boolean paused, Instant autoResumeAt) {}

    private static final Logger log = LoggerFactory.getLogger(TestHooks.class);

    private final KafkaListenerEndpointRegistry registry;
    private final String topic;
    private final Duration autoResume;
    private final Map<Integer, ScheduledFuture<?>> timers = new ConcurrentHashMap<>();
    private final Map<Integer, Instant> resumeAt = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "test-hooks");
        t.setDaemon(true);
        return t;
    });

    TestHooks(KafkaListenerEndpointRegistry registry, MeterRegistry meters, @Value("${fp.topic}") String topic,
              @Value("${fp.test-hooks.auto-resume}") Duration autoResume) {
        this.registry = registry;
        this.topic = topic;
        this.autoResume = autoResume;
        Gauge.builder("fleetpulse.processor.test_hooks.paused_partitions", resumeAt, Map::size).register(meters);
        log.warn("test hooks enabled: partitions can be paused through /test/v1/partitions");
    }

    @GetMapping
    List<State> list() {
        List<State> out = new ArrayList<>();
        for (KafkaMessageListenerContainer<?, ?> c : children()) {
            for (TopicPartition tp : c.getAssignedPartitions()) out.add(state(c, tp));
        }
        out.sort(java.util.Comparator.comparingInt(State::partition));
        return out;
    }

    @PostMapping(value = "/{partition}/pause", consumes = MediaType.APPLICATION_JSON_VALUE)
    ResponseEntity<State> pause(@PathVariable int partition, @RequestBody Map<String, Object> body) {
        KafkaMessageListenerContainer<?, ?> c = owner(partition, body);
        TopicPartition tp = new TopicPartition(topic, partition);
        c.pausePartition(tp);
        resumeAt.put(partition, Instant.now().plus(autoResume));
        ScheduledFuture<?> old = timers.put(partition, scheduler.schedule(() -> {
            try {
                KafkaMessageListenerContainer<?, ?> o = find(partition);
                if (o != null) o.resumePartition(tp);
            } finally {
                timers.remove(partition);
                resumeAt.remove(partition);
            }
            log.warn("test hook auto-resumed partition {}", partition);
        }, autoResume.toMillis(), TimeUnit.MILLISECONDS));
        if (old != null) old.cancel(false);
        log.warn("test hook paused partition {}", partition);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(state(c, tp));
    }

    @PostMapping(value = "/{partition}/resume", consumes = MediaType.APPLICATION_JSON_VALUE)
    ResponseEntity<State> resume(@PathVariable int partition, @RequestBody Map<String, Object> body) {
        KafkaMessageListenerContainer<?, ?> c = owner(partition, body);
        TopicPartition tp = new TopicPartition(topic, partition);
        c.resumePartition(tp);
        ScheduledFuture<?> t = timers.remove(partition);
        if (t != null) t.cancel(false);
        resumeAt.remove(partition);
        log.warn("test hook resumed partition {}", partition);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(state(c, tp));
    }

    private KafkaMessageListenerContainer<?, ?> owner(int partition, Map<String, Object> body) {
        if (partition < 0 || partition > 5) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "partition must be 0-5");
        if (!body.isEmpty()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "body must be {}");
        KafkaMessageListenerContainer<?, ?> c = find(partition);
        if (c == null) throw new ResponseStatusException(HttpStatus.CONFLICT, "partition not assigned to this replica");
        return c;
    }

    private KafkaMessageListenerContainer<?, ?> find(int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        for (KafkaMessageListenerContainer<?, ?> c : children()) {
            if (c.getAssignedPartitions().contains(tp)) return c;
        }
        return null;
    }

    private List<KafkaMessageListenerContainer<?, ?>> children() {
        List<KafkaMessageListenerContainer<?, ?>> out = new ArrayList<>();
        for (var c : registry.getListenerContainers()) {
            if (c instanceof ConcurrentMessageListenerContainer<?, ?> cc) out.addAll(cc.getContainers());
        }
        return out;
    }

    private State state(KafkaMessageListenerContainer<?, ?> c, TopicPartition tp) {
        return new State(tp.topic(), tp.partition(), c.isPartitionPauseRequested(tp), c.isPartitionPaused(tp), resumeAt.get(tp.partition()));
    }
}
