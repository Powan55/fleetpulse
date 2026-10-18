package dev.fleetpulse.processor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
class TelemetryListener {

    private static final Logger log = LoggerFactory.getLogger(TelemetryListener.class);

    private final DeviceStore store;
    private final KafkaTemplate<byte[], byte[]> kafka;
    private final MeterRegistry meters;
    private final String dlqTopic;

    TelemetryListener(DeviceStore store, KafkaTemplate<byte[], byte[]> kafka, MeterRegistry meters,
                      @Value("${fp.dlq-topic}") String dlqTopic) {
        this.store = store;
        this.kafka = kafka;
        this.meters = meters;
        this.dlqTopic = dlqTopic;
    }

    // Offsets commit after this returns (AckMode BATCH), so after the DB commit. An exception retries the batch.
    @KafkaListener(topics = "${fp.topic}")
    void onBatch(List<ConsumerRecord<byte[], byte[]>> records) throws Exception {
        List<DeviceStore.Applied> ok = new ArrayList<>(records.size());
        for (ConsumerRecord<byte[], byte[]> r : records) {
            Validator.Result v = Validator.validate(r.key(), r.value(), r.timestamp());
            if (v.reason() == null) {
                ok.add(new DeviceStore.Applied(v.event(), r.timestamp()));
            } else {
                dlq(r, v.reason());
            }
        }
        store.apply(ok);
    }

    private void dlq(ConsumerRecord<byte[], byte[]> r, String reason) throws Exception {
        var out = new ProducerRecord<>(dlqTopic, null, r.key(), r.value());
        out.headers().add("fp-dlq-reason", reason.getBytes(StandardCharsets.UTF_8));
        out.headers().add("fp-source-partition", Integer.toString(r.partition()).getBytes(StandardCharsets.UTF_8));
        out.headers().add("fp-source-offset", Long.toString(r.offset()).getBytes(StandardCharsets.UTF_8));
        kafka.send(out).get(10, TimeUnit.SECONDS);
        meters.counter("fleetpulse.processor.dlq", "reason", reason).increment();
        log.warn("dlq reason={} partition={} offset={}", reason, r.partition(), r.offset());
    }
}
