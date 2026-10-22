package dev.fleetpulse.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

@Component
class KafkaLagProbe implements Sweeper.LagProbe {

    private static final long CALL_SECONDS = 3;

    private final Admin admin;
    private final String topic, group;

    KafkaLagProbe(KafkaAdmin kafkaAdmin, @Value("${fp.topic}") String topic, @Value("${spring.kafka.consumer.group-id}") String group) {
        this.admin = Admin.create(kafkaAdmin.getConfigurationProperties());
        this.topic = topic;
        this.group = group;
    }

    @Override
    public long maxLag() throws Exception {
        var desc = admin.describeTopics(List.of(topic)).allTopicNames().get(CALL_SECONDS, TimeUnit.SECONDS).get(topic);
        Map<TopicPartition, OffsetSpec> latest = new HashMap<>(), earliest = new HashMap<>();
        for (var p : desc.partitions()) {
            TopicPartition tp = new TopicPartition(topic, p.partition());
            latest.put(tp, OffsetSpec.latest());
            earliest.put(tp, OffsetSpec.earliest());
        }
        Map<TopicPartition, OffsetAndMetadata> committed =
                admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(CALL_SECONDS, TimeUnit.SECONDS);
        var ends = admin.listOffsets(latest).all().get(CALL_SECONDS, TimeUnit.SECONDS);
        var begins = admin.listOffsets(earliest).all().get(CALL_SECONDS, TimeUnit.SECONDS);
        long max = 0;
        for (TopicPartition tp : latest.keySet()) {
            OffsetAndMetadata c = committed.get(tp);
            long base = c != null ? c.offset() : begins.get(tp).offset();
            max = Math.max(max, ends.get(tp).offset() - base);
        }
        return max;
    }

    @PreDestroy
    void close() {
        admin.close();
    }
}
