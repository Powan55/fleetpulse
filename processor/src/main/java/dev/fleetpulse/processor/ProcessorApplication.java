package dev.fleetpulse.processor;

import java.time.Duration;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;

@SpringBootApplication
@EnableScheduling
public class ProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProcessorApplication.class, args);
    }

    // Infra errors retry forever; the container pauses and keeps polling while it waits.
    @Bean
    CommonErrorHandler errorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS));
    }

    @Bean
    DefaultKafkaConsumerFactoryCustomizer sessionTimeout(@Value("${FP_SESSION_TIMEOUT:10s}") Duration d) {
        return f -> f.updateConfigs(Map.of("session.timeout.ms", (int) d.toMillis()));
    }
}
