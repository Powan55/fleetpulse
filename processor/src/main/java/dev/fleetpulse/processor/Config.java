package dev.fleetpulse.processor;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class Config {

    @Bean
    Rules rules(@Value("${fp.rules-file:}") String file) throws IOException {
        return Rules.load(file);
    }
}
