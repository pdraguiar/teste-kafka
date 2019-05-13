package com.pdraguiar.testekafka.config;

import com.pdraguiar.testekafka.kafka.Pipeline;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaPipelineConfig {
    @Bean
    public Pipeline defaultPipeline() {
        return new Pipeline(new KafkaConfig());
    }

    @Bean
    public AdminClient kafkaAdminClient() {
        return KafkaAdminClient.create(new KafkaConfig().getProperties());
    }
}

