package com.pdraguiar.testekafka.kafka;

import com.pdraguiar.testekafka.config.KafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
public class Pipeline {
    @Autowired
    private AdminClient kafkaAdminClient;
    private KafkaConfig kafkaConfig;
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Value("${broker.kafka.prefix}")
    private String kafkaPrefix;

    public Pipeline(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public <T> void sendToTopic(String topicName, T message ) {
        try (Producer<String, T> producer = new KafkaProducer<>(kafkaConfig.getDefaultProducerProperties())) {
            producer.send(new ProducerRecord<>(topicName, message));
        }

        ContainerProperties containerProps = new ContainerProperties(topicName);
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        containerProps.setMessageListener((AcknowledgingMessageListener<String, T>) (consumerRecord, acknowledgment) -> {
            logger.info("************ " + consumerRecord.value().toString() + "********************");
            acknowledgment.acknowledge();
        });

        DefaultKafkaConsumerFactory<String, T> cf = new DefaultKafkaConsumerFactory<String, T>(kafkaConfig.getDefaultConsumerProperties("group_"+topicName));
        KafkaMessageListenerContainer<String, T> con = new KafkaMessageListenerContainer<>(cf, containerProps);
        con.start();
    }
}
