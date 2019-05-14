package com.pdraguiar.testekafka.controller;

import com.pdraguiar.testekafka.kafka.Pipeline;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@RestController
@RequestMapping("/v1/kafka")
public class KafkaController {
    private Pipeline pipeline;

    @Autowired
    public KafkaController(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    @PostMapping("/new-message")
    public void createTopic(@RequestBody HashMap<String, String> fields) {
        pipeline.sendToTopic(fields.get("topic"), fields.get("msg"));
        pipeline.createConsumer(fields.get("topic"));
    }
}
