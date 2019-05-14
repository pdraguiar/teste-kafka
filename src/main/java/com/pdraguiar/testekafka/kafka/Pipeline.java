package com.pdraguiar.testekafka.kafka;

import com.pdraguiar.testekafka.config.KafkaConfig;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.pdraguiar.testekafka.common.AsyncExecutionHelper.runAsync;

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
    }

    public <T> void createConsumer(String topicName) {
        runAsync(() -> {
            ContainerProperties containerProps = new ContainerProperties(topicName);
            containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            containerProps.setMessageListener((AcknowledgingMessageListener<String, T>) (consumerRecord, acknowledgment) -> {
                int counter = 1;
                int maxRetries = 3;
                boolean isAcknowledged = false;
                Map<Integer, Integer> delays = new HashMap<>();
                delays.put(1, 10_000);
                delays.put(2, 30_000);
                delays.put(3, 60_000);

                do {
                    logger.info("Tentativa {}: Mensagem={}, Tópico={}", counter, consumerRecord.value(), topicName);

                    try (Response response = makeRequest(consumerRecord, topicName)) {
                        if (response.isSuccessful()) {
                            logger.info("[Sucesso] Tentativa {}: Mensagem={}, Tópico={}", counter, consumerRecord.value(), topicName);
                            acknowledgment.acknowledge();
                            isAcknowledged = true;
                        } else if (!isRetryable(response))  {
                            sendToTopic("WASSENGER_TO_INTERCOM_NOT_RETRYABLE", consumerRecord.value());
                        }
                        else {
                            Thread.sleep(delays.get(counter));
                        }
                    } catch (IOException | InterruptedException e) {
                        System.out.println(e.getMessage());
                    }

                    if (counter == maxRetries) {
                        logger.info("[Falhou] Tentativa {}: Mensagem={}, Tópico={}", counter, consumerRecord.value(), topicName);
                        sendToTopic("WASSENGER_TO_INTERCOM_FAILED", consumerRecord.value());
                    }
                    counter++;
                } while ((!isAcknowledged && (counter <= maxRetries)));
            });


            DefaultKafkaConsumerFactory<String, T> cf = new DefaultKafkaConsumerFactory<String, T>(kafkaConfig.getDefaultConsumerProperties("group_"+topicName));
            KafkaMessageListenerContainer<String, T> con = new KafkaMessageListenerContainer<>(cf, containerProps);

            con.start();
        },
                "Can't start consumer for topic " + topicName);
    }

    private boolean isRetryable(Response response) {
        return (response.code() >= 500);
    }

    private <T> Response makeRequest(ConsumerRecord<String, T> consumerRecord, String topicName) throws IOException {
        Request request = new Request.Builder()
                .url("http://localhost:8081/v1/kafka/new-message")
                .post(RequestBody.create(MediaType.parse("application/json; charset=utf-8"), String.format("{\"msg\":\"%s\"}", consumerRecord.value()+" "+topicName)))
                .build();

        return new OkHttpClient().newCall(request).execute();
    }
}
