package com.music.api.service;

import com.alibaba.fastjson.JSON;
import com.music.api.model.AiOdsEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Properties;

@Component
public class AiOdsEventPublisher {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public AiOdsEventPublisher(
        @Value("${ai.assistant.kafka-bootstrap-servers}") String bootstrapServers,
        @Value("${ai.assistant.ods-topic}") String topic) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(properties);
        this.topic = topic;
    }

    public void publish(AiOdsEvent event) {
        producer.send(new ProducerRecord<>(topic, JSON.toJSONString(event)));
    }

    @PreDestroy
    public void close() {
        producer.close();
    }
}
