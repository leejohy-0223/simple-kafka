package com.example.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducer {

    private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class);
    private static final String TOPIC_NAME = "test";
    private static final String BOOTSTRAP_SERVER = "my-kafka:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String messageValue = "testMessage3";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Pangyo", "25");
        Future<RecordMetadata> metadata = producer.send(record);

        log.info("record info is : {}", record);
        log.info("future metadata is : {}", metadata.get().toString());

        producer.flush();
        producer.close();
    }
}
