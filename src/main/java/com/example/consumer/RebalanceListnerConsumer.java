package com.example.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceListnerConsumer {

    private static final Logger log = LoggerFactory.getLogger(RebalanceListnerConsumer.class);
    private static final String TOPIC_NAME = "test";
    private static final String BOOTSTRAP_SERVER = "my-kafka:9092";
    private static final String GROUP_ID = "test-group";

    private static KafkaConsumer<String, String> consumer;
    private static Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC_NAME), new RebalanceListener());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("record info is : {}", record);
                currentOffset.put(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1, null)
                );
                consumer.commitSync(currentOffset);
            }
        }
    }

    private static class RebalanceListener implements ConsumerRebalanceListener {

        /* 리밸런스 끝난 뒤에 파티션이 할당 완료되면 호출되는 메서드 */
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.warn("partitions are assigned");
        }

        /* 리밸런스가 시작되기 직전에 호출되는 메서드 */
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.warn("partitions are revoked");
            consumer.commitSync(currentOffset);
        }
    }
}
