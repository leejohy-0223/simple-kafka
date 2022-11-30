package com.example.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

/**
 * stream_log 토픽의 내용을 그대로 stream_log_copy로 복사하는 Stream Application
 */
public class SimpleStreamApplication {

    private static String APPLICATION_NAME = "streams-application";
    private static String BOOTSTRAP_SERVER = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_COPY = "stream_log_copy";

    public static void main(String[] args) {

        Properties configs = new Properties();

        /* 애플리케이션 아이디 지정 필요 */
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        /* 스트림 토폴로지를 정의하기 위한 용도 */
        StreamsBuilder builder = new StreamsBuilder();

        /* 특정 토픽을 KStream 형태로 가져오기 - 소스 프로세서 */
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);

        /* KSTream의 데이터를 특정 토픽으로 저장하기 - 싱크 프로세서 */
        streamLog.to(STREAM_LOG_COPY);

        KafkaStreams streams = new KafkaStreams(builder.build(), configs);
        streams.start();
    }
}
