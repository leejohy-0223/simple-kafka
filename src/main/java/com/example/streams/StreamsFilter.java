package com.example.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Stream Processor의 Filter 기능 추가
 */
public class StreamsFilter {

    private static String APPLICATION_NAME = "streams-filter-application";
    private static String BOOTSTRAP_SERVER = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {

        Properties configs = new Properties();

        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        /* 스트림 토폴로지를 정의하기 위한 용도 */
        StreamsBuilder builder = new StreamsBuilder();

        /* 특정 토픽을 KStream 형태로 가져오기 - 소스 프로세서 */
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);

        /* 소스로부터 얻은 데이터를 필터링 하기 - 스트림 프로세서 */
        KStream<String, String> filteredStream = streamLog.filter(
            ((key, value) -> value.length() > 5));

        /* 가공된 KStream 데이터를 특정 토픽으로 저장하기 - 싱크 프로세서 */
        filteredStream.to(STREAM_LOG_FILTER);

        KafkaStreams streams = new KafkaStreams(builder.build(), configs);
        streams.start();
    }
}
