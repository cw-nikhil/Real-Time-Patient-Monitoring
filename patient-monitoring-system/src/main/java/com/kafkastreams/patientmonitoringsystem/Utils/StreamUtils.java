package com.kafkastreams.patientmonitoringsystem.Utils;

import java.util.Properties;

import org.apache.kafka.streams.StreamsConfig;

public class StreamUtils {
    public static Properties getStreamProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "patient-monitoring-system");
        return properties;
    }
}
