package com.kafkastreams.patientmonitoringsystem.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;

import com.kafkastreams.patientmonitoringsystem.Config.StreamsConfiguration;

public class StreamUtils {
    private static int topicPartitions = 1;
    private static short topicReplicas = 1;
    private static ArrayList<String> sourceTopics = new ArrayList<String>(
        Arrays.asList(
            StreamsConfiguration.bpTopic,
            StreamsConfiguration.highBpTopic,
            StreamsConfiguration.rawHbTopic,
            StreamsConfiguration.recordedHbTopic,
            StreamsConfiguration.deviceAvgHbTopic,
            StreamsConfiguration.combinedValuesTopic
        )
    );

    public static Properties getStreamProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "patient-monitoring-system");
        return properties;
    }

    public static void createAllTopics() {
        Admin adminClient = Admin.create(getStreamProperties());
        List<NewTopic> topics = sourceTopics
            .stream()
            .map(topicName -> new NewTopic(topicName, topicPartitions, topicReplicas))
            .collect(Collectors.toList());
        adminClient.createTopics(topics);
    }
}
