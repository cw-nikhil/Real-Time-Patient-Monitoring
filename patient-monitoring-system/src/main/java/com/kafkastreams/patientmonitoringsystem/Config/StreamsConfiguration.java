package com.kafkastreams.patientmonitoringsystem.Config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Component
public class StreamsConfiguration {
    @Value("${bpTopic}")
    public String bpTopic;

    @Value("${highBpTopic}")
    public String highBpTopic;

    @Value("${rawHbTopic}")
    public String rawHbTopic;

    @Value("${recordedHbTopic}")
    public String recordedHbTopic;

    @Value("${deviceAvgHbTopic}")
    public String deviceAvgHbTopic;

    @Value("${combinedValuesTopic}")
    public String combinedValuesTopic;

    @Value("${deviceStatsStore}")
    public String deviceStatsStore;

    @Value("${patientCombinedStatsStore}")
    public String patientCombinedStatsStore;
}
