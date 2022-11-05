package com.kafkastreams.patientmonitoringsystem.Config;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Component
public class StaticConfigInitializer {

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

    @Value("${recentJoinedStatsStore}")
    public String recentJoinedStatsStore;

    @PostConstruct
    public void setConfig() {
        StreamsConfiguration.bpTopic = bpTopic;
        StreamsConfiguration.highBpTopic = highBpTopic;
        StreamsConfiguration.rawHbTopic = rawHbTopic;
        StreamsConfiguration.recordedHbTopic = recordedHbTopic;
        StreamsConfiguration.deviceAvgHbTopic = deviceAvgHbTopic;
        StreamsConfiguration.combinedValuesTopic = combinedValuesTopic;
        StreamsConfiguration.deviceStatsStore = deviceStatsStore;
        StreamsConfiguration.patientCombinedStatsStore = patientCombinedStatsStore;
        StreamsConfiguration.recentJoinedStatsStore = recentJoinedStatsStore;
    }

}
