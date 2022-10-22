package com.kafkastreams.patientmonitoringsystem.Topology.Interface;

import org.apache.kafka.streams.KafkaStreams;

public interface PatientMonitoringTopology {
    public KafkaStreams run();
}
