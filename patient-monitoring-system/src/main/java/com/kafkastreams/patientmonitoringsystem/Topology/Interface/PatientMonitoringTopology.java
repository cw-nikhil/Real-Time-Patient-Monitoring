package com.kafkastreams.patientmonitoringsystem.Topology.Interface;

import org.apache.kafka.streams.StreamsBuilder;

public interface PatientMonitoringTopology {
    public void addTopology(StreamsBuilder builder);
}
