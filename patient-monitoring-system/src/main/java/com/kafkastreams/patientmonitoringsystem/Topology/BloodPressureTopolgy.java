package com.kafkastreams.patientmonitoringsystem.Topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;

import com.kafkastreams.patientmonitoringsystem.Config.StreamsConfiguration;
import com.kafkastreams.patientmonitoringsystem.CustomSerdes.JsonSerde;
import com.kafkastreams.patientmonitoringsystem.Models.BloodPressure;
import com.kafkastreams.patientmonitoringsystem.Topology.Interface.PatientMonitoringTopology;

@Component
public class BloodPressureTopolgy implements PatientMonitoringTopology {
    public void addTopology(StreamsBuilder builder) {
        builder
        .stream(StreamsConfiguration.bpTopic, Consumed.with(Serdes.String(), new JsonSerde<BloodPressure>(BloodPressure.class)))
        .split()
        .branch((patientId, bp) -> isNormalBp(bp), Branched.as("normalBp"))
        .branch((patientId, bp) -> isLowBp(bp), Branched.as("lowBp"))
        .branch(
            (patientId, bp) -> isHighBp(bp),
            Branched.withConsumer(ks -> ks.to(
                StreamsConfiguration.highBpTopic,
                Produced.with(Serdes.String(), new JsonSerde<BloodPressure>(BloodPressure.class))
            ))
        )
        .noDefaultBranch();
    }

    private static boolean isNormalBp(BloodPressure bp) {
        int systolicPressure = bp.getSystolicPressure();
        int diastolicPressure = bp.getDiastolicPressure();
        return systolicPressure >= 90 && systolicPressure <= 120 &&
                diastolicPressure >= 60 && diastolicPressure <= 80;
    }
    private static boolean isLowBp(BloodPressure bp) {
        return bp.getSystolicPressure() < 90 && bp.getDiastolicPressure() < 60;
    }
    private static boolean isHighBp(BloodPressure bp) {
        return bp.getSystolicPressure() > 140 && bp.getDiastolicPressure() > 90;
    }
}
