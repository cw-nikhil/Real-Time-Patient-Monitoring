package com.kafkastreams.patientmonitoringsystem.Topology;

import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;

import com.kafkastreams.patientmonitoringsystem.StreamUtils;
import com.kafkastreams.patientmonitoringsystem.Config.StreamsConfiguration;
import com.kafkastreams.patientmonitoringsystem.CustomSerdes.JsonSerde;
import com.kafkastreams.patientmonitoringsystem.Models.BloodPressure;
import com.kafkastreams.patientmonitoringsystem.Topology.Interface.PatientMonitoringTopology;

public class BloodPressureTopolgy implements PatientMonitoringTopology {
    
    @Autowired
	private StreamsConfiguration streamsConfig;
    
    public KafkaStreams run() {
        StreamsBuilder builder = new StreamsBuilder();
        Map<String, KStream<String, BloodPressure>>   branchedStream = builder
        .stream(streamsConfig.bpTopic, Consumed.with(Serdes.String(), new JsonSerde<BloodPressure>()))
        .split()
        .branch((patientId, bp) -> isNormalBp(bp), Branched.as("normalBp"))
        .branch((patientId, bp) -> isLowBp(bp), Branched.as("lowBp"))
        .branch((patientId, bp) -> isHighBp(bp), Branched.as("highBp"))
        .noDefaultBranch();

        branchedStream.get("highBp").to(streamsConfig.highBpTopic, Produced.with(Serdes.String(), new JsonSerde<BloodPressure>()));
        KafkaStreams kstreams = new KafkaStreams(builder.build(), StreamUtils.getStreamProperties());
        return kstreams;
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
