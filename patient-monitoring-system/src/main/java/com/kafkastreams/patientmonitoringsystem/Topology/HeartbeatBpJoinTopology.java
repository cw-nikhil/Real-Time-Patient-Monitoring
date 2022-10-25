package com.kafkastreams.patientmonitoringsystem.Topology;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;

import com.kafkastreams.patientmonitoringsystem.Utils.StreamUtils;
import com.kafkastreams.patientmonitoringsystem.Config.StreamsConfiguration;
import com.kafkastreams.patientmonitoringsystem.CustomSerdes.JsonSerde;
import com.kafkastreams.patientmonitoringsystem.Models.BloodPressure;
import com.kafkastreams.patientmonitoringsystem.Models.HbBpJoinedValue;
import com.kafkastreams.patientmonitoringsystem.Topology.Interface.PatientMonitoringTopology;

public class HeartbeatBpJoinTopology implements PatientMonitoringTopology {

    @Autowired
	private StreamsConfiguration streamsConfig;

    private static int joinWindowInSeconds = 30;
    private static int highHbThreshold = 100;
    public KafkaStreams run() {
        var builder = new StreamsBuilder();
        KStream<String, BloodPressure> highBpStream = builder
            .stream(streamsConfig.highBpTopic, Consumed.with(Serdes.String(), new JsonSerde<BloodPressure>()));
        KStream<String, Integer> highHeartbeatStream = builder
            .stream(streamsConfig.deviceAvgHbTopic, Consumed.with(Serdes.String(), Serdes.Integer()))
            .filter((patientId, heartbeat) -> heartbeat > highHbThreshold);
        
        ValueJoiner<BloodPressure, Integer, HbBpJoinedValue> valueJoiner = (bp, hb) -> {
            return new HbBpJoinedValue(hb, bp.getSystolicPressure(), bp.getDiastolicPressure());
        };
        
        KStream<String, HbBpJoinedValue> joinedStream = highBpStream.join(
            highHeartbeatStream,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(
                Duration.ofSeconds(joinWindowInSeconds)).before(Duration.ofSeconds(joinWindowInSeconds)
            )
        );
        joinedStream.to(streamsConfig.combinedValuesTopic, Produced.with(Serdes.String(), new JsonSerde<HbBpJoinedValue>()));

        materializeJoinedStream(joinedStream);
        return new KafkaStreams(builder.build(), StreamUtils.getStreamProperties());
    }

    private void materializeJoinedStream(KStream<String, HbBpJoinedValue> joinedStream) {
        joinedStream.
            groupByKey()
            .count(
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>
                    as(streamsConfig.patientCombinedStatsStore)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())
            );
    }
}
