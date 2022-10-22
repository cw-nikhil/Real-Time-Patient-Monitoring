package com.kafkastreams.patientmonitoringsystem.Topology;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;

import com.kafkastreams.patientmonitoringsystem.CustomSerdes.JsonSerde;
import com.kafkastreams.patientmonitoringsystem.Models.BloodPressure;
import com.kafkastreams.patientmonitoringsystem.Models.HbBpJoinedValue;

public class HeartbeatBpJoinTopology {
    private static String highBpTopic = "high-bp";
    private static String highHeartbeatTopic = "high-heartbeat";
    private static String deviceAvgHeartbeatValues = "device-avg-hb";
    private static String combinedValuesTopic = "bp-hb-topic";
    private static String patientCombinedStatsStore = "patient-stats";
    private static int joinWindowInSeconds = 30;
    public void run() {
        var builder = new StreamsBuilder();
        KStream<String, BloodPressure> highBpStream = builder
            .stream(highBpTopic, Consumed.with(Serdes.String(), new JsonSerde<BloodPressure>()));
        KStream<String, Integer> highHeartbeatStream = builder
            .stream(deviceAvgHeartbeatValues, Consumed.with(Serdes.String(), Serdes.Integer()));
        
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
        joinedStream.to(combinedValuesTopic, Produced.with(Serdes.String(), new JsonSerde<HbBpJoinedValue>()));

        materializeJoinedStream(joinedStream);

    }

    private void materializeJoinedStream(KStream<String, HbBpJoinedValue> joinedStream) {
        joinedStream.
            groupByKey()
            .count(
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>
                    as(patientCombinedStatsStore)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())
            );
    }
}
