package com.kafkastreams.patientmonitoringsystem.Topology;

import java.time.Duration;
import java.util.ArrayList;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Component;

import com.kafkastreams.patientmonitoringsystem.Config.StreamsConfiguration;
import com.kafkastreams.patientmonitoringsystem.CustomSerdes.JsonSerde;
import com.kafkastreams.patientmonitoringsystem.Models.BloodPressure;
import com.kafkastreams.patientmonitoringsystem.Models.HbBpJoinedValue;
import com.kafkastreams.patientmonitoringsystem.Topology.Interface.PatientMonitoringTopology;

@Component
public class HeartbeatBpJoinTopology implements PatientMonitoringTopology {
    private static int joinWindowInSeconds = 30;
    private static int highHbThreshold = 100;
    private static int latestRecordsCount = 10;

    public void addTopology(StreamsBuilder builder) {
        KStream<String, BloodPressure> highBpStream = builder
            .stream(StreamsConfiguration.highBpTopic, Consumed.with(Serdes.String(), new JsonSerde<BloodPressure>(BloodPressure.class)));
        KStream<String, Integer> highHeartbeatStream = builder
            .stream(StreamsConfiguration.deviceAvgHbTopic, Consumed.with(Serdes.String(), Serdes.Integer()))
            .filter((patientId, heartbeat) -> heartbeat > highHbThreshold);
        
        ValueJoiner<BloodPressure, Integer, HbBpJoinedValue> valueJoiner = (bp, hb) -> {
            return new HbBpJoinedValue(hb, bp.getSystolicPressure(), bp.getDiastolicPressure());
        };
        
        KStream<String, HbBpJoinedValue> joinedStream = highBpStream.join(
            highHeartbeatStream,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(joinWindowInSeconds)),
            StreamJoined.with(
                Serdes.String(),
                new JsonSerde<BloodPressure>(BloodPressure.class),
                Serdes.Integer()
            )
        );
        joinedStream.to(StreamsConfiguration.combinedValuesTopic, Produced.with(Serdes.String(), new JsonSerde<HbBpJoinedValue>(HbBpJoinedValue.class)));

        KGroupedStream<String, HbBpJoinedValue> groupedStream = joinedStream.groupByKey();
        materializeTotalCount(groupedStream);
        materializeRecentRecords(groupedStream);
    }

    private void materializeTotalCount(KGroupedStream<String, HbBpJoinedValue> groupedStream) {
        groupedStream
            .count(
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>
                    as(StreamsConfiguration.patientCombinedStatsStore)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())
            );
    }

    private void materializeRecentRecords(KGroupedStream<String, HbBpJoinedValue> groupedStream) {
        groupedStream
            .aggregate(
                () -> new ArrayList<HbBpJoinedValue>(),
                (patientId, joinedStat, recentStats) -> {
                    if (recentStats.size() > latestRecordsCount) {
                        recentStats.remove(0);
                    }
                    recentStats.add(joinedStat);
                    return recentStats;
                },
                Materialized.<String, ArrayList<HbBpJoinedValue>, KeyValueStore<Bytes, byte[]>>
                    as(StreamsConfiguration.recentJoinedStatsStore)
                    .withKeySerde(Serdes.String())
                    // .withValueSerde(new JsonSerde<ArrayList<HbBpJoinedValue>>(a))
            );
    }
}
