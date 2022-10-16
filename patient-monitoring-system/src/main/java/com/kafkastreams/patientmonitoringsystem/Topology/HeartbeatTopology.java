package com.kafkastreams.patientmonitoringsystem.Topology;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;

import com.kafkastreams.patientmonitoringsystem.StreamUtils;
import com.kafkastreams.patientmonitoringsystem.CustomSerdes.JsonSerde;
import com.kafkastreams.patientmonitoringsystem.Models.Heartbeat;

public class HeartbeatTopology {
    private static String heartbeatTopic = "heartbeats";
    private static String recordedHeartbeatValues = "recordedHeartbeatValues";
    private static int heartbeatWindowInSeconds = 60;
    public KafkaStreams run() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Heartbeat> heartbeatStream = builder.stream(
            heartbeatTopic,
            Consumed.with(Serdes.String(), new JsonSerde<Heartbeat>())
        );
        heartbeatStream.groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(heartbeatWindowInSeconds)))
            .count()
            .suppress(
                Suppressed.untilWindowCloses(BufferConfig.unbounded().shutDownWhenFull())
            )
            .toStream()
            .to(recordedHeartbeatValues);
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), StreamUtils.getStreamProperties());
        return kafkaStreams;
    }
}
