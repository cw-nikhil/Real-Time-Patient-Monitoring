package com.kafkastreams.patientmonitoringsystem.Topology;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.springframework.stereotype.Component;

import com.kafkastreams.patientmonitoringsystem.Config.StreamsConfiguration;
import com.kafkastreams.patientmonitoringsystem.CustomSerdes.JsonSerde;
import com.kafkastreams.patientmonitoringsystem.Models.Heartbeat;
import com.kafkastreams.patientmonitoringsystem.Topology.Interface.PatientMonitoringTopology;

@Component
public class HeartbeatTopology implements PatientMonitoringTopology {
    private static int heartbeatWindowInSeconds = 60;
    private static int minimumHb = 35;
    public void addTopology(StreamsBuilder builder) {
        KStream<String, Heartbeat> heartbeatStream = builder.stream(
            StreamsConfiguration.rawHbTopic,
            Consumed.with(Serdes.String(), new JsonSerde<Heartbeat>())
        );
        heartbeatStream
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(heartbeatWindowInSeconds)))
            .count()
            .suppress(
                Suppressed.untilWindowCloses(BufferConfig.unbounded().shutDownWhenFull())
            )
            .toStream()
            .filter((windowedPatientId, heartbeat) -> heartbeat >= minimumHb)
            .to(StreamsConfiguration.recordedHbTopic, Produced.with(new JsonSerde<Windowed<String>>(), Serdes.Long()));
    }
}
