package com.kafkastreams.patientmonitoringsystem.Topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;

import com.kafkastreams.patientmonitoringsystem.StreamUtils;
import com.kafkastreams.patientmonitoringsystem.Config.StreamsConfiguration;
import com.kafkastreams.patientmonitoringsystem.CustomSerdes.JsonSerde;
import com.kafkastreams.patientmonitoringsystem.Models.DeviceStats;
import com.kafkastreams.patientmonitoringsystem.Models.RecordedHB;
import com.kafkastreams.patientmonitoringsystem.Models.RecordedHBWithValidation;
import com.kafkastreams.patientmonitoringsystem.Topology.Interface.PatientMonitoringTopology;

public class FaultyHBDeviceDetectionTopology implements PatientMonitoringTopology {

    @Autowired
	private StreamsConfiguration streamsConfig;

    private int maxHbDeviation = 2;
    
    public KafkaStreams run() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Windowed<String>, ArrayList<RecordedHBWithValidation>> validatedHBStream = builder.stream(
            streamsConfig.recordedHbTopic,
            Consumed.with(new JsonSerde<Windowed<String>>(), Serdes.Long())
        )
        .map((windowedKey, hb) -> {
            String patientIdDeviceId = windowedKey.key();
            String patientId = patientIdDeviceId.split("-", -1)[0];
            String deviceId = patientIdDeviceId.split("-", -1)[1];
            Windowed<String> windowedPatientId = new Windowed<String>(patientId, windowedKey.window());
            var value = new RecordedHB(deviceId, Math.toIntExact(hb));
            return new KeyValue<>(windowedPatientId, value);
        })
        .groupByKey()
        .aggregate(
            new Initializer<ArrayList<RecordedHB>>() {

                @Override
                public ArrayList<RecordedHB> apply() {
                    return new ArrayList<>();
                }
            },
            new Aggregator<Windowed<String>, RecordedHB, ArrayList<RecordedHB>>() {

                @Override
                public ArrayList<RecordedHB> apply(Windowed<String> key, RecordedHB value,
                        ArrayList<RecordedHB> aggregate) {
                    aggregate.add(value);
                    return aggregate;
                }
                
            },
            null,
            null
        )
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded().shutDownWhenFull()))
        .toStream()
        .mapValues(recordedHBs -> getRecordedHBWithValidation(recordedHBs));
        
        materializeDeviceStats(validatedHBStream);
        streamDeviceAvgHB(validatedHBStream);
        return new KafkaStreams(builder.build(), StreamUtils.getStreamProperties());
    }

    private void materializeDeviceStats(KStream<Windowed<String>, ArrayList<RecordedHBWithValidation>> validatedHBStream) {
        validatedHBStream
        .flatMapValues(recordedHBWithValidations -> recordedHBWithValidations)
        .map((windowedPatientId, deviceInfo) -> {
            String deviceId = deviceInfo.getDeviceId();
            Boolean isCorrectRecording = deviceInfo.getIsCorrectRecording();
            return new KeyValue<>(deviceId, isCorrectRecording);
        })
        .groupByKey()
        .aggregate(
            () -> new DeviceStats(),
            (deviceId, isDeviceHealthy, deviceStats) -> {
                deviceStats.setDeviceId(deviceId);
                deviceStats.setTotalRecordings(deviceStats.getTotalRecordings() + 1);
                deviceStats.setCorrectRecordings(deviceStats.getCorrectRecordings() + (isDeviceHealthy ? 1 : 0));
                return deviceStats;
            },
            Materialized.<String, DeviceStats, KeyValueStore<Bytes,byte[]>>as(streamsConfig.deviceStatsStore)
        );
    }

    private void streamDeviceAvgHB(KStream<Windowed<String>, ArrayList<RecordedHBWithValidation>> validatedHBStream) {
        validatedHBStream
        .map((windowedPatientId, recordedHBWithValidations) -> {
            String patientId = windowedPatientId.key();
            int hbSum = 0;
            int correctRecordings = 0;
            for (RecordedHBWithValidation recordedHBWithValidation : recordedHBWithValidations) {
                if (recordedHBWithValidation.getIsCorrectRecording()) {
                    hbSum += recordedHBWithValidation.getHeartbeat();
                    correctRecordings++;
                }
            }
            int avgHb = hbSum / correctRecordings;
            return new KeyValue<String, Integer>(patientId, avgHb);
        })
        .to(streamsConfig.deviceAvgHbTopic, Produced.with(Serdes.String(), Serdes.Integer()));
    }

    private ArrayList<RecordedHBWithValidation> getRecordedHBWithValidation(ArrayList<RecordedHB> recordedValues) {
        if (recordedValues.isEmpty()) {
            return new ArrayList<>();
        }
        int minHbValue = 30;
        int maxHbValue = 150;
        HashMap<Integer, ArrayList<String>> devicesByHb = new HashMap<Integer, ArrayList<String>>();
        recordedValues.forEach(recordedValue -> {
            devicesByHb.get(recordedValue.heartbeat).add(recordedValue.deviceId);
        });
        HashMap<Integer, Integer> deviceCountInWindow = new HashMap<Integer, Integer>();
        int maxDevices = 0;
        for (int hbWindowStart = minHbValue; hbWindowStart < maxHbValue; hbWindowStart++) {
            int devicesInCurrentWindow = 0;
            for (int hb = hbWindowStart; hb < hbWindowStart + maxHbDeviation; hb++) {
                devicesInCurrentWindow += devicesByHb.get(hb).size();
            }
            deviceCountInWindow.put(hbWindowStart, devicesInCurrentWindow);
            maxDevices = Math.max(maxDevices, devicesInCurrentWindow);
        }
        final int maxDevicesFinal = maxDevices;
        ArrayList<Integer> windowsWithMaxDevices = new ArrayList<>();
        deviceCountInWindow.forEach((windowStart, deviceCount) -> {
            if (deviceCount == maxDevicesFinal) {
                windowsWithMaxDevices.add(windowStart);
            }
        });
        int chosenWindow = windowsWithMaxDevices.get(windowsWithMaxDevices.size() - 1);
        ArrayList<RecordedHBWithValidation> deviceStatus = new ArrayList<>();
        for (int hb = minHbValue; hb < maxHbValue; hb++) {
            final int hbFinal = hb;
            Boolean isCorrectHb = hb >= chosenWindow && hb < chosenWindow + maxHbDeviation;
            deviceStatus.addAll(
                devicesByHb
                .get(hb)
                .stream()
                .map(deviceId -> new RecordedHBWithValidation(deviceId, hbFinal, isCorrectHb))
                .collect(Collectors.toList())
            );
        }
        return deviceStatus;
    }
}
