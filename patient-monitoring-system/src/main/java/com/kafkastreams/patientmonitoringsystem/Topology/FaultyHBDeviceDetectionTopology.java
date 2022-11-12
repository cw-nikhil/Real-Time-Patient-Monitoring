package com.kafkastreams.patientmonitoringsystem.Topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Component;

import com.kafkastreams.patientmonitoringsystem.Config.StreamsConfiguration;
import com.kafkastreams.patientmonitoringsystem.CustomSerdes.JsonSerde;
import com.kafkastreams.patientmonitoringsystem.Models.AllDeviceRecordedHBs;
import com.kafkastreams.patientmonitoringsystem.Models.DeviceStats;
import com.kafkastreams.patientmonitoringsystem.Models.RecordedHB;
import com.kafkastreams.patientmonitoringsystem.Models.RecordedHBWithValidation;
import com.kafkastreams.patientmonitoringsystem.Models.WindowedString;
import com.kafkastreams.patientmonitoringsystem.Topology.Interface.PatientMonitoringTopology;

@Component
public class FaultyHBDeviceDetectionTopology implements PatientMonitoringTopology {
    private int maxHbDeviation = 2;
    
    public void addTopology(StreamsBuilder builder) {
        KStream<WindowedString, ArrayList<RecordedHBWithValidation>> validatedHBStream = builder.stream(
            StreamsConfiguration.recordedHbTopic,
            Consumed.with(new JsonSerde<WindowedString>(WindowedString.class), Serdes.Long())
        )
        .map((windowedKey, hb) -> {
            String patientIdDeviceId = windowedKey.getKey();
            String patientId = patientIdDeviceId.split("-", -1)[0];
            String deviceId = patientIdDeviceId.split("-", -1)[1];
            WindowedString windowedPatientId = new WindowedString(patientId, windowedKey.getWindow());
            var value = new RecordedHB(deviceId, Math.toIntExact(hb));
            return new KeyValue<>(windowedPatientId, value);
        })
        .groupByKey(
            Grouped.with(
                new JsonSerde<WindowedString>(WindowedString.class),
                new JsonSerde<RecordedHB>(RecordedHB.class)
            )
        )
        .aggregate(
            () -> new AllDeviceRecordedHBs(),
            (key, value, aggregate) -> {
                aggregate.recordedValues.add(value);
                return aggregate;
            },
            Materialized.with(
                new JsonSerde<WindowedString>(WindowedString.class),
                new JsonSerde<AllDeviceRecordedHBs>(AllDeviceRecordedHBs.class)
            )
        )
        .toStream()
        .mapValues(allDeviceRecordedHBs -> getRecordedHBWithValidation(allDeviceRecordedHBs.recordedValues));
        
        materializeDeviceStats(validatedHBStream);
        streamDeviceAvgHB(validatedHBStream);
    }

    private void materializeDeviceStats(KStream<WindowedString, ArrayList<RecordedHBWithValidation>> validatedHBStream) {
        validatedHBStream
        .flatMapValues(recordedHBWithValidations -> recordedHBWithValidations)
        .map((windowedPatientId, deviceInfo) -> {
            String deviceId = deviceInfo.getDeviceId();
            Boolean isCorrectRecording = deviceInfo.getIsCorrectRecording();
            return new KeyValue<>(deviceId, isCorrectRecording);
        })
        .groupByKey(
            Grouped.with(
                Serdes.String(),
                new JsonSerde<Boolean>(Boolean.class)
            )
        )
        .aggregate(
            () -> new DeviceStats(),
            (deviceId, isDeviceHealthy, deviceStats) -> {
                deviceStats.setDeviceId(deviceId);
                deviceStats.setTotalRecordings(deviceStats.getTotalRecordings() + 1);
                deviceStats.setCorrectRecordings(deviceStats.getCorrectRecordings() + (isDeviceHealthy ? 1 : 0));
                return deviceStats;
            },
            Materialized.<String, DeviceStats, KeyValueStore<Bytes,byte[]>>
                as(StreamsConfiguration.deviceStatsStore)
                .withKeySerde(Serdes.String())
                .withValueSerde(new JsonSerde<DeviceStats>(DeviceStats.class))
        );
    }

    private void streamDeviceAvgHB(KStream<WindowedString, ArrayList<RecordedHBWithValidation>> validatedHBStream) {
        validatedHBStream
        .map((windowedPatientId, recordedHBWithValidations) -> {
            String patientId = windowedPatientId.getKey();
            int hbSum = 0;
            int correctRecordings = 0;
            for (RecordedHBWithValidation recordedHBWithValidation : recordedHBWithValidations) {
                if (recordedHBWithValidation.getIsCorrectRecording()) {
                    hbSum += recordedHBWithValidation.getHeartbeat();
                    correctRecordings++;
                }
            }
            int avgHb = correctRecordings > 0 ? hbSum / correctRecordings : -1;
            return new KeyValue<String, Integer>(patientId, avgHb);
        })
        .filter((patientId, avgHb) -> avgHb != -1)
        .to(StreamsConfiguration.deviceAvgHbTopic, Produced.with(Serdes.String(), Serdes.Integer()));
    }

    private ArrayList<RecordedHBWithValidation> getRecordedHBWithValidation(ArrayList<RecordedHB> recordedValues) {
        if (recordedValues.isEmpty()) {
            return new ArrayList<>();
        }
        int minHbValue = 30;
        int maxHbValue = 150;
        HashMap<Integer, ArrayList<String>> devicesByHb = new HashMap<Integer, ArrayList<String>>();
        recordedValues.forEach(recordedValue -> {
            if (devicesByHb.get(recordedValue.heartbeat) == null) {
                devicesByHb.put(recordedValue.heartbeat, new ArrayList<String>());
            }
            devicesByHb.get(recordedValue.heartbeat).add(recordedValue.deviceId);
        });
        HashMap<Integer, Integer> deviceCountInWindow = new HashMap<Integer, Integer>();
        int maxDevices = 0;
        for (int hbWindowStart = minHbValue; hbWindowStart < maxHbValue; hbWindowStart++) {
            int devicesInCurrentWindow = 0;
            for (int hb = hbWindowStart; hb < hbWindowStart + maxHbDeviation; hb++) {
                ArrayList<String> devices = devicesByHb.get(hb);
                devicesInCurrentWindow += devices != null ? devices.size() : 0;
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
            if (devicesByHb.get(hb) != null) {
                deviceStatus.addAll(
                    devicesByHb
                    .get(hb)
                    .stream()
                    .map(deviceId -> new RecordedHBWithValidation(deviceId, hbFinal, isCorrectHb))
                    .collect(Collectors.toList())
                );
            }
        }
        return deviceStatus;
    }
}
