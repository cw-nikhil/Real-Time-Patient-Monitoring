package com.kafkastreams.patientmonitoringsystem.Topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.state.KeyValueStore;

import com.kafkastreams.patientmonitoringsystem.CustomSerdes.JsonSerde;
import com.kafkastreams.patientmonitoringsystem.Models.DeviceStats;
import com.kafkastreams.patientmonitoringsystem.Models.RecordedHB;

public class FaultyHBDeviceDetectionTopology {
    private static String recordedHeartbeatValues = "recordedHeartbeatValues";
    private static String deviceStatsStore = "device-stats-store";
    private static int maxHbDeviation = 4;
    public void run() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<Windowed<String>, ArrayList<RecordedHB>> table = builder.stream(
            recordedHeartbeatValues,
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
        );
        
        materializeDeviceStats(table);
    }

    private void materializeDeviceStats(KTable<Windowed<String>, ArrayList<RecordedHB>> table) {
        table
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded().shutDownWhenFull()))
        .toStream()
        .mapValues(recordedValues -> getDeviceStatus(recordedValues))
        .flatMapValues(healtyDevices -> healtyDevices)
        .map((windowedPatientId, deviceInfo) -> {
            String deviceId = deviceInfo.key;
            Boolean isDeviceHealthy = deviceInfo.value;
            return new KeyValue<>(deviceId, isDeviceHealthy);
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
            Materialized.<String, DeviceStats, KeyValueStore<Bytes,byte[]>>as(deviceStatsStore)
        );
    }

    private static ArrayList<KeyValue<String, Boolean>> getDeviceStatus(ArrayList<RecordedHB> recordedValues) {
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
        ArrayList<KeyValue<String, Boolean>> deviceStatus = new ArrayList<>();
        for (int hb = chosenWindow; hb < chosenWindow + maxHbDeviation; hb++) {
            deviceStatus.addAll(
                devicesByHb.get(hb).stream().map(deviceId -> new KeyValue<>(deviceId, true)).collect(Collectors.toList())
            );
        }
        for (int hb = minHbValue; hb < maxHbValue; hb++) {
            Boolean isCorrectHb = hb >= chosenWindow && hb < chosenWindow + maxHbDeviation;
            deviceStatus.addAll(
                devicesByHb.get(hb).stream().map(deviceId -> new KeyValue<>(deviceId, isCorrectHb)).collect(Collectors.toList())
            );
        }
        return deviceStatus;
    }
}
