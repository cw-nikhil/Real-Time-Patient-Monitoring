package com.kafkastreams.patientmonitoringsystem.Topology;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;

import com.kafkastreams.patientmonitoringsystem.CustomSerdes.JsonSerde;
import com.kafkastreams.patientmonitoringsystem.Models.RecordedHB;

public class FaultyHBDeviceDetectionTopology {
    private static String recordedHeartbeatValues = "recordedHeartbeatValues";
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

        table
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded().shutDownWhenFull()))
        .toStream()
        .mapValues(recordedValues -> getHealthyDeviceIds(recordedValues))
        .flatMapValues(healtyDevices -> healtyDevices);

    }

    private static ArrayList<String> getHealthyDeviceIds(ArrayList<RecordedHB> recordedValues) {
        if (recordedValues.isEmpty()) {
            return new ArrayList<>();
        }
        int maxHbValue = 150;
        HashMap<Integer, ArrayList<String>> devicesByHb = new HashMap<Integer, ArrayList<String>>();
        recordedValues.forEach(recordedValue -> {
            devicesByHb.get(recordedValue.heartbeat).add(recordedValue.deviceId);
        });
        HashMap<Integer, Integer> deviceCountInWindow = new HashMap<Integer, Integer>();
        int maxDevices = 0;
        for (int hbWindowStart = 30; hbWindowStart < maxHbValue; hbWindowStart++) {
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
        ArrayList<String> healthyDevices = new ArrayList<>();
        for (int hb = chosenWindow; hb < chosenWindow + maxHbDeviation; hb++) {
            healthyDevices.addAll(devicesByHb.get(hb));
        }
        return healthyDevices;
    }
}
