package com.kafkastreams.patientmonitoringsystem.Controller;


import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.kafkastreams.patientmonitoringsystem.Config.StreamsConfiguration;
import com.kafkastreams.patientmonitoringsystem.Models.DeviceStats;

@RestController
public class DeviceStatsController {

    @Autowired
    private KafkaStreams kafkaStreams;

    @GetMapping("api/devicestats/{deviceId}")
    public DeviceStats getDeviceStatsById(@PathVariable String deviceId) {
        ReadOnlyKeyValueStore<String, DeviceStats> store = kafkaStreams.store(
            StoreQueryParameters.fromNameAndType(
                StreamsConfiguration.deviceStatsStore, QueryableStoreTypes.keyValueStore()
            )
        );
        return store.get(deviceId);
    }

}
