package com.kafkastreams.patientmonitoringsystem.Controller;

import java.util.ArrayList;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.kafkastreams.patientmonitoringsystem.Config.StreamsConfiguration;
import com.kafkastreams.patientmonitoringsystem.Models.HbBpJoinedValue;

@RestController
public class JoinedStatsController {
    
    @Autowired
    private KafkaStreams kafkaStreams;

    @Autowired
	private StreamsConfiguration streamsConfig;

    @GetMapping("api/joinedstats/recent/{patientId}")
    public ArrayList<HbBpJoinedValue> GetRecentAbnormalStatsByPatientId(@PathVariable String patientId) {
        ReadOnlyKeyValueStore<String, ArrayList<HbBpJoinedValue>> store = kafkaStreams.store(
            StoreQueryParameters.fromNameAndType(
                streamsConfig.recentJoinedStatsStore, QueryableStoreTypes.keyValueStore()
            )
        );
        ArrayList<HbBpJoinedValue> records = store.get(patientId);
        return records;
    }

    @GetMapping("api/joinedstats/totalCount/{patientId}")
    public long GetAbnormalStatsCountByPatientId(@PathVariable String patientId) {
        ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(
            StoreQueryParameters.fromNameAndType(
                streamsConfig.patientCombinedStatsStore, QueryableStoreTypes.keyValueStore()
            )
        );
        return store.get(patientId);
    }
}
