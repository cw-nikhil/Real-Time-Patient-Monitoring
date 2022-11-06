package com.kafkastreams.patientmonitoringsystem;

import java.util.List;


import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.kafkastreams.patientmonitoringsystem.Topology.Interface.PatientMonitoringTopology;
import com.kafkastreams.patientmonitoringsystem.Utils.StreamUtils;

@SpringBootApplication
public class PatientMonitoringSystemApplication {

	public static void main(String[] args) {
		SpringApplication.run(PatientMonitoringSystemApplication.class, args);
	}

    @Autowired
	private List<PatientMonitoringTopology> topologies;

	@Bean
	public KafkaStreams getKsClient() {
		StreamUtils.createAllTopics();
		StreamsBuilder builder = new StreamsBuilder();
		for (PatientMonitoringTopology topology : topologies) {
			topology.addTopology(builder);
		}
		var ksClient = new KafkaStreams(builder.build(), StreamUtils.getStreamProperties());
		ksClient.start();
		return ksClient;
	}

}
