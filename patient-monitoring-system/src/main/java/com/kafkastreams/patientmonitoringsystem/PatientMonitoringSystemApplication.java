package com.kafkastreams.patientmonitoringsystem;

import java.util.Properties;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.kafkastreams.patientmonitoringsystem.Utils.StreamUtils;

@SpringBootApplication
public class PatientMonitoringSystemApplication {

	public static void main(String[] args) {
		SpringApplication.run(PatientMonitoringSystemApplication.class, args);
	}

	@Bean
	public Properties getStreamsProperties() {
		return StreamUtils.getStreamProperties();
	}

}
