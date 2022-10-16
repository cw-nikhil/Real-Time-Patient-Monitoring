package com.kafkastreams.patientmonitoringsystem.Models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor


public class BloodPressure {
    private int systolicPressure;
    private int diastolicPressure;
    private long timestamp;
    private int deviceId;
}
