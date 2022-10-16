package com.kafkastreams.patientmonitoringsystem.Models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Heartbeat {
    private long timestamp;
    private int patientId;
    private int deviceId;
}
