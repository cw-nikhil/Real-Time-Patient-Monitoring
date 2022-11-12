package com.kafkastreams.patientmonitoringsystem.Models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Heartbeat {
    private long timestamp;
    private String patientId;
    private String deviceId;
}
