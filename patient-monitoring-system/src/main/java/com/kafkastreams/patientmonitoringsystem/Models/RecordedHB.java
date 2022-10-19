package com.kafkastreams.patientmonitoringsystem.Models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RecordedHB {
    public String deviceId;
    public int heartbeat;
}
