package com.kafkastreams.patientmonitoringsystem.Models;

import lombok.Data;

@Data
public class DeviceStats {
    //primary key
    private String deviceId;

    private int totalRecordings;
    private int correctRecordings;
}
