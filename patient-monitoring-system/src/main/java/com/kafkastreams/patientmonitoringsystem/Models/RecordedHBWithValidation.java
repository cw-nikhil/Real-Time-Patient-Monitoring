package com.kafkastreams.patientmonitoringsystem.Models;

import lombok.Data;
import lombok.EqualsAndHashCode;


@Data
@EqualsAndHashCode(callSuper=false)
public class RecordedHBWithValidation extends RecordedHB {
    private Boolean isCorrectRecording;
    public RecordedHBWithValidation(String deviceId, int heartbeat, boolean isCorrectRecording) {
        super(deviceId, heartbeat);
        this.isCorrectRecording = isCorrectRecording;
    }
}
