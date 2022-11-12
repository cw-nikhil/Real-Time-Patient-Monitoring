package com.kafkastreams.patientmonitoringsystem.Models;

import org.apache.kafka.streams.kstream.internals.TimeWindow;

import lombok.AllArgsConstructor;
import lombok.Data;


@AllArgsConstructor
@Data
public class WindowedString {
    private final String key;

    private final TimeWindow window;
}
