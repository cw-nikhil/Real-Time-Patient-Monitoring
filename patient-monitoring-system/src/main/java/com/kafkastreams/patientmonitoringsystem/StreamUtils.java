package com.kafkastreams.patientmonitoringsystem;

import java.io.FileInputStream;
import java.util.Properties;

public class StreamUtils {
    private static final String propertiesFilePath = "";
    public static Properties getStreamProperties() {
        try {
            Properties properties = new Properties();
            FileInputStream fis = new FileInputStream(propertiesFilePath);
            properties.load(fis);
            return properties;
        }
        catch(Exception ex) {
            return new Properties();
        }
    }
}
