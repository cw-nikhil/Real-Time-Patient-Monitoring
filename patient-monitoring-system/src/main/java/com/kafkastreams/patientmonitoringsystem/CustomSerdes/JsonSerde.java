package com.kafkastreams.patientmonitoringsystem.CustomSerdes;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class JsonSerde<T> implements Serde<T> {

    public JsonSerde(Class<T> classType) {
        this.classType = classType;
    }

    private final Class<T> classType;

    @Override
    public Deserializer<T> deserializer() {
        return new JsonDesializer<>();
    }

    @Override
    public Serializer<T> serializer() {
        return new JsonSerializer<>();
    }

    public class JsonDesializer<T> implements Deserializer<T> {

        @Override
        public T deserialize(String topic, byte[] data) {
            Gson gson = new GsonBuilder().create();
            if (data == null) {
                return null;
            }
            Type typeOfT = classType;
            return gson.fromJson(new String(data, StandardCharsets.UTF_8), typeOfT);
        }
    }

    public class JsonSerializer<T> implements Serializer<T> {

        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) {
                return null;
            }
            Gson gson = new Gson();
            return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
        }
    }
}