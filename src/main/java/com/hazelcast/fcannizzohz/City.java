package com.hazelcast.fcannizzohz;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public record City(Integer city_id, String country, String name, Integer population, Double latitude, Double longitude) {
    private static final ObjectMapper mapper = new ObjectMapper();

    public String toJson() {
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize City to JSON", e);
        }
    }
}
