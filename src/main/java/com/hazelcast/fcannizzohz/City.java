package com.hazelcast.fcannizzohz;

public record City(Integer city_id, String country, String name, Integer population) {
    private static String escape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Serialize this City into a JSON string:
     * {"country":"…","name":"…"}
     */
    public String toJson() {
        // Basic escaping of backslashes and quotes
        return String.format("{\"city_id\":%d, \"country\":\"%s\",\"name\":\"%s\", \"population\":%d}", city_id, escape(country), escape(name),
                population);
    }
}
