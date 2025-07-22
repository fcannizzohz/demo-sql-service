package com.hazelcast.fcannizzohz;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;

import static com.hazelcast.fcannizzohz.Utils.executeOnClientAndShutdown;

public class InsertIntoCitiesMap {
    public static void main(String[] args) {
        executeOnClientAndShutdown(client -> {
            System.out.println("---------");
            // 1) Get the map typed for JSON values
            IMap<Integer, HazelcastJsonValue> citiesMap = client.getMap("cities");

            // 2) Build your JSON payload
            City rome = new City(2001, "Italy", "Rome", 4_223_885, 41.9028, 12.4964);

            // 3) Wrap it in HazelcastJsonValue
            HazelcastJsonValue jsonValue = new HazelcastJsonValue(rome.toJson());
            Integer key = 200;
            citiesMap.put(key, jsonValue);
            System.out.println("Inserted JSON under key=" + key + ": " + rome);

            // 4) Put directly into the map
            City milan = new City(2002, "Italy", "Milan", 1_371_499, 45.4642, 9.1900);
            IMap<Integer, City> citiesMap2 = client.getMap("cities");
            citiesMap2.put(201, milan);
            System.out.println("Inserted JSON under key=" + key + ": " + milan + "\n");

            // 5) Read from mapping
            int minPopulation = 1_000_000;
            String sql = "SELECT * FROM cities WHERE population > ? order by population desc";
            try (SqlResult result = client.getSql().execute(sql, minPopulation)) {
                System.out.println("Cities with more than " + minPopulation + " people:");
                for (SqlRow row : result) {
                    String cityName = row.getObject("name");
                    int population = row.getObject("population");
                    System.out.println(" - " + cityName + "\t(" + population + ")");
                }
            }
        });
    }
}
