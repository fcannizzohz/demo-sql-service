package com.hazelcast.fcannizzohz;

import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;

import java.util.Map;

import static com.hazelcast.fcannizzohz.Utils.executeOnClientAndShutdown;

public class ReadData {
    public static void main(String[] args) {
        executeOnClientAndShutdown(client -> {
            System.out.println("---------------");

            IMap<String, Object> map = client.getMap("cities");
            // 4) Print all entries
            System.out.println("Entries in map '" + map.getName() + "':");
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                System.out.printf("  key=%s, value=%s%n", entry.getKey(), entry.getValue());
            }

            System.out.println("---------------");

            String sql = "SELECT * FROM cities order by population desc";
            try (SqlResult result = client.getSql().execute(sql)) {
                System.out.println("Cities:");
                for (SqlRow row : result) {
                    String cityName = row.getObject("name");
                    String country = row.getObject("country");
                    int population = row.getObject("population");
                    System.out.println(" - " + cityName + ", " + country + " (" + population + ")");
                }
            }
        }, Context.DEV_LOCALHOST);
    }

}
