package com.hazelcast.fcannizzohz;

import com.hazelcast.sql.SqlService;

import static com.hazelcast.fcannizzohz.Utils.executeOnClientAndShutdown;
import static com.hazelcast.fcannizzohz.Utils.runSQLFromFile;

public class SetupSeedData {

    static class SetupCitiesMapping {
        public static void main(String[] args) {
            executeOnClientAndShutdown(client -> {
                SqlService sqlService = client.getSql();
                runSQLFromFile(sqlService, "src/main/resources/cities_mapping.sql");
            });
        }
    }

    static class SetupCitiesData {
        public static void main(String[] args) {
            executeOnClientAndShutdown(client -> {
                SqlService sqlService = client.getSql();
                runSQLFromFile(sqlService, "src/main/resources/cities_data.sql");
            });
        }
    }

    static class SetupTemperaturesMapping {
        public static void main(String[] args) {
            executeOnClientAndShutdown(client -> {
                SqlService sqlService = client.getSql();
                runSQLFromFile(sqlService, "src/main/resources/temperatures_mapping.sql");
            });
        }
    }

    static class SetupTemperatureUpdatesMapping {
        public static void main(String[] args) {
            executeOnClientAndShutdown(client -> {
                SqlService sqlService = client.getSql();
                runSQLFromFile(sqlService, "src/main/resources/temperature_updates_mapping.sql");
            });
        }
    }

    static class SetupTemperatureUpdatesOrderedView {
        public static void main(String[] args) {
            executeOnClientAndShutdown(client -> {
                SqlService sqlService = client.getSql();
                runSQLFromFile(sqlService, "src/main/resources/temperature_updates_ordered_view.sql");
            });
        }
    }

    static class SetupTemperaturesEnrichedView {
        public static void main(String[] args) {
            executeOnClientAndShutdown(client -> {
                SqlService sqlService = client.getSql();
                runSQLFromFile(sqlService, "src/main/resources/temperatures_enriched_view.sql");
            });
        }
    }

}
