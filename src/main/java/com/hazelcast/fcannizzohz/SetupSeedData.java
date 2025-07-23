package com.hazelcast.fcannizzohz;

import com.hazelcast.sql.SqlService;

import static com.hazelcast.fcannizzohz.Context.DEV_LOCALHOST;
import static com.hazelcast.fcannizzohz.Utils.executeOnClientAndShutdown;
import static com.hazelcast.fcannizzohz.Utils.runSQLFromFile;

public class SetupSeedData {

    private static void runSQL(String fileName, Context context) {
        executeOnClientAndShutdown(client -> {
            SqlService sqlService = client.getSql();
            runSQLFromFile(sqlService, fileName);
        }, context);
    }

    static class SetupCitiesMapping {

        static void run(Context context) {
            runSQL("src/main/resources/cities_mapping.sql", context);
        }

        public static void main(String[] args) {
            run(DEV_LOCALHOST);
        }
    }

    static class SetupCitiesData {
        static void run(Context context) {
            runSQL("src/main/resources/cities_data.sql", context);
        }

        public static void main(String[] args) {
            run(DEV_LOCALHOST);
        }
    }

    static class SetupTemperaturesMapping {
        static void run(Context context) {
            runSQL("src/main/resources/temperatures_mapping.sql", context);
        }

        public static void main(String[] args) {
            run(DEV_LOCALHOST);
        }
    }

    static class SetupTemperatureUpdatesMapping {
        static void run(Context context) {
            runSQL("src/main/resources/temperature_updates_mapping.sql",  context);
        }

        public static void main(String[] args) {
            run(DEV_LOCALHOST);
        }
    }

    static class SetupTemperatureUpdatesOrderedView {
        static void run(Context context) {
            runSQL("src/main/resources/temperature_updates_ordered_view.sql", context);
        }

        public static void main(String[] args) {
            run(DEV_LOCALHOST);
        }
    }

    static class SetupTemperaturesEnrichedView {
        static void run(Context context) {
            runSQL("src/main/resources/temperatures_enriched_view.sql", context);
        }

        public static void main(String[] args) {
            run(DEV_LOCALHOST);
        }
    }

}
