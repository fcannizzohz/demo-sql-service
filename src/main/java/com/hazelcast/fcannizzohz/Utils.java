package com.hazelcast.fcannizzohz;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.function.Function;

public final class Utils {

    static void runSQLFromFile(SqlService sqlService, String fileName) {
        String data;
        try {
            data = new String(Files.readAllBytes(Paths.get(fileName)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (SqlResult result = sqlService.execute(data)) {
            System.out.println(result);
        }
    }

    static void executeOnClientAndShutdown(Consumer<HazelcastInstance> consumer) {
        executeOnClientAndShutdown((Function<HazelcastInstance, Void>) hazelcastInstance -> {
            consumer.accept(hazelcastInstance);
            return null;
        });
    }

    static <T> T executeOnClientAndShutdown(Function<HazelcastInstance, T> function) {
        ClientConfig cfg = new ClientConfig();
        cfg.setClusterName("dev");                  // default for hazelcast/hazelcast:latest
        ClientNetworkConfig net = cfg.getNetworkConfig();
        net.addAddress("127.0.0.1:5701");           // point at your Docker container

        HazelcastInstance client = HazelcastClient.newHazelcastClient(cfg);
        try {
            return function.apply(client);
        } finally {
            try {
                //System.out.println("Client shutting down.");
                client.shutdown();
                //System.out.println("Client shut down.");
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

    }

}
