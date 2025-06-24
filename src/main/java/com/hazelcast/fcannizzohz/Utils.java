package com.hazelcast.fcannizzohz;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;

import java.util.function.Consumer;

public final class Utils {

    static void executeOnClientAndShutdown(Consumer<HazelcastInstance> function) {
        ClientConfig cfg = new ClientConfig();
        cfg.setClusterName("dev");                  // default for hazelcast/hazelcast:latest
        ClientNetworkConfig net = cfg.getNetworkConfig();
        net.addAddress("127.0.0.1:5701");           // point at your Docker container

        HazelcastInstance client = HazelcastClient.newHazelcastClient(cfg);
        try {
            if (client == null) {
                throw new IllegalStateException("Unable to continue with null client");
            }
            function.accept(client);
        } finally {
            try {
                if (client != null) {
                    //System.out.println("Client shutting down.");
                    client.shutdown();
                    //System.out.println("Client shut down.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
