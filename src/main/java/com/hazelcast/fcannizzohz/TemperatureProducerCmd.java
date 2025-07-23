package com.hazelcast.fcannizzohz;

import picocli.CommandLine;
import picocli.CommandLine.*;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;

@Command(name = "temperature-producer",
        mixinStandardHelpOptions = true,
        description = "Produces random temperature events to Kafka.")
public class TemperatureProducerCmd implements Runnable {

    @Option(names = {"-b", "--bootstrap"}, defaultValue = "localhost:9092",
            description = "Kafka bootstrap servers host:port (default: ${DEFAULT-VALUE})")
    String bootstrap;

    @Option(names = {"-c", "--cluster"}, defaultValue = "dev",
            description = "Hazelcast cluster name (default: ${DEFAULT-VALUE})")
    String cluster;

    @Option(names = {"-m", "--member"}, defaultValue = "127.0.0.1:5701",
            description = "Hazelcast member address host:port, (default: ${DEFAULT-VALUE})")
    String member;

    @Option(names = {"--seed"}, defaultValue = "false", negatable = true,
            description = "Load seed data into Hazelcast (default: ${DEFAULT-VALUE})")
    boolean seed;

    @Override
    public void run() {
        System.out.println("Connecting to bootstrap server: " + bootstrap);
        System.out.println("Connecting to cluster: " + cluster + " with member: " + member);
        Context c = new Context(cluster, member);
        if (seed) {
            System.out.println("Setting seed data and mappings");
            SetupSeedData.SetupCitiesMapping.run(c);
            SetupSeedData.SetupCitiesData.run(c);
            SetupSeedData.SetupTemperaturesMapping.run(c);
            SetupSeedData.SetupTemperatureUpdatesMapping.run(c);
            SetupSeedData.SetupTemperatureUpdatesOrderedView.run(c);
            SetupSeedData.SetupTemperaturesEnrichedView.run(c);
        }
        TemperatureProducer.run(bootstrap, new Context(cluster, member));
    }

    public static void main(String[] args) {
        int code = new CommandLine(new TemperatureProducerCmd()).execute(args);
        System.exit(code);
    }
}
