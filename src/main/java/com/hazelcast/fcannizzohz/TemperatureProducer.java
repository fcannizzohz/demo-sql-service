package com.hazelcast.fcannizzohz;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import org.apache.kafka.common.serialization.StringSerializer;

import static com.hazelcast.fcannizzohz.Utils.executeOnClientAndShutdown;

public class TemperatureProducer implements AutoCloseable {
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final KafkaProducer<String, String> producer;
    private final ScheduledExecutorService executor;
    private final String topic = "temperature_updates";

    public static void run(String bootstrap, Context context) {
        System.out.println("Finding available city IDs");
        List<Integer> cityIds = TemperatureProducer.findCityIDs(context);
        System.out.println("Found city IDs: " +  cityIds);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("client.id", "temp-producer");

        try (TemperatureProducer prod = new TemperatureProducer(props, cityIds)) {
            System.out.println("Streaming started. Ctrl+C to exit.");
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        TemperatureProducer.run("localhost:9092", new Context("dev", "localhost:5701"));
    }

    public TemperatureProducer(Properties props, List<Integer> cityIDs) {
        this.producer = new KafkaProducer<>(props);
        this.executor = Executors.newSingleThreadScheduledExecutor();

        Runnable task = () -> {
            try {
                int cityId = cityIDs.get(ThreadLocalRandom.current().nextInt(cityIDs.size()));
                int temp = 10 + ThreadLocalRandom.current().nextInt(23);
                String now = Instant.now().atZone(ZoneId.systemDefault()).format(TS_FMT);

                String json = String.format("{\"city_id\":%d,\"temperature\":%d,\"ts\":\"%s\"}",
                        cityId, temp, now);

                producer.send(new ProducerRecord<>(topic, Integer.toString(cityId), json),
                        (meta, ex) -> {
                            if (ex == null) {
                                System.out.printf("Sent to %s[%d]@%d: %s%n",
                                        meta.topic(), meta.partition(), meta.offset(), json);
                            } else {
                                ex.printStackTrace(System.err);
                            }
                        });
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        };

        executor.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);
    }

    public static List<Integer> findCityIDs(Context context) {
        return executeOnClientAndShutdown(client -> {
            SqlService sql = client.getSql();
            try (SqlResult r = sql.execute("select city_id from cities")) {
                return r.stream()
                        .map(row -> row.getObject("city_id"))
                        .map(o -> Integer.parseInt(o.toString()))
                        .collect(Collectors.toList());
            }
        }, context);
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        producer.close();
    }
}
