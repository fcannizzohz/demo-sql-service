package com.hazelcast.fcannizzohz;

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hazelcast.fcannizzohz.Utils.executeOnClientAndShutdown;

public class TemperatureProducer implements AutoCloseable {
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final KafkaProducer<String, String> producer;
    private final ScheduledExecutorService executor;

    public TemperatureProducer(Properties props, List<Integer> cityIDs, String topic) {
        this.producer = new KafkaProducer<>(props);
        this.executor = Executors.newSingleThreadScheduledExecutor();

        Runnable task = () -> {
            try {
                int cityId = cityIDs.get(ThreadLocalRandom.current().nextInt(cityIDs.size()));
                int temp = 10 + ThreadLocalRandom.current().nextInt(23);

                String now = Instant.now().atZone(ZoneId.systemDefault()).format(TIMESTAMP_FORMATTER);

                String json = String.format("{\"city_id\":%d,\"temperature\":%d, \"ts\": \"%s\"}", cityId, temp, now);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, Integer.toString(cityId), json);

                producer.send(record, (meta, ex) -> {
                    if (ex == null) {
                        System.out.printf("Sent to %s [%d] @ offset %d: %s%n", meta.topic(), meta.partition(), meta.offset(),
                                json);
                    } else {
                        System.err.println(ex.getMessage());
                    }
                });
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        };

        executor.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);
    }

    public static void main(String[] args) {
        // 1) Kafka producer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("client.id", "temp-producer");

        // 2) Topic name
        String topic = "temperature_updates";
        List<Integer> cityIDs = findCityIDs();
        try (TemperatureProducer streamingProducer = new TemperatureProducer(props, cityIDs, topic)) {
            System.out.println("Streaming started. Press Ctrl+C to exit...");
            Thread.currentThread().join(); // Keep the main thread alive
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static List<Integer> findCityIDs() {
        return executeOnClientAndShutdown(client -> {
            SqlService sqlService = client.getSql();
            try (SqlResult result = sqlService.execute("select city_id from cities")) {
                return result.stream().map(row -> row.getObject("city_id")).map(obj -> Integer.parseInt(obj.toString()))
                             .collect(Collectors.toList());
            }

        });
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