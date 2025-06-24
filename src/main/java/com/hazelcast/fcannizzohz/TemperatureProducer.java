package com.hazelcast.fcannizzohz;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import static java.lang.Thread.sleep;

public class TemperatureProducer {
    private static DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static int[] CITY_ID = new int[]{1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010};

    public static void main(String[] args) {
        // 1) Kafka producer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",   StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("client.id",        "temp-producer");

        // 2) Topic name
        String topic = "temperature_updates";

        // 3) Random generator for city IDs and temps
        Random rnd = new Random();

        // 4) Create the producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            while (true) {
                // pick a city ID between 1001 and 1008
                int cityId = CITY_ID[rnd.nextInt(CITY_ID.length)];
                // pick a temperature between 10 and 32
                int temp   = 10   + rnd.nextInt(23);

                String now = Instant
                        .now()
                        .atZone(ZoneId.systemDefault())
                        .format(TIMESTAMP_FORMATTER);                // build JSON string
                String json = String.format(
                        "{\"city_id\":%d,\"temperature\":%d, \"ts\": \"%s\"}",
                        cityId, temp, now
                );

                // send it (we use cityId as the record key for partitioning)
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, Integer.toString(cityId), json);

                Future<RecordMetadata> future = producer.send(record);
                // option: block until acked
                RecordMetadata meta = future.get();
                System.out.printf(
                        "Sent to %s [%d] @ offset %d: %s%n",
                        meta.topic(), meta.partition(), meta.offset(), json
                );

                // sleep 1s before next event
                sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}