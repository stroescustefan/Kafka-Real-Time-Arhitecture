package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerWithAvroAndSchemaRegistry {
    public static void main(String[] args) {
        String topic = "test-avro-topic";

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "EXTERNAL://localhost:39092,EXTERNAL://localhost:39093,EXTERNAL://localhost:39094");
        props.put("group.id", "test-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("specific.avro.reader", "true");

        KafkaConsumer<String, org.example.User> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        System.out.println("Waiting for messages...");
        while (true) {
            ConsumerRecords<String, User> records = consumer.poll(Duration.ofSeconds(10));

            for (ConsumerRecord<String, User> record : records) {
                System.out.printf("Consumed record with key %s and value %s%n", record.key(), record.value());
            }
        }
    }
}

