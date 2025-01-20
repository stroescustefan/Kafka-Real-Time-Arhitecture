package org.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithAvroAndSchemaRegistry {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWithAvroAndSchemaRegistry.class);

    public static void main(String[] args) {
        String topic = "test-avro-topic";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "EXTERNAL://localhost:39092,EXTERNAL://localhost:39093,EXTERNAL://localhost:39094");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, org.example.User> producer = new KafkaProducer<>(properties);

        User user = org.example.User.newBuilder()
                .setName("John Doe")
                .setAge(25)
                .build();

        ProducerRecord<String, User> record = new ProducerRecord<>(topic, "user", user);
        producer.send(record, (recordMetadata, e) -> {
            if (e == null) {
                logger.info("Received new metadata. \n" +
                        "Topic: " + recordMetadata.topic() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset() + "\n" +
                        "Timestamp: " + recordMetadata.timestamp());
            } else {
                logger.error("Error while producing", e);
            }
        });
        producer.flush();
        producer.close();
    }
}
