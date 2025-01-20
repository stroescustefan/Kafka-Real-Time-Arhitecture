package org.example;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AnomalyConsumer {

    private static final String ANOMALY_TOPIC = "anomalies";
    private static final String MONGO_URI = "mongodb://admin:password@localhost:27017";
    private static final String DATABASE_NAME = "weatherDB";
    private static final String COLLECTION_NAME = "anomalies";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "EXTERNAL://localhost:39092,EXTERNAL://localhost:39093,EXTERNAL://localhost:39094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "anomaly-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        MongoClient mongoClient = new MongoClient(new MongoClientURI(MONGO_URI));
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> anomalyCollection = database.getCollection(COLLECTION_NAME);

        consumer.subscribe(Collections.singletonList(ANOMALY_TOPIC));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown initiated...");
            try {
                consumer.wakeup();
                consumer.close();
                mongoClient.close();
                System.out.println("Shutdown complete.");
            } catch (Exception e) {
                System.err.println("Error during shutdown: " + e.getMessage());
            }
        }));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    String city = record.key();
                    String value = record.value();

                    String timestamp = extractValue(value, "timestamp");
                    int humidity = Integer.parseInt(extractValue(value, "humidity"));
                    int temperature = Integer.parseInt(extractValue(value, "temperature"));

                    saveAnomaly(anomalyCollection, city, timestamp, humidity, temperature);

                    consumer.commitAsync((offsets, exception) -> {
                        if (exception != null) {
                            System.err.println("Failed to commit offsets asynchronously: " + exception.getMessage());
                        }
                    });
                }

                consumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            mongoClient.close();
        }
    }

    private static void saveAnomaly(MongoCollection<Document> collection, String city, String timestamp, int humidity, int temperature) {
        Document anomalyDocument = new Document("city", city)
                .append("timestamp", timestamp)
                .append("humidity", humidity)
                .append("temperature", temperature);

        collection.insertOne(anomalyDocument);
    }

    private static String extractValue(String value, String key) {
        for (String part : value.split(",")) {
            String[] keyValue = part.split(":");
            if (keyValue[0].equals(key)) {
                return keyValue[1];
            }
        }
        throw new IllegalArgumentException("Key not found in value: " + key);
    }
}
