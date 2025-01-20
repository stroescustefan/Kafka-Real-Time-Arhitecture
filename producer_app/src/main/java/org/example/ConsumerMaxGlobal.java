package org.example;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerMaxGlobal {

    private static final String TEMPERATURE_TOPIC = "global-max-temperature";
    private static final String HUMIDITY_TOPIC = "global-max-humidity";
    private static final String MONGO_URI = "mongodb://admin:password@localhost:27017";
    private static final String DATABASE_NAME = "weatherDB";
    private static final String TEMPERATURE_COLLECTION = "global_max_temperature";
    private static final String HUMIDITY_COLLECTION = "global_max_humidity";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "EXTERNAL://localhost:39092,EXTERNAL://localhost:39093,EXTERNAL://localhost:39094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "global-max-consumer-group-5");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props);
        MongoClient mongoClient = new MongoClient(new MongoClientURI(MONGO_URI));

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

        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> temperatureCollection = database.getCollection(TEMPERATURE_COLLECTION);
        MongoCollection<Document> humidityCollection = database.getCollection(HUMIDITY_COLLECTION);

        consumer.subscribe(Arrays.asList(TEMPERATURE_TOPIC, HUMIDITY_TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Integer> record : records) {
                    String city = record.key();
                    int value = record.value();
                    String timestamp = Instant.now().toString();
                    System.out.println("City: " + city + ", Value: " + value + ", Timestamp: " + timestamp);

                    if (record.topic().equals(TEMPERATURE_TOPIC)) {
                        updateCollection(temperatureCollection, city, value, "maxTemperature", timestamp);
                    } else if (record.topic().equals(HUMIDITY_TOPIC)) {
                        updateCollection(humidityCollection, city, value, "maxHumidity", timestamp);
                    }

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
    private static void updateCollection(MongoCollection<Document> collection, String city, int value,
                                         String fieldName, String timestamp) {
        Document query = new Document("_id", city);
        Document existingDoc = collection.find(query).first();

        if (existingDoc != null) {
            String lastUpdated = existingDoc.getString("lastUpdated");

            if (Instant.parse(timestamp).isAfter(Instant.parse(lastUpdated))) {
                Document update = new Document("$set", new Document()
                        .append("city", city)
                        .append(fieldName, value)
                        .append("lastUpdated", timestamp));
                collection.updateOne(query, update);
            }
        } else {
            Document newDoc = new Document("_id", city)
                    .append("city", city)
                    .append(fieldName, value)
                    .append("lastUpdated", timestamp);
            collection.insertOne(newDoc);
        }
    }
}
