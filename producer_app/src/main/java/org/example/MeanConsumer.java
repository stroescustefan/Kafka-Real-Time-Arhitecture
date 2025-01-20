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
import java.util.Arrays;
import java.util.Properties;

public class MeanConsumer {

    private static final String TEMPERATURE_MEAN_TOPIC = "mean-10min-temperature";
    private static final String HUMIDITY_MEAN_TOPIC = "mean-10min-humidity";
    private static final String MONGO_URI = "mongodb://admin:password@localhost:27017";
    private static final String DATABASE_NAME = "weatherDB";
    private static final String TEMPERATURE_COLLECTION = "mean_temperature";
    private static final String HUMIDITY_COLLECTION = "mean_humidity";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "EXTERNAL://localhost:39092,EXTERNAL://localhost:39093,EXTERNAL://localhost:39094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "mean-consumer-group-2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        MongoClient mongoClient = new MongoClient(new MongoClientURI(MONGO_URI));
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> temperatureCollection = database.getCollection(TEMPERATURE_COLLECTION);
        MongoCollection<Document> humidityCollection = database.getCollection(HUMIDITY_COLLECTION);

        consumer.subscribe(Arrays.asList(TEMPERATURE_MEAN_TOPIC, HUMIDITY_MEAN_TOPIC));

        // Add Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown initiated...");
            try {
                consumer.wakeup(); // Interrupt poll() during shutdown
                consumer.close(); // Gracefully close the consumer
                mongoClient.close(); // Close MongoDB connection
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

                    // Extract fields from the value
                    String windowStart = extractWindowStart(value);
                    String windowEnd = extractWindowEnd(value);
                    double meanValue = record.topic().equals(TEMPERATURE_MEAN_TOPIC)
                            ? extractMeanTemperature(value)
                            : extractMeanHumidity(value);

                    if (record.topic().equals(TEMPERATURE_MEAN_TOPIC)) {
                        saveOrUpdateMean(temperatureCollection, city, windowStart, windowEnd, meanValue, "meanTemperature");
                    } else if (record.topic().equals(HUMIDITY_MEAN_TOPIC)) {
                        saveOrUpdateMean(humidityCollection, city, windowStart, windowEnd, meanValue, "meanHumidity");
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

    private static void saveOrUpdateMean(MongoCollection<Document> collection, String city, String windowStart,
                                         String windowEnd, double meanValue, String fieldName) {
        Document query = new Document("city", city)
                .append("windowStart", windowStart)
                .append("windowEnd", windowEnd);

        Document existingDoc = collection.find(query).first();

        if (existingDoc != null) {
            Document update = new Document("$set", new Document(fieldName, meanValue));
            collection.updateOne(query, update);
        } else {
            Document newDoc = new Document("city", city)
                    .append("windowStart", windowStart)
                    .append("windowEnd", windowEnd)
                    .append(fieldName, meanValue);
            collection.insertOne(newDoc);
        }
    }

    private static String extractWindowStart(String value) {
        String window = extractBetween(value, "Window: [", "]");
        return window.split(" - ")[0].trim();
    }

    private static String extractWindowEnd(String value) {
        String window = extractBetween(value, "Window: [", "]");
        return window.split(" - ")[1].trim();
    }

    private static double extractMeanTemperature(String value) {
        return Double.parseDouble(extractBetween(value, "Mean Temp: ", "").trim());
    }

    private static double extractMeanHumidity(String value) {
        return Double.parseDouble(extractBetween(value, "Mean Humidity: ", "").trim());
    }

    private static String extractBetween(String text, String startDelimiter, String endDelimiter) {
        int start = text.indexOf(startDelimiter) + startDelimiter.length();
        int end = endDelimiter.isEmpty() ? text.length() : text.indexOf(endDelimiter, start);
        if (start < 0 || end < 0) {
            throw new IllegalArgumentException("Failed to extract data: Missing delimiters in: " + text);
        }
        return text.substring(start, end).trim();
    }
}
