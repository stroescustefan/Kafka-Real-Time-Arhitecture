package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Properties;

public class WeatherProducer {

    private static final String KAFKA_BROKER = "EXTERNAL://localhost:39092,EXTERNAL://localhost:39093,EXTERNAL://localhost:39094";
    private static final String TOPIC = "weather-raw-data";
    private static final String RSS_FEED_URL = "http://localhost:8080/rss";
    private final Producer<String, String> producer;
    private static final Logger logger = LoggerFactory.getLogger(WeatherProducer.class);

    public WeatherProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        this.producer = new KafkaProducer<>(props);
    }

    public static void main(String[] args) {
        WeatherProducer producerApp = new WeatherProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Closing producer...");
            producerApp.producer.close();
        }));
        producerApp.start();
    }

    public void start() {
        while (true) {
            try {
                List<Element> items = fetchRssFeedItems();
                for (Element item : items) {
                    String cityName = item.getChildText("city");
                    String timestamp = item.getChildText("timestamp");
                    String humidity = item.getChildText("humidity");
                    String temperature = item.getChildText("temperature");

                    String value = String.format("timestamp:%s,humidity:%s,temperature:%s",
                            timestamp, humidity, temperature);

                    sendToKafka(cityName, value);
                }
                producer.flush();
                Thread.sleep(10000);

            } catch (Exception e) {
                logger.error("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private List<Element> fetchRssFeedItems() throws Exception {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(RSS_FEED_URL);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);

            SAXBuilder saxBuilder = new SAXBuilder();
            try (InputStream inputStream = connection.getInputStream()) {
                Document document = saxBuilder.build(inputStream);
                Element root = document.getRootElement();
                Element channel = root.getChild("channel");
                return channel.getChildren("item");
            } catch (Exception e) {
                logger.error("Error fetching RSS feed: " + e.getMessage());
                throw e;
            }
        } catch (Exception e) {
            logger.error("Error fetching RSS feed: " + e.getMessage());
            throw e;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private void sendToKafka(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Error producing message: " + exception.getMessage());
                System.err.println("Error producing message: " + exception.getMessage());
            } else {
                logger.info("Message sent to topic " + metadata.topic() +
                        ", partition " + metadata.partition() +
                        ", offset " + metadata.offset() +
                        ", key: " + key +
                        ", value: " + value);
            }
        });
    }
}
