package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class KafkaStreamsProcessWeather {

    private static final String INPUT_TOPIC = "weather-raw-data";
    private static final String ANOMALY_TOPIC = "anomalies";
    private static final String MAX_TOPIC = "global-max";
    private static final String MEAN_TOPIC = "mean-10min";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-weather-processing");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "EXTERNAL://localhost:39092,EXTERNAL://localhost:39093,EXTERNAL://localhost:39094");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "kafka-streams-weather-processing-10");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);

        KStream<String, String> validStream = inputStream.filter((key, value) -> {
            try {
                String[] parts = value.split(",");
                boolean hasTimestamp = parts[0].startsWith("timestamp:");
                boolean hasHumidity = parts[1].startsWith("humidity:");
                boolean hasTemperature = parts[2].startsWith("temperature:");
                return hasTimestamp && hasHumidity && hasTemperature;
            } catch (Exception e) {
                return false;
            }
        });

        KStream<String, String>[] branches = validStream.branch(
                (key, value) -> isAnomaly(value),
                (key, value) -> !isAnomaly(value)
        );

        KStream<String, String> anomalyStream = branches[0];
        anomalyStream.to(ANOMALY_TOPIC);


        KStream<String, String> nonAnomalyStream = branches[1];

        KTable<String, Integer> maxTemperature = nonAnomalyStream
                .mapValues(KafkaStreamsProcessWeather::extractTemperature)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .reduce(Integer::max, Materialized.as("max-temperature-store"));

        KTable<String, Integer> maxHumidity = nonAnomalyStream
                .mapValues(KafkaStreamsProcessWeather::extractHumidity)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .reduce(Integer::max, Materialized.as("max-humidity-store"));

        maxTemperature.toStream().to(MAX_TOPIC + "-temperature", Produced.with(Serdes.String(), Serdes.Integer()));
        maxHumidity.toStream().to(MAX_TOPIC + "-humidity", Produced.with(Serdes.String(), Serdes.Integer()));

        computeTemperatureMeanOver10Minutes(nonAnomalyStream);
        computeHumidityMeanOver10Minutes(nonAnomalyStream);

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void computeHumidityMeanOver10Minutes(KStream<String, String> nonAnomalyStream) {
        nonAnomalyStream
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(10)))
                .aggregate(
                        () -> "0,0",
                        (key, value, aggStr) -> {
                            AggregateState agg = AggregateState.fromString(aggStr);
                            agg.add(extractHumidity(value));
                            return agg.toString();
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .map((windowedKey, aggStr) -> {
                    AggregateState agg = AggregateState.fromString(aggStr);

                    String city = windowedKey.key();
                    long windowStart = windowedKey.window().start();
                    long windowEnd = windowedKey.window().end();

                    String result = String.format(
                            "Window: [%s - %s], Mean Humidity: %.2f",
                            new java.util.Date(windowStart),
                            new java.util.Date(windowEnd),
                            agg.calculateMean()
                    );

                    return KeyValue.pair(city, result);
                })
                .to(MEAN_TOPIC + "-humidity", Produced.with(Serdes.String(), Serdes.String()));
    }

    private static void computeTemperatureMeanOver10Minutes(KStream<String, String> nonAnomalyStream) {
        nonAnomalyStream
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(10)))
                .aggregate(
                        () -> "0,0",
                        (key, value, aggStr) -> {
                            AggregateState agg = AggregateState.fromString(aggStr);
                            agg.add(extractTemperature(value));
                            return agg.toString();
                        }, Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .map((windowedKey, aggStr) -> {
                    AggregateState agg = AggregateState.fromString(aggStr);

                    String city = windowedKey.key();
                    long windowStart = windowedKey.window().start();
                    long windowEnd = windowedKey.window().end();

                    String result = String.format(
                            "Window: [%s - %s], Mean Temp: %.2f",
                            new java.util.Date(windowStart),
                            new java.util.Date(windowEnd),
                            agg.calculateMean()
                    );
                    return KeyValue.pair(city, result);
                })
                .to(MEAN_TOPIC + "-temperature", Produced.with(Serdes.String(), Serdes.String()));
    }

    private static boolean isAnomaly(String value) {
        int temperature = extractTemperature(value);
        int humidity = extractHumidity(value);

        return temperature > 50 || humidity < 10;
    }

    private static int extractTemperature(String value) {
        String[] parts = value.split(",");
        return Integer.parseInt(parts[2].split(":")[1]);
    }

    private static int extractHumidity(String value) {
        String[] parts = value.split(",");
        return Integer.parseInt(parts[1].split(":")[1]);
    }
}

