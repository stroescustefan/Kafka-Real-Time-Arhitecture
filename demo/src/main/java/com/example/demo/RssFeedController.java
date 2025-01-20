package com.example.demo;

import com.rometools.rome.feed.synd.*;
import com.rometools.rome.io.SyndFeedOutput;
import org.jdom2.Element;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

@RestController
public class RssFeedController {

    private static final String[] CITIES = {
            "New York", "London", "Tokyo", "Sydney", "Paris",
            "Mumbai", "Beijing", "Cape Town", "Dubai", "Rio de Janeiro"
    };

    private static final Random RANDOM = new Random();

    @GetMapping(value = "/rss", produces = MediaType.APPLICATION_XML_VALUE)
    public String getRssFeed() {
        try {

            SyndFeed feed = new SyndFeedImpl();
            feed.setFeedType("rss_2.0");
            feed.setTitle("Meteorological Data Feed");
            feed.setLink("http://localhost:8080/rss");
            feed.setDescription("Real-time meteorological updates for 10 cities, including clear anomalies.");

            List<SyndEntry> entries = new ArrayList<>();
            for (String city : CITIES) {
                SyndEntry entry = new SyndEntryImpl();
                entry.setTitle("Weather Update for " + city);

                String readableCity = city.replace(" ", "-");
                entry.setLink("http://localhost:8080/weather/" + readableCity);

                String timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date());
                int temperature = generateTemperature();
                int humidity = generateHumidity();

                Element cityElement = new Element("city");
                cityElement.setText(city);
                Element timestampElement = new Element("timestamp");
                timestampElement.setText(timestamp);
                Element temperatureElement = new Element("temperature");
                temperatureElement.setText(String.valueOf(temperature));
                Element humidityElement = new Element("humidity");
                humidityElement.setText(String.valueOf(humidity));

                List<Element> foreignMarkup = new ArrayList<>();
                foreignMarkup.add(cityElement);
                foreignMarkup.add(timestampElement);
                foreignMarkup.add(temperatureElement);
                foreignMarkup.add(humidityElement);
                entry.setForeignMarkup(foreignMarkup);

                entries.add(entry);
            }
            feed.setEntries(entries);

            StringWriter writer = new StringWriter();
            SyndFeedOutput output = new SyndFeedOutput();
            output.output(feed, writer);

            return writer.toString();

        } catch (Exception e) {
            e.printStackTrace();
            return "Error generating RSS feed.";
        }
    }

    private int generateTemperature() {
        int normalTemperature = RANDOM.nextInt(21) + 15;
        boolean isAnomaly = RANDOM.nextDouble() < 0.1;
        return isAnomaly ? RANDOM.nextInt(101) + 100 : normalTemperature;
    }

    private int generateHumidity() {
        int normalHumidity = RANDOM.nextInt(21) + 30;
        boolean isAnomaly = RANDOM.nextDouble() < 0.1;
        return isAnomaly ? RANDOM.nextInt(6) : normalHumidity;
    }
}
