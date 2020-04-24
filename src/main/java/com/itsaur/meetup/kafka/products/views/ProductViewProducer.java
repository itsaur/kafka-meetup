package com.itsaur.meetup.kafka.products.views;

import com.itsaur.meetup.kafka.util.Json;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.itsaur.meetup.kafka.products.Parameters.*;

public class ProductViewProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "3000");

        KafkaProducer<String, ProductView> producer = new KafkaProducer<>(
                properties,
                new StringSerializer(),
                new Json<>(ProductView.class));

        Iterable<ProductView> productViews = new StreamRandomProductViewDataset(
                NUM_OF_PRODUCT_VIEWS,
                NUM_OF_USERS,
                NUM_OF_PRODUCTS);
        StopWatch watch = StopWatch.createStarted();

        for (ProductView productView : productViews) {
            producer.send(new ProducerRecord<>(
                    "product-views",
                    null,
                    productView.getTime().toInstant().toEpochMilli(),
                    productView.getProduct(),
                    productView
            ));
        }

        producer.close();
        watch.stop();
        System.out.println("Finished in " + watch.getTime(TimeUnit.SECONDS) + " secs");
    }
}
