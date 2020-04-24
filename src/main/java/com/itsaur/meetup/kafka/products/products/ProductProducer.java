package com.itsaur.meetup.kafka.products.products;

import com.itsaur.meetup.kafka.util.Json;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.itsaur.meetup.kafka.products.Parameters.NUM_OF_PRODUCTS;

public class ProductProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "3000");

        KafkaProducer<String, Product> producer = new KafkaProducer<>(
                properties,
                new StringSerializer(),
                new Json<>(Product.class));

        Iterable<Product> products = new RandomProductDataset(NUM_OF_PRODUCTS);
        StopWatch watch = StopWatch.createStarted();

        for (Product product: products) {
            producer.send(new ProducerRecord<>(
                    "products",
                    product.getId(),
                    product
            ));
        }

        producer.close();
        watch.stop();
        System.out.println("Finished in " + watch.getTime(TimeUnit.SECONDS) + " secs");
    }
}
