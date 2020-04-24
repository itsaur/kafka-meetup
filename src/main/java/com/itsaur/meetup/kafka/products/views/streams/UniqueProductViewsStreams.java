package com.itsaur.meetup.kafka.products.views.streams;

import com.itsaur.meetup.kafka.products.views.ProductView;
import com.itsaur.meetup.kafka.util.Json;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class UniqueProductViewsStreams {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, ProductView> productViewsStream = builder.stream("product-views", Consumed.with(
                new Serdes.StringSerde(), //product
                new Json<>(ProductView.class))
        );

        productViewsStream
                .groupBy((key, value) -> value.getUser() + "-" + value.getProduct(), Grouped.with(
                        new Serdes.StringSerde(),
                        new Json<>(ProductView.class)
                ))
                .windowedBy(SessionWindows.with(Duration.ofMinutes(15)))
                .reduce((value1, value2) -> value2 == null? value1 : value2)
                .toStream()
                .map((key, value) -> KeyValue.pair(value.getProduct(), value))
                .to("unique-product-views", Produced.with(
                        new Serdes.StringSerde(),
                        new Json<>(ProductView.class)
                ));

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "unique-product-views");
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "50");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.start();
    }
}
