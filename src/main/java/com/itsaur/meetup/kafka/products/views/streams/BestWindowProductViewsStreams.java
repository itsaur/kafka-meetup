package com.itsaur.meetup.kafka.products.views.streams;

import com.itsaur.meetup.kafka.products.views.ProductView;
import com.itsaur.meetup.kafka.util.Json;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

public class BestWindowProductViewsStreams {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, ProductView> productViewsStream = builder.stream("product-views", Consumed.with(
                new Serdes.StringSerde(), //product
                new Json<>(ProductView.class))
        );

        productViewsStream.groupByKey(Grouped.with(
                new Serdes.StringSerde(),
                new Json<>(ProductView.class)
        ))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
                .count(Materialized.as(Stores.persistentWindowStore(
                        "product-5min-windows", Duration.ofDays(1), Duration.ofMinutes(5), false)))
                .suppress(untilWindowCloses(unbounded()))
                .toStream()
                .map((key, value) -> KeyValue.pair(
                        key.key(),
                        new ProductViewWindowCount(
                                key.key(),
                                OffsetDateTime.ofInstant(key.window().startTime(), ZoneOffset.UTC),
                                OffsetDateTime.ofInstant(key.window().endTime(), ZoneOffset.UTC),
                                value
                        )
                ))
                .groupByKey(Grouped.with(
                        new Serdes.StringSerde(),
                        new Json<>(ProductViewWindowCount.class)
                ))
                .reduce((previousValue, newValue) -> {
                    if (newValue.count > previousValue.count) {
                        return newValue;
                    } else {
                        return previousValue;
                    }
                }, Materialized.<String, ProductViewWindowCount>as(Stores.persistentKeyValueStore("product-view-best-window"))
                        .withKeySerde(new Serdes.StringSerde())
                        .withValueSerde(new Json<>(ProductViewWindowCount.class)))
                .toStream()
                .to("product-view-best-window", Produced.with(
                        new Serdes.StringSerde(),
                        new Json<>(ProductViewWindowCount.class)
                ));

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "product-view-best-window");
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "50");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.start();
    }

    public static class ProductViewWindowCount {
        private String product;
        private OffsetDateTime from;
        private OffsetDateTime to;
        private long count;

        public ProductViewWindowCount() {
        }

        public ProductViewWindowCount(String product, OffsetDateTime from, OffsetDateTime to, long count) {
            this.product = product;
            this.from = from;
            this.to = to;
            this.count = count;
        }

        public String getProduct() {
            return product;
        }

        public void setProduct(String product) {
            this.product = product;
        }

        public OffsetDateTime getFrom() {
            return from;
        }

        public void setFrom(OffsetDateTime from) {
            this.from = from;
        }

        public OffsetDateTime getTo() {
            return to;
        }

        public void setTo(OffsetDateTime to) {
            this.to = to;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }
}
