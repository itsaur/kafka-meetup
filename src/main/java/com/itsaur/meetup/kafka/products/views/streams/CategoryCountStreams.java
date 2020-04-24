package com.itsaur.meetup.kafka.products.views.streams;

import com.itsaur.meetup.kafka.products.products.Product;
import com.itsaur.meetup.kafka.products.views.ProductView;
import com.itsaur.meetup.kafka.util.Json;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class CategoryCountStreams {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Product> productsTable = builder.table("products", Consumed.with(
                new Serdes.StringSerde(),
                new Json<>(Product.class)
        ));

        KStream<String, ProductView> productViewsStream = builder.stream("product-views", Consumed.with(
                new Serdes.StringSerde(), //product
                new Json<>(ProductView.class))
        );

        productViewsStream.join(productsTable, (productView, product) -> new CategoryView(product.getId(), product.getCategory()))
                .groupBy((key, value) -> value.getCategory(), Grouped.with(
                        new Serdes.StringSerde(),
                        new Json<>(CategoryView.class)
                ))
                .count(Materialized.<String, Long>as(Stores.persistentKeyValueStore("category-count"))
                        .withKeySerde(new Serdes.StringSerde())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .map((key, value) -> KeyValue.pair(key, new CategoryCount(key, value)))
                .to("category-count", Produced.with(
                        new Serdes.StringSerde(),
                        new Json<>(CategoryCount.class)
                ));

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "category-count");
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "50");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.start();
    }

    public static class CategoryCount {
        private String category;
        private long count;

        public CategoryCount() {
        }

        public CategoryCount(String category, long count) {
            this.category = category;
            this.count = count;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }

    public static class CategoryView {
        private String product;
        private String category;

        public CategoryView() {
        }

        public CategoryView(String product, String category) {
            this.product = product;
            this.category = category;
        }

        public String getProduct() {
            return product;
        }

        public void setProduct(String product) {
            this.product = product;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }
    }
}
