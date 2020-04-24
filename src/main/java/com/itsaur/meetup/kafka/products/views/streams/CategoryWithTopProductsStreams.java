package com.itsaur.meetup.kafka.products.views.streams;

import com.itsaur.meetup.kafka.products.products.Product;
import com.itsaur.meetup.kafka.products.views.ProductView;
import com.itsaur.meetup.kafka.util.Json;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.util.*;

public class CategoryWithTopProductsStreams {

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

        productViewsStream
                .groupByKey(Grouped.with(
                        new Serdes.StringSerde(), //product
                        new Json<>(ProductView.class)))
                .count(Materialized.<String, Long>as(Stores.persistentKeyValueStore("product-count"))
                        .withKeySerde(new Serdes.StringSerde())
                        .withValueSerde(new Serdes.LongSerde()))
                .toStream()
                .join(productsTable, (views, product) -> new EnrichedProduct(
                        product.getId(),
                        product.getName(), product.getCategory(),
                        views
                ), Joined.with(
                        new Serdes.StringSerde(),
                        Serdes.Long(),
                        new Json<>(Product.class)
                ))
                .groupBy((key, value) -> value.getCategory(), Grouped.with(
                        new Serdes.StringSerde(),
                        new Json<>(EnrichedProduct.class)
                ))
                .<CategoryTopProducts>aggregate(() -> null, (key, value, aggregate) -> {
                    CategoryTopProducts categoryTopProducts = aggregate;
                    if (categoryTopProducts == null) {
                        categoryTopProducts = new CategoryTopProducts(
                                value.getCategory(),
                                new TreeSet<>()
                        );
                    }

                    final CategoryTopProduct newCategoryTopProduct = new CategoryTopProduct(
                            value.getId(),
                            value.getName(),
                            value.getViews()
                    );

                    CategoryTopProduct toBeRemoved = categoryTopProducts
                            .getTopProducts()
                            .stream()
                            .filter(categoryTopProduct -> categoryTopProduct.getId().equals(value.getId()))
                            .findFirst()
                            .orElse(null);

                    if (toBeRemoved == null && categoryTopProducts.getTopProducts().size() > 20) {
                        toBeRemoved = categoryTopProducts.getTopProducts()
                                .stream()
                                .filter(categoryTopProduct -> categoryTopProduct.getViews() < value.getViews())
                                .findFirst()
                                .orElse(null);
                    }

                    if (toBeRemoved != null) {
                        categoryTopProducts.getTopProducts().remove(toBeRemoved);
                    }

                    categoryTopProducts.getTopProducts().add(newCategoryTopProduct);

                    return categoryTopProducts;
                }, Materialized.<String, CategoryTopProducts>as(Stores.persistentKeyValueStore("category-top-products"))
                        .withKeySerde(new Serdes.StringSerde())
                        .withValueSerde(new Json<>(CategoryTopProducts.class)))
                .toStream()
                .to("category-top-products", Produced.with(
                        new Serdes.StringSerde(),
                        new Json<>(CategoryTopProducts.class)
                ));


        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "category-top-products");
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "50");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.start();
    }

    public static class CategoryTopProducts {
        private String category;
        private SortedSet<CategoryTopProduct> topProducts;

        public CategoryTopProducts() {
        }

        public CategoryTopProducts(String category, SortedSet<CategoryTopProduct> topProducts) {
            this.category = category;
            this.topProducts = topProducts;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public SortedSet<CategoryTopProduct> getTopProducts() {
            return topProducts;
        }

        public void setTopProducts(SortedSet<CategoryTopProduct> topProducts) {
            this.topProducts = topProducts;
        }
    }

    public static class EnrichedProduct {
        private String id;
        private String name;
        private String category;
        private long views;

        public EnrichedProduct() {
        }

        public EnrichedProduct(String id, String name, String category, long views) {
            this.id = id;
            this.name = name;
            this.category = category;
            this.views = views;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public long getViews() {
            return views;
        }

        public void setViews(long views) {
            this.views = views;
        }
    }

    public static class CategoryTopProduct implements Comparable<CategoryTopProduct> {
        private static final Comparator<CategoryTopProduct> COMPARATOR = Comparator
                .comparingLong(CategoryTopProduct::getViews);

        private String id;
        private String name;
        private long views;

        public CategoryTopProduct() {
        }

        public CategoryTopProduct(String id, String name, long views) {
            this.id = id;
            this.name = name;
            this.views = views;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getViews() {
            return views;
        }

        public void setViews(long views) {
            this.views = views;
        }

        @Override
        public int compareTo(CategoryTopProduct o) {
            return COMPARATOR.compare(this, o);
        }
    }
}
