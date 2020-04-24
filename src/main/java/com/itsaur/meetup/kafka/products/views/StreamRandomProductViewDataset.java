package com.itsaur.meetup.kafka.products.views;

import com.github.javafaker.Faker;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StreamRandomProductViewDataset implements Iterable<ProductView> {
    private final int total;
    private final Faker faker = new Faker();
    private final CountDownLatch ready = new CountDownLatch(3);
    private final List<String> products = Collections.synchronizedList(new ArrayList<>());
    private final List<String> users = Collections.synchronizedList(new ArrayList<>());

    public StreamRandomProductViewDataset(int total, int expectedUsers, int expectedProducts) {
        this.total = total;

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream("products", Consumed.with(
                new Serdes.StringSerde(),
                new Serdes.StringSerde()
        )).foreach((key, value) -> {
            products.add(key);
            if (products.size() >= expectedProducts) {
                ready.countDown();
            }
        });

        builder.stream("users", Consumed.with(
                new Serdes.StringSerde(),
                new Serdes.StringSerde()
        )).foreach((key, value) -> {
            users.add(key);
            if (users.size() >= expectedUsers) {
                ready.countDown();
            }
        });

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                ready.countDown();
            }
        });

        streams.start();
        try {
            ready.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        streams.close();
        streams.cleanUp();
    }

    @Override
    public Iterator<ProductView> iterator() {
        try {
            ready.await();
            return new ProductViewIterator();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private class ProductViewIterator implements Iterator<ProductView> {
        private int current = 0;
        private OffsetDateTime lastDate = OffsetDateTime.now(ZoneOffset.UTC);

        @Override
        public boolean hasNext() {
            return current < total;
        }

        @Override
        public ProductView next() {
            current++;
            lastDate = lastDate.plus(faker.number().numberBetween(1, 5000), ChronoUnit.MILLIS);

            return new ProductView(
                    products.get(faker.number().numberBetween(0, products.size())),
                    users.get(faker.number().numberBetween(0, users.size())),
                    lastDate
            );
        }
    }
}
