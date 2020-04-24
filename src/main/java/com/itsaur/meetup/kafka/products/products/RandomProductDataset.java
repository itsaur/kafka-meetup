package com.itsaur.meetup.kafka.products.products;

import com.github.javafaker.Faker;

import java.util.Iterator;
import java.util.UUID;

public class RandomProductDataset implements Iterable<Product> {
    private final int total;
    private final Faker faker = new Faker();

    public RandomProductDataset(int total) {
        this.total = total;
    }

    @Override
    public Iterator<Product> iterator() {
        return new ProductIterator();
    }

    private class ProductIterator implements Iterator<Product> {
        private int current = 0;

        @Override
        public boolean hasNext() {
            return current < total;
        }

        @Override
        public Product next() {
            current++;
            return new Product(
                    UUID.randomUUID().toString(),
                    faker.commerce().productName(),
                    faker.commerce().department()
            );
        }
    }
}
