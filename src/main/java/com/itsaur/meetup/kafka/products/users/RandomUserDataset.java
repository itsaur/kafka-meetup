package com.itsaur.meetup.kafka.products.users;

import com.github.javafaker.Faker;
import com.itsaur.meetup.kafka.products.products.Product;

import java.util.Iterator;
import java.util.UUID;

public class RandomUserDataset implements Iterable<User> {
    private final int total;
    private final Faker faker = new Faker();

    public RandomUserDataset(int total) {
        this.total = total;
    }

    @Override
    public Iterator<User> iterator() {
        return new UserIterator();
    }

    private class UserIterator implements Iterator<User> {
        private int current = 0;

        @Override
        public boolean hasNext() {
            return current < total;
        }

        @Override
        public User next() {
            current++;
            return new User(
                    UUID.randomUUID().toString(),
                    faker.name().username(),
                    faker.address().countryCode(),
                    faker.address().city(),
                    faker.number().numberBetween(14, 60)
            );
        }
    }
}
