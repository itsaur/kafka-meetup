package com.itsaur.meetup.kafka.books;

import com.github.javafaker.Faker;

import java.util.Iterator;

public class RandomBooksDataset implements Iterable<Book> {
    private final int total;
    private final Faker faker = new Faker();

    public RandomBooksDataset(int total) {
        this.total = total;
    }

    @Override
    public Iterator<Book> iterator() {
        return new BookIterator();
    }

    private class BookIterator implements Iterator<Book> {
        private int current = 0;

        @Override
        public boolean hasNext() {
            return current < total;
        }

        @Override
        public Book next() {
            current++;
            return new Book(
                    faker.book().title(),
                    faker.lorem().sentence(faker.number().numberBetween(1000, 10000))
            );
        }
    }
}
