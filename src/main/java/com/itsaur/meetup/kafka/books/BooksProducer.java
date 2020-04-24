package com.itsaur.meetup.kafka.books;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class BooksProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "3000");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());

        Iterable<Book> books = new RandomBooksDataset(1_000);
        StopWatch watch = StopWatch.createStarted();

        for (Book book: books) {
            producer.send(new ProducerRecord<>("books",
                    book.getTitle(),
                    book.getContents()));
        }

        producer.close();
        watch.stop();
        System.out.println("Finished in " + watch.getTime(TimeUnit.SECONDS) + " secs");
    }
}
