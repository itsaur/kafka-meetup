package com.itsaur.meetup.kafka.books;

import com.itsaur.meetup.kafka.util.Json;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

public class WordCountBooksStreams {

    public static void main(String[] args) throws InterruptedException {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> booksStream = builder.stream("books", Consumed.with(
                new Serdes.StringSerde(), //book title
                new Serdes.StringSerde()) //book contents
        );

        KStream<String, String> bookWordsStream = builder.stream("book-words", Consumed.with(
                new Serdes.StringSerde(), //word
                new Serdes.StringSerde()) //book title
        );

        booksStream
                .flatMap((key, value) -> Arrays.stream(value.split("\\W+"))
                        .map(word -> KeyValue.pair(word, key))
                        .collect(Collectors.toList()))
                .to("book-words", Produced.with(new Serdes.StringSerde(), new Serdes.StringSerde()));

        bookWordsStream
                .groupByKey(Grouped.with(
                        new Serdes.StringSerde(),
                        new Serdes.StringSerde()))
                .count(Materialized.
                        <String, Long>as(Stores.persistentKeyValueStore("word-count"))
                        .withKeySerde(new Serdes.StringSerde())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .map((key, value) -> KeyValue.pair(key, new WordCount(key, value)))
                .to("word-count", Produced.with(
                        new Serdes.StringSerde(),
                        new Json<>(WordCount.class)
                ));

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "50");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
    }

    public static class WordCount {
        private String word;
        private Long count;

        public WordCount() {
        }

        public WordCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }
    }
}
