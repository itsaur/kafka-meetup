package com.itsaur.meetup.kafka.sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleConsumer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "A");
        //properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        //properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(Collections.singleton("topic"));
        //consumer.assign(Collections.singleton(new TopicPartition("events", 1)));
        //consumer.seek(new TopicPartition("events", 1), 120);

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            consumerRecords.forEach(consumerRecord -> {
                System.out.println(consumerRecord.timestamp());
                System.out.println(consumerRecord.partition());
                System.out.println(consumerRecord.topic());
                System.out.println(consumerRecord.key());
                System.out.println(consumerRecord.value());
            });
            consumer.commitSync();
        }
    }
}
