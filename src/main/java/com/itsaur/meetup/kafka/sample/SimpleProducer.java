package com.itsaur.meetup.kafka.sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

public class SimpleProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties, new StringSerializer(), new StringSerializer());
        //producer.send(new ProducerRecord<>("events","key1", "value1"));
        producer.send(new ProducerRecord<>("events", 1, new Date().getTime(), "key1", "value1"));
    }
}
