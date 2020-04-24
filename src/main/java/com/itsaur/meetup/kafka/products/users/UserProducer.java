package com.itsaur.meetup.kafka.products.users;

import com.itsaur.meetup.kafka.util.Json;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.itsaur.meetup.kafka.products.Parameters.NUM_OF_USERS;

public class UserProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "3000");

        KafkaProducer<String, User> producer = new KafkaProducer<>(
                properties,
                new StringSerializer(),
                new Json<>(User.class));

        Iterable<User> users = new RandomUserDataset(NUM_OF_USERS);
        StopWatch watch = StopWatch.createStarted();

        for (User user : users) {
            producer.send(new ProducerRecord<>(
                    "users",
                    user.getId(),
                    user
            ));
        }

        producer.close();
        watch.stop();
        System.out.println("Finished in " + watch.getTime(TimeUnit.SECONDS) + " secs");
    }
}
