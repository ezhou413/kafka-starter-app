package io.confluent.developer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;


public class ProducerExample {

    public static void main(final String[] args) throws InterruptedException{
        final Properties props = new Properties();

        if (args.length < 1) {
            System.err.println("Please provide the path to the config.properties file as the first argument.");
            System.exit(1);
        }
        String configFile = args[0];

        try (FileInputStream in = new FileInputStream(configFile)) {
            props.load(in);
        } catch (IOException e) {
            // Handle exception
            e.printStackTrace();
            System.exit(1);
        }

        final String topic = "evan_rebalance_topic";

        String[] users = {"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther", "evan"};
        String[] words = {"book", "batteries", "and", "could", "potato", "cat", "a", "a", "a"};
        try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
            final Random rnd = new Random();
            // final int numMessages = 470;
            System.out.println("Starting to produce messages...");
            int i = 0;
            while (true) {
                String user = users[rnd.nextInt(users.length)];
                String word = words[rnd.nextInt(words.length)];

                producer.send(
                        new ProducerRecord<>(topic, word, user),
                        (event, ex) -> {
                            if (ex != null)
                                ex.printStackTrace();
                            else
                                System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, word, user);
                        });
                i++;

                Thread.sleep(200);
            }
            // // Flush to ensure all messages are sent before closing
            // producer.flush();
            // System.out.printf("%s events were produced to topic %s%n", numMessages, topic);
            // System.out.println("Finished producing messages.");
        }
    }
}
