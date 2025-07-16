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

public class OAuthProducerExample {

    public static void main(final String[] args) throws InterruptedException {
        final Properties props = new Properties();

        if (args.length < 1) {
            System.err.println("Please provide the path to the oauth-config.properties file as the first argument.");
            System.exit(1);
        }
        String configFile = args[0];

        try (FileInputStream in = new FileInputStream(configFile)) {
            props.load(in);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Set up OAuth authentication
        setupOAuthAuthentication(props);

        final String topic = "evan_oauth_topic";

        String[] users = {"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther", "evan"};
        String[] words = {"book", "batteries", "and", "could", "potato", "cat", "a", "a", "a"};
        
        try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
            final Random rnd = new Random();
            System.out.println("Starting to produce messages with OAuth authentication...");
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

                Thread.sleep(100);
            }
        }
    }

    private static void setupOAuthAuthentication(Properties props) {
        // OAuth configuration
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "OAUTHBEARER");

        // Set up JAAS configuration with standard login module
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        props.put("sasl.login.callback.handler.class", "io.confluent.developer.CustomOAuthBearerLoginCallbackHandler");

        // Producer specific configurations
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }
} 