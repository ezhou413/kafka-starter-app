package io.confluent.developer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
// import io.confluent.common.utils.testUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class StreamsExample {

    static final String inTopic = "topic_1";
    static final String outTopic = "topic_2";

    public static void main(String[] args) {
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

        // final String defaultKey = "key";
        // // produce words to the input topic
        // String[] words = {"book", "batteries", "and", "could", "potato", "cat", "a", "a", "a"};
        // try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
        //     final Random rnd = new Random();
        //     final int numMessages = 100;
        //     System.out.println("Starting to produce messages...");
        //     for (int i = 0; i < numMessages; i++) {
        //         String word = words[rnd.nextInt(words.length)];

        //         producer.send(
        //                 new ProducerRecord<>(inTopic, defaultKey, word),
        //                 (event, ex) -> {
        //                     if (ex != null)
        //                         ex.printStackTrace();
        //                     else
        //                         System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", inTopic, defaultKey, word);
        //                 });
        //     }
        //     // Flush to ensure all messages are sent before closing
        //     producer.flush();
        //     System.out.printf("%s events were produced to topic %s%n", numMessages, inTopic);
        //     System.out.println("Finished producing messages.");
        // }

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> text = builder.stream(inTopic);

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        final KTable<String, Long> wordCounts = text
        .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
        .groupBy((keyIgnored, word) -> word)
        .count();

        // Write the `KTable<String, Long>` to the output topic.
        wordCounts.toStream().to(outTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
