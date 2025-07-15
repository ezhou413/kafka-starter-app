package io.confluent.developer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class StreamsExample {

    static final String inTopic1 = "topic_2";
    static final String inTopic2 = "topic_3";
    static final String outTopic = "joined_topic";

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

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

        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> table1 = builder.table(inTopic1);
        final KTable<String, String> table2 = builder.table(inTopic2);

        // Perform an inner join on the keys
        KTable<String, String> joined = table1.join(
            table2,
            (value1, value2) -> value1 + "-" + value2
        );

        // Add logging to debug output
        joined.toStream().peek((key, value) -> System.out.println("Joined: " + key + " -> " + value))
            .to(outTopic, Produced.with(Serdes.String(), Serdes.String()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
