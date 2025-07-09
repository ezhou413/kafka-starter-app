package io.confluent.developer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class StreamsPAPIExample {
    static final String inTopic = "topic_1";
    static final String outTopic = "topic_2";
    static final String STORE_NAME = "word-counts-store";

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
            e.printStackTrace();
            System.exit(1);
        }

        final Topology topology = new Topology();

        topology.addSource("Source", inTopic)
                .addProcessor("WordCountProcessor", WordCountProcessor::new, "Source")
                .addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(STORE_NAME), Serdes.String(),Serdes.Long()), "WordCountProcessor")
                .addSink("Sink", outTopic, Serdes.String().serializer(), Serdes.Long().serializer(), "WordCountProcessor");

        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static class WordCountProcessor implements Processor<String, String, String, Long> {
        private ProcessorContext<String, Long> context;
        private KeyValueStore<String, Long> kvStore;
        private final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        public void init(ProcessorContext<String, Long> context) {
            this.context = context;
            this.kvStore = context.getStateStore(STORE_NAME);
        }

        public void process(Record<String, String> record) {
            String value = record.value();
            if (value == null) return;
            Arrays.stream(pattern.split(value.toLowerCase()))
                .filter(word -> !word.isEmpty())
                .forEach(word -> {
                    Long count = kvStore.get(word);
                    if (count == null) count = 0L;
                    count += 1;
                    kvStore.put(word, count);
                    context.forward(new Record<>(word, count, record.timestamp()));
                });
        }
    }
}