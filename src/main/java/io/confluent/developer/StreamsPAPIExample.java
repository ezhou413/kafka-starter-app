package io.confluent.developer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class StreamsPAPIExample {
    static final String topic1 = "evan_topic_1";
    static final String topic2 = "evan_topic_2";
    static final String outTopic = "papi_join_topic";
    static final String STORE1_NAME = "topic2-store";
    static final String STORE2_NAME = "topic3-store";

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
            e.printStackTrace();
            System.exit(1);
        }

        final Topology topology = new Topology();

        // Add sources for both topics with different processors to track the source
        topology.addSource("Source1", topic1)
                .addProcessor("Topic1Processor", Topic1Processor::new, "Source1")
                .addSource("Source2", topic2)
                .addProcessor("Topic2Processor", Topic2Processor::new, "Source2")
                .addProcessor("JoinProcessor", JoinProcessor::new, "Topic1Processor", "Topic2Processor")
                .addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(STORE1_NAME), Serdes.String(), Serdes.String()), "JoinProcessor")
                .addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(STORE2_NAME), Serdes.String(), Serdes.String()), "JoinProcessor")
                .addSink("Sink", outTopic, Serdes.String().serializer(), Serdes.String().serializer(), "JoinProcessor");

        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static class Topic1Processor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;

        public void init(ProcessorContext<String, String> context) {
            this.context = context;
        }

        public void process(Record<String, String> record) {
            String markedValue = "TOPIC1:" + record.value();
            context.forward(new Record<>(record.key(), markedValue, record.timestamp()));
        }
    }

    static class Topic2Processor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;

        public void init(ProcessorContext<String, String> context) {
            this.context = context;
        }

        public void process(Record<String, String> record) {
            String markedValue = "TOPIC2:" + record.value();
            context.forward(new Record<>(record.key(), markedValue, record.timestamp()));
        }
    }

    static class JoinProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;
        private KeyValueStore<String, String> store1;
        private KeyValueStore<String, String> store2;

        public void init(ProcessorContext<String, String> context) {
            this.context = context;
            this.store1 = context.getStateStore(STORE1_NAME);
            this.store2 = context.getStateStore(STORE2_NAME);
        }

        public void process(Record<String, String> record) {
            String key = record.key();
            String value = record.value();
            
            System.out.println("Processing record: key=" + key + ", value=" + value);
            
            if (value.startsWith("TOPIC2:")) {
                // from topic_2
                String actualValue = value.substring(7); // remove prefix
                store1.put(key, actualValue);
                System.out.println("stored in store1: " + key + " -> " + actualValue);
                
                // check if record exists intopic_3
                String valueFromTopic3 = store2.get(key);
                if (valueFromTopic3 != null) {
                    String joinedValue = actualValue + "-" + valueFromTopic3;
                    System.out.println("key: " + key + ", joined value: " + joinedValue);
                    context.forward(new Record<>(key, joinedValue, record.timestamp()));
                }
            } else if (value.startsWith("TOPIC3:")) {
                // from topic_3
                String actualValue = value.substring(7); // remove prefix
                store2.put(key, actualValue);
                System.out.println("stored in store2: " + key + " -> " + actualValue);
                
                // check if record exists in topic_2
                String valueFromTopic2 = store1.get(key);
                if (valueFromTopic2 != null) {
                    String joinedValue = valueFromTopic2 + "-" + actualValue;
                    System.out.println("key: " + key + ", joined value: " + joinedValue);
                    context.forward(new Record<>(key, joinedValue, record.timestamp()));
                }
            }
            
            context.commit();
        }
    }
}