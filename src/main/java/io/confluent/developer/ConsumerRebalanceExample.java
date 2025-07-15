package io.confluent.developer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ConsumerRebalanceExample {

    public static void main(String[] args) {
        System.out.println("I am a Kafka Consumer with a Rebalance Listener!");

        String topic = "evan_rebalance_topic";

        // Create Consumer configs
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

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

        // Create a consumer
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // Add a shutdown hook to gracefully close the consumer
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                // wakeup() is a special method to exit the poll loop
                consumer.wakeup();

                // Join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


        try {
            // Create a ConsumerRebalanceListener
            // This listener allows us to react to partitions being assigned or revoked.
            ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // This method is called before a rebalance starts and after the consumer has stopped fetching data.
                    // This is where you would typically commit offsets for the partitions that are about to be taken away.
                    System.out.println("Partitions are being revoked: " + partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // This method is called after the rebalance has completed and the consumer
                    // has been assigned its new set of partitions.
                    System.out.println("Partitions have been assigned: " + partitions);
                }
            };

            // Subscribe consumer to our topic(s) and attach the listener
            consumer.subscribe(Collections.singletonList(topic), listener);

            // Poll for new data in a loop
            while (true) {
                // The poll method is where the consumer fetches records from Kafka.
                // It's also the point where rebalancing is coordinated. When a rebalance is triggered,
                // the `onPartitionsRevoked` and `onPartitionsAssigned` methods will be called
                // from within the poll() call.
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Key: " + record.key() + ", Value: " + record.value());
                    System.out.println("Partition: " + record.partition() + ", Offset:" + record.offset());
                }
            }
        } catch (Exception e) {
            // The consumer.wakeup() call in the shutdown hook will throw a WakeupException.
            // This is the expected way to exit the infinite loop.
            System.out.printf("Woke up from consumer.poll() %s%n", e);
        } finally {
            // Close the consumer. This will also commit offsets if auto-commit is enabled.
            System.out.println("Closing the consumer.");
            consumer.close();
            System.out.println("Consumer is now gracefully closed.");
        }
    }
}
