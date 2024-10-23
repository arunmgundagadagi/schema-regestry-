package org.example;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Main {
    public static void main(String[] args) {
        // Set up properties for the Kafka consumer
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092"); // Kafka broker address
        props.setProperty("group.id", "my_consumer_group"); // Consumer group ID
        props.setProperty("enable.auto.commit", "true"); // Auto-commit offsets
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "earliest"); // Start from the beginning of the topic

        // Create the Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList("test_count"));

        // Poll for new messages in a loop
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); // Poll for new messages
            for (ConsumerRecord<String, String> record : records) {
                // Print the received message
                System.out.printf("Received message: key=%s, value=%s%n", record.key(), record.value());
            }
        }
    }
}
