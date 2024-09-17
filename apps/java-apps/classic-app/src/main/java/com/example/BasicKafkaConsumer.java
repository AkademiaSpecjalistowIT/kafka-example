package com.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class BasicKafkaConsumer {

    public static void main(String[] args) {

        // Create Kafka consumer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");  // Kafka broker address
        props.put("group.id", "my-java-consumer-group");  // Consumer group ID
        props.put("enable.auto.commit", "true");  // Automatically commit offsets
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Set auto_offset_reset to 'latest' so that we consume only new messages
        props.put("auto.offset.reset", "latest");

        // Create the Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList("my-topic"));

        // Continuously poll the topic for new messages
        try {
            while (true) {
                // Poll messages with a timeout of 1 second
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed message: key = %s, value = %s, partition = %d, offset = %d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } finally {
            consumer.close();  // Ensure consumer is closed on exit
        }
    }
}
