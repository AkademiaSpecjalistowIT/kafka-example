package com.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    @KafkaListener(topics = "my-topic", groupId = "my-group")
    public void consume(ConsumerRecord<String, String> record) {
        System.out.println("Consumed message: " + record.value());
        // Add your custom logic here (e.g., saving to a database or processing the message)
    }
}
