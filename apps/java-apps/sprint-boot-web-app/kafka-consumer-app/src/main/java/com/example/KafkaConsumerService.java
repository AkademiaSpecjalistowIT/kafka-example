package com.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    // Injecting KafkaTemplate to send messages
    // @Autowired
    // public KafkaConsumerService(KafkaTemplate<String, String> kafkaTemplate) {
    //     this.kafkaTemplate = kafkaTemplate;
    // }

    @KafkaListener(topics = "my-topic", groupId = "my-group")
    public void consume(ConsumerRecord<String, String> record) {
        String consumedMessage = record.value();
        System.out.println("Consumed message: " + consumedMessage);

        String replyMessage = "Processed: " + consumedMessage;

        kafkaTemplate.send("test", replyMessage);
        System.out.println("Sent reply to topic 'my-reply-topic': " + replyMessage);
    }
}