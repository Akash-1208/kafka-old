package com.practice.kafkaold.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {
    

    @KafkaListener(topics="test123",groupId="test")
    public void consume(String message){
        System.out.println("Message Recived  "+message);
    }
}
