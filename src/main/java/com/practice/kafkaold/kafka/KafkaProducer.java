package com.practice.kafkaold.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {
    

    private KafkaTemplate<String,String> kafkaTemplate;
    

    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    public void sendMessage(String message){
        kafkaTemplate.send("test123",message);
        System.out.println("Hello I sent Message");
    }
}
