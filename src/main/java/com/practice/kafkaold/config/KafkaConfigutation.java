package com.practice.kafkaold.config;


import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfigutation {
    
    
    private String bootStrapAddress="localhost:9092";

    @Bean
    public AdminClient getAdminClient(){
        Properties configs=new Properties();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapAddress);
        return AdminClient.create(configs);
    }

    @Bean
    public NewTopic topic(){
        return new NewTopic("test123", 1,(short)1);
    }


}
