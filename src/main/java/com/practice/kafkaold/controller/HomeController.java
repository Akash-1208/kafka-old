package com.practice.kafkaold.controller;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.practice.kafkaold.kafka.KafkaProducer;

@RestController
@RequestMapping("/api/kafka")
public class HomeController {
    
    // private KafkaProducer kafkaProducer;
    @Autowired
    private AdminClient adminClient;

    public String bootStrapAddress="localhost:9092";
    private  KafkaConsumer<String, String> consumer;
    private static final Logger LOGGER = LoggerFactory.getLogger(HomeController.class);

    public HomeController() {
        adminClient = getAdminClient(bootStrapAddress);
        consumer = getKafkaConsumer(bootStrapAddress);
    }

    @Autowired
    KafkaProducer kafkaProducer;

    @GetMapping("/publish")
    public ResponseEntity<String> publish(@RequestParam("message") String message) throws InterruptedException, ExecutionException{
        kafkaProducer.sendMessage(message);
        analyzeLag("test");
        return ResponseEntity.ok("Message Sent");
    }
    

    public Map<TopicPartition, Long> getConsumerGrpOffsets(String groupId)
    throws ExecutionException, InterruptedException {
      ListConsumerGroupOffsetsResult info = adminClient.listConsumerGroupOffsets(groupId);
      Map<TopicPartition, OffsetAndMetadata> metadataMap = info.partitionsToOffsetAndMetadata().get();
      Map<TopicPartition, Long> groupOffset = new HashMap<>();          //Map<map<topic,partiion>, long>
      for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : metadataMap.entrySet()) {
          TopicPartition key = entry.getKey();
          OffsetAndMetadata metadata = entry.getValue();
          groupOffset.putIfAbsent(new TopicPartition(key.topic(), key.partition()), metadata.offset());
          //metadata.offset is the value that a partition and topic are currently consuming by the consumer
      }
      return groupOffset;
  }

  private Map<TopicPartition, Long> getProducerOffsets(Map<TopicPartition, Long> consumerGrpOffset) {
        List<TopicPartition> topicPartitions = new LinkedList<>();
        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffset.entrySet()) {
            TopicPartition key = entry.getKey();
            topicPartitions.add(new TopicPartition(key.topic(), key.partition()));
        }
        return consumer.endOffsets(topicPartitions); //endoffsets is the last value of the consumer fetched at last state
    }

    public Map<TopicPartition, Long> computeLags(
      Map<TopicPartition, Long> consumerGrpOffsets,
      Map<TopicPartition, Long> producerOffsets) {
      Map<TopicPartition, Long> lags = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffsets.entrySet()) {
            Long producerOffset = producerOffsets.get(entry.getKey());
            Long consumerOffset = consumerGrpOffsets.get(entry.getKey());
            long lag = Math.abs(Math.max(0, producerOffset) - Math.max(0, consumerOffset));
            System.out.println("The Difference is "+ String.valueOf(lag));
            lags.putIfAbsent(entry.getKey(), lag);
        }
        return lags;
    }

    public Map<TopicPartition, Long> analyzeLag(String groupId) 
    throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> consumerGrpOffsets = getConsumerGrpOffsets(groupId);
        Map<TopicPartition, Long> producerOffsets = getProducerOffsets(consumerGrpOffsets);
        Map<TopicPartition, Long> lags = computeLags(consumerGrpOffsets, producerOffsets);
        for (Map.Entry<TopicPartition, Long> lagEntry : lags.entrySet()) {
            String topic = lagEntry.getKey().topic();
            int partition = lagEntry.getKey().partition();
            Long lag = lagEntry.getValue();
            LOGGER.info(" Lag for topic = {}, partition = {}, groupId = {} is {}",topic,partition,groupId,lag);
        }
        return lags;
    }
    private AdminClient getAdminClient(String bootstrapServerConfig) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServerConfig);
        return AdminClient.create(config);
    }

    private KafkaConsumer<String, String> getKafkaConsumer(String bootStrapAddress) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapAddress);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }
}
