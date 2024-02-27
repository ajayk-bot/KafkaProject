package com.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);
    String topicName = "first-topic";
    KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> propsmap) {
        this.kafkaProducer = new KafkaProducer<String, String>(propsmap);
    }

    public static Map<String, Object> propsMap() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return propsMap;
    }

    public void publishMessageSync(String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, key, value);
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            //System.out.println("partition : " + recordMetadata.partition());
            //System.out.println("offset : " + recordMetadata.offset());
            logger.info("partition : {}", recordMetadata.partition());
            logger.info("offset : {} ", recordMetadata.offset());

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            logger.error("Exception in publishedMesaageSync {}", e.getMessage());
        }
    }


    public static void main(String[] args) {
        MessageProducer messageProducer = new MessageProducer(propsMap());
        messageProducer.publishMessageSync(null, "ABC");
    }

}
