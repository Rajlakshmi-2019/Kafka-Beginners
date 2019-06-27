package com.github.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        System.out.println("Welcome !!!");
        Properties p = new Properties();
        String bootstrapserver = "localhost:9092";

        // Create Producer properties
       // p.setProperty("bootstrap.servers",bootstrapserver);    // Old way of setting property

        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        p.setProperty("key.serializer", StringSerializer.class.getName());
        p.setProperty("value.serializer",StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(p);

        // Create Producer Record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","hello world");

        producer.send(record);
        producer.flush();
        producer.close();
    }
}
