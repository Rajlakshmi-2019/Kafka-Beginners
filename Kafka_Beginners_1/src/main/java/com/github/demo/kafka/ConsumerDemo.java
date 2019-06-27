package com.github.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {

        String bootstrapserver = "localhost:9092";
        String group_id = "my-second-application";
        String topic = "first_topic_1";
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        Properties p = new Properties();

        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        p.setProperty("key.serializer",
                StringSerializer.class.getName());
        p.setProperty("key.deserializer",
                StringSerializer.class.getName());
        p.setProperty("value.serializer",
                StringSerializer.class.getName());

        p.setProperty("value.deserializer",StringSerializer.class.getName());
                //"org.apache.kafka.common.serialization.StringSerializer");
        //p.setProperty("key.serializer", StringSerializer.class.getName());
        //p.setProperty("value.serializer",StringSerializer.class.getName());
       // p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
       // p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Thread.currentThread().setContextClassLoader(null);


        Thread currentThread = Thread.currentThread();
        ClassLoader savedClassLoader = currentThread.getContextClassLoader();

        currentThread.setContextClassLoader(null);
         KafkaConsumer<String,String> consumer;
        consumer = new KafkaConsumer<String, String>(p);

        //KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        currentThread.setContextClassLoader(savedClassLoader);

        // Create Consumer
       // KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(p);
        // Subscribe Consumer to our Topics
       // consumer.subscribe(Collections.singleton(topic));
        consumer.subscribe(Arrays.asList(topic));
        //Poll for new data
        while(true)
        {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String,String> record : records) {
                logger.info("Key:"+record.key()+"Value:"+record.value()+"Partition :"+record.partition()+"Offset :"+record.offset());
                
            }
        }

    }
}
