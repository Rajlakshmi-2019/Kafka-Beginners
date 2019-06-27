package com.github.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        Properties p = new Properties();
        String bootstrapserver = "localhost:9092";

        // Create Producer properties
        // p.setProperty("bootstrap.servers",bootstrapserver);    // Old way of setting property

        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        p.setProperty("key.serializer", StringSerializer.class.getName());
        p.setProperty("value.serializer",StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(p);

        for(int i =0;i<=10;i++) {

            String topic = "first_topic_1" ;
            String value = "hello world " +Integer.toString(i);
            String key = "id_"+ Integer.toString(i);

            // Create Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);

            logger.info("Key :"+key);

            //send As-sync
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Executes every time a record was successfully sent or an exception is thrown

                    if (e == null) {
                        logger.info("Received new Meta-Data \n"
                                + "Topic:" + recordMetadata.topic() + "\n"
                                + "Partition :" + recordMetadata.partition() + "\n"
                                + "Offset :" + recordMetadata.offset() + "\n"
                                + "Timestamp : " + recordMetadata.timestamp());
                    } else {

                        logger.error("Error while producing ", e);

                    }
                }
            }).get(); // Block the send do not do in production
        }
        producer.flush();
        producer.close();
    }
}
