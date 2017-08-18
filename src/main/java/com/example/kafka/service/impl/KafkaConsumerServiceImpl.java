package com.example.kafka.service.impl;

import com.example.kafka.service.KafkaConsumerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by Menaka on 8/18/17.
 */
public class KafkaConsumerServiceImpl implements KafkaConsumerService {
    private Properties kafkaProps = new Properties();
    private KafkaConsumer<String, String> consumer;

    @Override
    public void configure(String brokerList, String groupId) {
            kafkaProps.put("group.id",groupId);
            kafkaProps.put("bootstrap.servers",brokerList);
            kafkaProps.put("auto.offset.reset","earliest");         // when in doubt, read everything
            kafkaProps.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
            kafkaProps.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

    }

    @Override
    public void start() {
        consumer = new KafkaConsumer<String, String>(kafkaProps);
    }

    @Override
    public void subscribe(String topic) throws ExecutionException, InterruptedException {
        consumer.subscribe(Collections.singletonList(topic));
    }


    @Override
    public void close() {

        final Thread mainThread = Thread.currentThread();

        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Starting exit...");
                // Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        consumer.close();
    }

    @Override
    public void processMessage(String topic) {
        try {
            subscribe(topic);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                System.out.println(System.currentTimeMillis() + "  --  waiting for data...");
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                    System.err.println(record.value() + "\n\n");

                }
                for (TopicPartition tp: consumer.assignment())
                    System.out.println("Committing offset at position:" + consumer.position(tp));
                consumer.commitSync();
            }

        } catch (ExecutionException e) {


        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
