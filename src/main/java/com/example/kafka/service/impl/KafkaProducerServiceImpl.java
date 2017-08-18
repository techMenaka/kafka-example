package com.example.kafka.service.impl;

import com.example.kafka.service.KafkaProducerService;
import org.apache.kafka.clients.producer.*;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by Menaka on 8/18/17.
 */

@Component
public class KafkaProducerServiceImpl implements KafkaProducerService {

    String topic;
    String sync;
    private Properties kafkaProps = new Properties();
    private Producer<String, String> producer;

    public KafkaProducerServiceImpl(String topic) {
        this.topic = topic;
    }

    @Override
    public void configure(String brokerList, String sync) {
        this.sync = sync;
        kafkaProps.put("bootstrap.servers", brokerList);

        // This is mandatory, even though we don't send keys
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("acks", "1");

        // how many times to retry when produce request fails?
        kafkaProps.put("retries", "3");
        kafkaProps.put("linger.ms", 5);
    }

    @Override
    public void start() {
        producer = new KafkaProducer<String, String>(kafkaProps);
    }

    @Override
    public void produce(String value) throws ExecutionException, InterruptedException {
        if (sync.equals("sync"))
            produceSync(value);
        else if (sync.equals("async"))
            produceAsync(value);
        else throw new IllegalArgumentException("Expected sync or async, got " + sync);

    }

    @Override
    public void close() {
        producer.close();
    }

    /* Produce a record and wait for server to reply. Throw an exception if something goes wrong */
    private void produceSync(String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);
        producer.send(record).get();

    }

    /* Produce a record without waiting for server. This includes a callback that will print an error if something goes wrong */
    private void produceAsync(String value) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);
        producer.send(record, new KafkaProducerServiceImpl.KafkaProducerCallback());
    }

    private class KafkaProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error producing to topic " + recordMetadata.topic());
                e.printStackTrace();
            }
        }
    }

}

