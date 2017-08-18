package com.example.kafka.service;

import java.util.concurrent.ExecutionException;

/**
 * Created by techMenaka on 8/18/17.
 */
public interface KafkaConsumerService {

    void configure(String brokerList, String sync);

    void start();

    void subscribe(String topic) throws ExecutionException, InterruptedException;

    void close();

    void processMessage(String topic);
}
