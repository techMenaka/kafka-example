package com.example.kafka.service;

import java.util.concurrent.ExecutionException;

/**
 * Created by techMenaka on 8/18/17.
 */
public interface KafkaProducerService {
    void configure(String brokerList, String sync);

    void start();

    void produce(String value) throws ExecutionException, InterruptedException;

    void close();
}
