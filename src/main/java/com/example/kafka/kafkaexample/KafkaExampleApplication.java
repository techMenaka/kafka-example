package com.example.kafka.kafkaexample;

import com.example.kafka.service.KafkaConsumerService;
import com.example.kafka.service.KafkaProducerService;
import com.example.kafka.service.impl.KafkaConsumerServiceImpl;
import com.example.kafka.service.impl.KafkaProducerServiceImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class KafkaExampleApplication {

	public static void main(String[] args) {

		SpringApplication.run(KafkaExampleApplication.class, args);
		KafkaProducerService service = new KafkaProducerServiceImpl("guestProfile");
		service.configure("localhost:9092", "async");
		service.start();
		KafkaConsumerService consumerService = new KafkaConsumerServiceImpl();
		consumerService.configure("localhost:9092", "profile");
		consumerService.start();
		try {
			service.produce("{\n" +
					"\t\"emailId\" : \"a@c1.com\",\n" +
					"\t\"status\" : \"LoggedIn\",\n" +
					"\t\"id\" : \"12232\",\n" +
					"\t\"client\": \"6\",\n" +
					"\t\"app\": \"ios\",\n" +
					"\t\"loginTs\" : \"12:00:01\"\n" +
					"}");
			consumerService.processMessage("guestProfile");
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
