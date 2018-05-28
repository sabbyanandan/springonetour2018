package com.example.SCStKSink;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;

@SpringBootApplication
@EnableBinding(Sink.class)
public class SpringCloudStreamSinkKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringCloudStreamSinkKafkaApplication.class, args);
	}

	@StreamListener(Sink.INPUT)
	public void receive(String message) {
		System.out.println("The UpperCase Message = " + message);
	}
}
