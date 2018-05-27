package com.example.SK;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import java.util.Scanner;

@SpringBootApplication
public class SpringKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaApplication.class, args);
	}

	@Bean
	public NewTopic s1p() {
		return new NewTopic("s1p", 1, (short) 1);
	}

	@Bean
	public ApplicationRunner runner(KafkaTemplate<String, String> template) {
		return args -> {
			Scanner scanner = new Scanner(System.in);
			String line = scanner.nextLine();
			while (!line.equals("exit")) {
				template.send("s1p", line);
				line = scanner.nextLine();
			}
			scanner.close();
		};
	}

	@KafkaListener(topics = "s1p", groupId = "s1p")
	public void listen(String in,
			Consumer<?, ?> consumer,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		System.out.println("Received [" + in + "] from  the partition = " + partition);
	}
}
