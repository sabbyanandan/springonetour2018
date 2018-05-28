package com.example.SCStKProcessor;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.Scanner;

@SpringBootApplication
@EnableBinding(Processor.class)
public class SpringCloudStreamProcessorKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringCloudStreamProcessorKafkaApplication.class, args);
	}

	@Bean
	public ApplicationRunner runner(KafkaTemplate<byte[], byte[]> template) {
		return args -> {
			Scanner scanner = new Scanner(System.in);
			String line = scanner.nextLine();
			while (!line.equals("exit")) {
				template.send("s1p.dest.in", line.getBytes());
				line = scanner.nextLine();
			}
			scanner.close();
		};
	}

	@StreamListener(Processor.INPUT)
	@SendTo(Processor.OUTPUT)
	public String process(String in) {
		System.out.println("Received Message = " + in);
		if ("fail".equals(in)) {
			throw new RuntimeException("A FAILED MESSAGE!");
		}
		return in.toUpperCase();
	}
}
