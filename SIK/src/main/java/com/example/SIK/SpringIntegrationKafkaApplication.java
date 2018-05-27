package com.example.SIK;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.messaging.Message;

import java.util.Scanner;

@SpringBootApplication
public class SpringIntegrationKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringIntegrationKafkaApplication.class, args);
	}

	@Bean
	public NewTopic s1pInt() {
		return new NewTopic("s1pIntInput", 10, (short) 1);
	}

	@Bean
	public NewTopic s1pIntOut() {
		return new NewTopic("s1pIntOutput", 10, (short) 1);
	}

	@Bean
	public ApplicationRunner runner(KafkaTemplate<String, String> template) {
		return args -> {
			Scanner scanner = new Scanner(System.in);
			String line = scanner.nextLine();
			while (!line.equals("exit")) {
				template.send("s1pIntInput", line);
				line = scanner.nextLine();
			}
			scanner.close();
		};
	}

	@Bean
	public IntegrationFlow flow(ConsumerFactory<String, String> consumerFactory,
			KafkaTemplate<Object, Object> template) {
		return IntegrationFlows.from(Kafka.messageDrivenChannelAdapter(consumerFactory, containerProps()))
				.filter(p -> !p.equals("ignore"))
				.enrich(h -> h
						.headerExpression("originalPayload", "payload")
						.header("foo", "bar"))
				.<String, String>transform(String::toUpperCase)
				.<String, String>transform(s -> s + " -- " + s)
				.handle(Kafka.outboundChannelAdapter(template)
						.topic("s1pIntOutput")
						.headerMapper(new DefaultKafkaHeaderMapper()))
				.get();
	}

	public ContainerProperties containerProps() {
		ContainerProperties containerProperties = new ContainerProperties("s1pIntInput");
		containerProperties.setGroupId("s1pIntInput");
		return containerProperties;
	}

	@KafkaListener(topics = "s1pIntOutput", groupId = "s1pIntInput")
	public void listen(Message<String> in) {
		System.out.println("Received Message = " + in);
	}
}
