package com.example.SCStK;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.Scanner;

@SpringBootApplication
@EnableBinding(Processor.class)
public class SpringCloudStreamKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringCloudStreamKafkaApplication.class, args);
	}

	@Bean
	public ApplicationRunner runner(KafkaTemplate<byte[], byte[]> template) {
		return args -> {
			Scanner scanner = new Scanner(System.in);
			String line = scanner.nextLine();
			while (!line.equals("exit")) {
				template.send("s1p.dest", line.getBytes());
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

	@EnableBinding(Foo.class)
	static class TestSink {

		private final Log logger = LogFactory.getLog(getClass());

		@StreamListener("foo")
		public void receive(String message) {
			logger.info("The UpperCase Message = " + message);
		}
	}

	public interface Foo {
		String FOO = "foo";

		@Input(FOO)
		MessageChannel inuput();
	}
}
