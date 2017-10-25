package com.siemens.bt.ho.aws.kinesis.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.siemens.bt.ho")
public class KinesisConsumerApp {

	public static void main(String[] args) {
		SpringApplication.run(KinesisConsumerApp.class, args);
	}
}
