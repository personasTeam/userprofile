package com.viewstar;

import com.viewstar.controller.MultDataContorller;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication(scanBasePackages = {"com.viewstar", "com.bigdata.springcloud.hystrix"})
@EnableDiscoveryClient
@EnableFeignClients(basePackages = "com.bigdata.springcloud.openfeignclient")
public class UserprofileApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext run = SpringApplication.run(UserprofileApplication.class, args);
	}

}
