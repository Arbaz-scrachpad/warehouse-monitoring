package com.example.warehouse.monitoring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class CentralMonitoringApplication {
    public static void main(String[] args) {
        SpringApplication.run(CentralMonitoringApplication.class, args);
    }
}
