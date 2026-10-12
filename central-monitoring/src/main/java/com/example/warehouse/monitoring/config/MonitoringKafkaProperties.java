package com.example.warehouse.monitoring.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "monitoring.kafka")
public record MonitoringKafkaProperties(
        String bootstrapServers,
        String topic,
        String groupId,
        int concurrency
) {}
