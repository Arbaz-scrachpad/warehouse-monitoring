package com.example.warehouse.agent;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public record AgentConfig(
        String warehouseId,
        int temperaturePort,
        int humidityPort,
        int maxPacketBytes,
        int queueCapacity,
        String bootstrapServers,
        String topic,
        int publisherWorkers,
        int batchSize
) {
    public static AgentConfig load() throws IOException {
        Properties properties = new Properties();
        String external = System.getProperty("agent.config");

        if (external != null && !external.isBlank()) {
            try (InputStream in = Files.newInputStream(Path.of(external))) {
                properties.load(in);
            }
        } else {
            try (InputStream in = AgentConfig.class.getClassLoader()
                    .getResourceAsStream("agent.properties")) {
                if (in == null) {
                    throw new IOException("agent.properties is missing");
                }
                properties.load(in);
            }
        }

        AgentConfig config = new AgentConfig(
                required(properties, "warehouse.id"),
                integer(properties, "udp.temperature-port", 3344),
                integer(properties, "udp.humidity-port", 3355),
                integer(properties, "udp.max-packet-bytes", 1024),
                integer(properties, "queue.capacity", 50000),
                required(properties, "kafka.bootstrap-servers"),
                properties.getProperty("kafka.topic", "sensor-measurements").trim(),
                integer(properties, "kafka.publisher-workers", 2),
                integer(properties, "kafka.batch-size", 200)
        );

        validatePort(config.temperaturePort());
        validatePort(config.humidityPort());
        if (config.temperaturePort() == config.humidityPort()) {
            throw new IllegalArgumentException("UDP ports must be different");
        }
        if (config.maxPacketBytes() <= 0 || config.maxPacketBytes() > 65507
                || config.queueCapacity() <= 0
                || config.publisherWorkers() <= 0 || config.batchSize() <= 0) {
            throw new IllegalArgumentException("Packet size, queue capacity, worker count and batch size must be positive and valid");
        }
        if (config.topic().isBlank()) {
            throw new IllegalArgumentException("kafka.topic cannot be blank");
        }
        return config;
    }

    private static String required(Properties p, String key) {
        String value = p.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing property: " + key);
        }
        return value.trim();
    }

    private static int integer(Properties p, String key, int fallback) {
        try {
            return Integer.parseInt(p.getProperty(key, Integer.toString(fallback)).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer property: " + key, e);
        }
    }

    private static void validatePort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("UDP port must be between 1 and 65535");
        }
    }
}
