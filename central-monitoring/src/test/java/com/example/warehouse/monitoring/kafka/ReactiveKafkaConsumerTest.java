package com.example.warehouse.monitoring.kafka;

import com.example.warehouse.monitoring.config.MonitoringKafkaProperties;
import com.example.warehouse.monitoring.service.MeasurementProcessor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

class ReactiveKafkaConsumerTest {
    @Test
    void stopIsSafeBeforeConsumerStarts() {
        MonitoringKafkaProperties properties = mock(MonitoringKafkaProperties.class);
        MeasurementProcessor processor = mock(MeasurementProcessor.class);
        ReactiveKafkaConsumer consumer = new ReactiveKafkaConsumer(properties, processor);

        assertDoesNotThrow(consumer::stop);
        verifyNoInteractions(processor);
    }
}
