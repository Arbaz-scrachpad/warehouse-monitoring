package com.example.warehouse.monitoring.service;

import com.example.warehouse.contract.MeasurementEvent;
import com.example.warehouse.contract.SensorType;
import com.example.warehouse.monitoring.rules.AlarmRule;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.Mockito.*;

class MeasurementProcessorTest {
    private AlarmRule alarmRule;
    private MeasurementProcessor processor;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().findAndRegisterModules();
        alarmRule = mock(AlarmRule.class);
        processor = new MeasurementProcessor(objectMapper, alarmRule);
        when(alarmRule.evaluate(any())).thenReturn(Optional.empty());
    }

    @Test
    void validEventIsPassedToAlarmRule() throws Exception {
        MeasurementEvent event = new MeasurementEvent(UUID.randomUUID(),
                MeasurementEvent.CURRENT_SCHEMA_VERSION, "warehouse-1", "t1",
                SensorType.TEMPERATURE, 30.0, Instant.parse("2026-01-01T00:00:00Z"));

        processor.process(objectMapper.writeValueAsString(event));

        verify(alarmRule).evaluate(event);
    }

    @Test
    void invalidJsonIsIgnored() {
        processor.process("not-json");

        verifyNoInteractions(alarmRule);
    }

    @Test
    void unsupportedSchemaVersionIsIgnored() throws Exception {
        MeasurementEvent event = new MeasurementEvent(UUID.randomUUID(), 999,
                "warehouse-1", "t1", SensorType.TEMPERATURE, 30.0,
                Instant.parse("2026-01-01T00:00:00Z"));

        processor.process(objectMapper.writeValueAsString(event));

        verifyNoInteractions(alarmRule);
    }
}
