package com.example.warehouse.monitoring.rules.impl;

import com.example.warehouse.contract.MeasurementEvent;
import com.example.warehouse.contract.SensorType;
import com.example.warehouse.monitoring.config.ThresholdProperties;
import com.example.warehouse.monitoring.rules.AlarmRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ThresholdAlarmRuleTest {
    private ThresholdProperties thresholds;
    private ThresholdAlarmRule rule;

    @BeforeEach
    void setUp() {
        thresholds = mock(ThresholdProperties.class);
        when(thresholds.temperature()).thenReturn(35.0);
        when(thresholds.humidity()).thenReturn(50.0);
        rule = new ThresholdAlarmRule(thresholds);
    }

    @Test
    void temperatureAboveThresholdRaisesAlarm() {
        var result = rule.evaluate(event(SensorType.TEMPERATURE, 36.0));

        assertTrue(result.isPresent());
        AlarmRule.Alarm alarm = result.orElseThrow();
        assertEquals("warehouse-1", alarm.warehouseId());
        assertEquals("t1", alarm.sensorId());
        assertEquals("temperature", alarm.sensorType());
        assertEquals(36.0, alarm.observedValue());
        assertEquals(35.0, alarm.threshold());
    }

    @Test
    void temperatureAtThresholdDoesNotRaiseAlarm() {
        assertTrue(rule.evaluate(event(SensorType.TEMPERATURE, 35.0)).isEmpty());
    }

    @Test
    void humidityAboveThresholdRaisesAlarm() {
        var result = rule.evaluate(event(SensorType.HUMIDITY, 51.0));

        assertTrue(result.isPresent());
        assertEquals("humidity", result.orElseThrow().sensorType());
        assertEquals(50.0, result.orElseThrow().threshold());
    }

    @Test
    void humidityAtThresholdDoesNotRaiseAlarm() {
        assertTrue(rule.evaluate(event(SensorType.HUMIDITY, 50.0)).isEmpty());
    }

    private MeasurementEvent event(SensorType type, double value) {
        return new MeasurementEvent(UUID.randomUUID(), MeasurementEvent.CURRENT_SCHEMA_VERSION,
                "warehouse-1", "t1", type, value, Instant.now());
    }
}
