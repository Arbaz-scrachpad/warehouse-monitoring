package com.example.warehouse.monitoring.rules.impl;

import com.example.warehouse.contract.MeasurementEvent;
import com.example.warehouse.contract.SensorType;
import com.example.warehouse.monitoring.config.ThresholdProperties;
import com.example.warehouse.monitoring.rules.AlarmRule;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class ThresholdAlarmRule implements AlarmRule {
    private final ThresholdProperties thresholds;

    public ThresholdAlarmRule(ThresholdProperties thresholds) {
        this.thresholds = thresholds;
    }

    @Override
    public Optional<Alarm> evaluate(MeasurementEvent event) {
        double threshold = switch (event.sensorType()) {
            case TEMPERATURE -> thresholds.temperature();
            case HUMIDITY -> thresholds.humidity();
        };

        // Strict greater-than is intentional: equality is not an alarm.
        if (event.value() <= threshold) {
            return Optional.empty();
        }

        String type = event.sensorType() == SensorType.TEMPERATURE
                ? "temperature" : "humidity";
        return Optional.of(new Alarm(
                event.warehouseId(),
                event.sensorId(),
                type,
                event.value(),
                threshold,
                "ALARM: " + type + " threshold exceeded"
        ));
    }
}
