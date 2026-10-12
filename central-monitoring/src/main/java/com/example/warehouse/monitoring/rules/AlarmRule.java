package com.example.warehouse.monitoring.rules;

import java.util.Optional;
import com.example.warehouse.contract.MeasurementEvent;

public interface AlarmRule {
    Optional<Alarm> evaluate(MeasurementEvent event);

    record Alarm(String warehouseId, String sensorId, String sensorType,
                 double observedValue, double threshold, String message) {}
}
