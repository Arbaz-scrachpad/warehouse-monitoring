package com.example.warehouse.contract;

import java.time.Instant;
import java.util.UUID;

public record MeasurementEvent(
        UUID eventId,
        int schemaVersion,
        String warehouseId,
        String sensorId,
        SensorType sensorType,
        double value,
        Instant receivedAt
) {
    public static final int CURRENT_SCHEMA_VERSION = 1;

    public static MeasurementEvent create(
            String warehouseId,
            MeasurementPayload payload,
            SensorType sensorType) {
        return new MeasurementEvent(
                UUID.randomUUID(),
                CURRENT_SCHEMA_VERSION,
                warehouseId,
                payload.sensorId(),
                sensorType,
                payload.value(),
                Instant.now()
        );
    }
}
