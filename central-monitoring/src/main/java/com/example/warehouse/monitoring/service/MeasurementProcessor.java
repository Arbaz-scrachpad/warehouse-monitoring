package com.example.warehouse.monitoring.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.warehouse.monitoring.rules.AlarmRule;
import com.example.warehouse.contract.MeasurementEvent;
import com.example.warehouse.monitoring.rules.AlarmRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MeasurementProcessor {
    private static final Logger log = LoggerFactory.getLogger(MeasurementProcessor.class);

    private final ObjectMapper objectMapper;
    private final AlarmRule alarmRule;

    public MeasurementProcessor(ObjectMapper objectMapper, AlarmRule alarmRule) {
        this.objectMapper = objectMapper;
        this.alarmRule = alarmRule;
    }

    public void process(String json) {
        final MeasurementEvent event;
        try {
            event = objectMapper.readValue(json, MeasurementEvent.class);
        } catch (Exception e) {
            // Poison payloads are logged and handled by the consumer so they don't loop forever.
            log.warn("Ignoring invalid measurement event payload: {}", e.getMessage());
            return;
        }

        if (event.schemaVersion() != MeasurementEvent.CURRENT_SCHEMA_VERSION) {
            log.warn("Ignoring unsupported schema version {} for event {}",
                    event.schemaVersion(), event.eventId());
            return;
        }

        alarmRule.evaluate(event).ifPresent(alarm ->
                log.warn("{} warehouse={} sensor={} value={} threshold={} eventId={}",
                        alarm.message(), alarm.warehouseId(), alarm.sensorId(),
                        alarm.observedValue(), alarm.threshold(), event.eventId()));
    }
}

