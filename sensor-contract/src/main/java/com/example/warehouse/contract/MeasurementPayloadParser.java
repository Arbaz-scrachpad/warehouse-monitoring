package com.example.warehouse.contract;

import java.util.HashMap;
import java.util.Map;

public final class MeasurementPayloadParser {
    private static final int MAX_SENSOR_ID_LENGTH = 128;

    /*
     * Wire protocol:
     * sensor_id=t1; value=30
     *
     * Keys are case-sensitive. Exactly two fields are accepted.
     */
    public ParseResult parse(String payload) {
        if (payload == null || payload.isBlank()) {
            return ParseResult.failure("Payload is empty");
        }

        Map<String, String> fields = new HashMap<>();
        for (String segment : payload.split(";", -1)) {
            String item = segment.trim();
            int equals = item.indexOf('=');
            if (item.isEmpty() || equals <= 0 || equals == item.length() - 1) {
                return ParseResult.failure("Each field must use key=value syntax");
            }

            String key = item.substring(0, equals).trim();
            String value = item.substring(equals + 1).trim();
            if (key.isEmpty() || value.isEmpty()) {
                return ParseResult.failure("Field key and value are required");
            }
            if (fields.putIfAbsent(key, value) != null) {
                return ParseResult.failure("Duplicate field: " + key);
            }
        }

        if (fields.size() != 2
                || !fields.containsKey("sensor_id")
                || !fields.containsKey("value")) {
            return ParseResult.failure(
                    "Expected sensor_id=<id>; value=<number>");
        }

        String sensorId = fields.get("sensor_id");
        if (sensorId.length() > MAX_SENSOR_ID_LENGTH
                || !sensorId.matches("[A-Za-z0-9._:-]+")) {
            return ParseResult.failure("Invalid sensor_id");
        }

        try {
            double value = Double.parseDouble(fields.get("value"));
            if (!Double.isFinite(value)) {
                return ParseResult.failure("value must be finite");
            }
            return ParseResult.success(new MeasurementPayload(sensorId, value));
        } catch (NumberFormatException ex) {
            return ParseResult.failure("value must be a valid number");
        }
    }

    public record ParseResult(MeasurementPayload payload, String error) {
        public boolean valid() {
            return payload != null;
        }

        public static ParseResult success(MeasurementPayload payload) {
            return new ParseResult(payload, null);
        }

        public static ParseResult failure(String error) {
            return new ParseResult(null, error);
        }
    }
}
