package com.example.warehouse.contract;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MeasurementPayloadParserTest {
    private final MeasurementPayloadParser parser = new MeasurementPayloadParser();

    @Test
    void parsesValidTemperaturePayload() {
        var result = parser.parse("sensor_id=t1; value=30");

        assertTrue(result.valid());
        assertEquals("t1", result.payload().sensorId());
        assertEquals(30.0, result.payload().value());
    }

    @Test
    void acceptsWhitespaceAndNegativeValues() {
        var result = parser.parse(" sensor_id = h-1 ; value = -2.5 ");

        assertTrue(result.valid());
        assertEquals("h-1", result.payload().sensorId());
        assertEquals(-2.5, result.payload().value());
    }

    @Test
    void rejectsEmptyMalformedDuplicateAndUnexpectedFields() {
        assertFalse(parser.parse(null).valid());
        assertFalse(parser.parse("   ").valid());
        assertFalse(parser.parse("sensor_id=t1; value").valid());
        assertFalse(parser.parse("sensor_id=t1; sensor_id=t2; value=30").valid());
        assertFalse(parser.parse("sensor_id=t1; value=30; extra=x").valid());
    }

    @Test
    void rejectsInvalidSensorIdAndNonNumericOrNonFiniteValues() {
        assertFalse(parser.parse("sensor_id=t 1; value=30").valid());
        assertFalse(parser.parse("sensor_id=t1; value=abc").valid());
        assertFalse(parser.parse("sensor_id=t1; value=NaN").valid());
        assertFalse(parser.parse("sensor_id=t1; value=Infinity").valid());
    }
}
