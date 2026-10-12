package com.example.warehouse.agent.queue;

import com.example.warehouse.contract.MeasurementEvent;
import com.example.warehouse.contract.SensorType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MeasurementQueueTest {
    @Test
    void acceptsEventsAndDrainsOnlyRequestedBatchSize() {
        MeasurementQueue queue = new MeasurementQueue(3);
        MeasurementEvent first = event("t1");
        MeasurementEvent second = event("t2");

        assertTrue(queue.offer(first));
        assertTrue(queue.offer(second));
        assertEquals(2, queue.depth());
        assertEquals(3, queue.capacity());

        var batch = queue.drainBatch(1);
        assertEquals(1, batch.size());
        assertSame(first, batch.get(0));
        assertEquals(1, queue.depth());
        assertEquals(1, queue.drainBatch(5).size());
        assertEquals(0, queue.depth());
        assertEquals(2, queue.acceptedCount());
    }

    @Test
    void countsOverflowWithoutBlockingOrLosingExistingQueuedEvent() {
        MeasurementQueue queue = new MeasurementQueue(1);
        MeasurementEvent accepted = event("t1");

        assertTrue(queue.offer(accepted));
        assertFalse(queue.offer(event("t2")));

        assertEquals(1, queue.depth());
        assertEquals(1, queue.acceptedCount());
        assertEquals(1, queue.overflowCount());
        assertSame(accepted, queue.drainBatch(1).get(0));
    }

    @Test
    void rejectsInvalidCapacityEventAndBatchSize() {
        assertThrows(IllegalArgumentException.class, () -> new MeasurementQueue(0));
        MeasurementQueue queue = new MeasurementQueue(1);
        assertThrows(IllegalArgumentException.class, () -> queue.offer(null));
        assertThrows(IllegalArgumentException.class, () -> queue.drainBatch(0));
    }

    private MeasurementEvent event(String sensorId) {
        return new MeasurementEvent(UUID.randomUUID(), MeasurementEvent.CURRENT_SCHEMA_VERSION,
                "warehouse-1", sensorId, SensorType.TEMPERATURE, 25.0, Instant.now());
    }
}
