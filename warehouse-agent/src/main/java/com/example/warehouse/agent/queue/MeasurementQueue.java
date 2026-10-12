package com.example.warehouse.agent.queue;

import com.example.warehouse.contract.MeasurementEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class MeasurementQueue {
    private final ArrayBlockingQueue<MeasurementEvent> queue;
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong overflow = new AtomicLong();

    public MeasurementQueue(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive");
        queue = new ArrayBlockingQueue<>(capacity);
    }

    public boolean offer(MeasurementEvent event) {
        if (event == null) throw new IllegalArgumentException("event cannot be null");
        if (!queue.offer(event)) {
            overflow.incrementAndGet();
            return false;
        }
        accepted.incrementAndGet();
        return true;
    }

    public List<MeasurementEvent> drainBatch(int maxElements) {
        if (maxElements <= 0) throw new IllegalArgumentException("maxElements must be positive");
        List<MeasurementEvent> batch = new ArrayList<>(maxElements);
        queue.drainTo(batch, maxElements);
        return batch;
    }

    public int depth() { return queue.size(); }
    public int capacity() { return queue.size() + queue.remainingCapacity(); }
    public long acceptedCount() { return accepted.get(); }
    public long overflowCount() { return overflow.get(); }
}

