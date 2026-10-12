package com.example.warehouse.agent.publisher;

import com.example.warehouse.agent.queue.MeasurementQueue;

public interface MeasurementPublisher extends AutoCloseable{
    void start(MeasurementQueue queue);
    @Override
    void close();
}
