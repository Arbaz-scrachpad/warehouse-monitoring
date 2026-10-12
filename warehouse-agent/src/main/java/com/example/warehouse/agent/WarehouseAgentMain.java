package com.example.warehouse.agent;

import com.example.warehouse.agent.publisher.Impl.KafkaMeasurementPublisher;
import com.example.warehouse.agent.queue.MeasurementQueue;
import com.example.warehouse.agent.receiver.UdpReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public final class WarehouseAgentMain {
    private static final Logger log = LoggerFactory.getLogger(WarehouseAgentMain.class);

    private WarehouseAgentMain() {}

    public static void main(String[] args) throws Exception {
        AgentConfig config = AgentConfig.load();
        MeasurementQueue queue = new MeasurementQueue(config.queueCapacity());
        KafkaMeasurementPublisher publisher = new KafkaMeasurementPublisher(
                config.bootstrapServers(), config.topic(),
                config.publisherWorkers(), config.batchSize());
        UdpReceiver receiver = new UdpReceiver(
                config.warehouseId(), config.temperaturePort(), config.humidityPort(),
                config.maxPacketBytes(), queue);

        AtomicBoolean stopping = new AtomicBoolean();
        CountDownLatch stopSignal = new CountDownLatch(1);

        Runnable shutdown = () -> {
            if (!stopping.compareAndSet(false, true)) return;
            log.info("Stopping warehouse agent");
            try {
                receiver.close();
                // Give the reactive publisher time to drain the queue before disposing it.
                long deadline = System.nanoTime() + java.time.Duration.ofSeconds(10).toNanos();
                while (queue.depth() > 0 && System.nanoTime() < deadline) {
                    try {
                        Thread.sleep(10); // shutdown thread only; never a Netty event-loop thread
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                publisher.close();
                log.info("Stopped. queueDepth={}, accepted={}, overflow={}",
                        queue.depth(), queue.acceptedCount(), queue.overflowCount());
            } finally {
                stopSignal.countDown();
            }
        };

        Runtime.getRuntime().addShutdownHook(new Thread(shutdown, "warehouse-agent-shutdown"));

        try {
            publisher.start(queue);
            receiver.start();
            log.info("Warehouse agent started for {}", config.warehouseId());
            stopSignal.await();
        } catch (Exception e) {
            shutdown.run();
            throw e;
        }
    }
}
