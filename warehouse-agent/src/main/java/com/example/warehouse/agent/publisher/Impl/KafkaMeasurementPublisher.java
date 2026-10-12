package com.example.warehouse.agent.publisher.Impl;

import com.example.warehouse.agent.publisher.MeasurementPublisher;
import com.example.warehouse.agent.queue.MeasurementQueue;
import com.example.warehouse.contract.MeasurementEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.time.Duration;
import reactor.core.publisher.Flux;

public final class KafkaMeasurementPublisher implements MeasurementPublisher {
    private static final Logger log = LoggerFactory.getLogger(KafkaMeasurementPublisher.class);

    private final String topic;
    private final int batchSize;
    private final KafkaSender<String, String> sender;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    private volatile Disposable subscription;

    public KafkaMeasurementPublisher(String bootstrapServers, String topic, int workers, int batchSize) {
        this.topic = topic;
        this.batchSize = batchSize;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);

        this.sender = KafkaSender.create(SenderOptions.<String, String>create(props)
                .maxInFlight(Math.max(batchSize * workers, 100)));
    }

    @Override
    public synchronized void start(MeasurementQueue queue) {
        if (subscription != null) throw new IllegalStateException("Publisher already started");

        /*
         * ArrayBlockingQueue.offer/drainTo are bounded and non-waiting from the caller's
         * perspective. The periodic drain is on a Reactor scheduler, never on Netty's event loop.
         */
        subscription = Flux.interval(Duration.ZERO, Duration.ofMillis(10), Schedulers.parallel())
                .onBackpressureDrop()
                .map(tick -> queue.drainBatch(batchSize))
                .filter(batch -> !batch.isEmpty())
                .concatMap(this::publishBatch, 1)
                .retryWhen(
                        Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
                                .maxBackoff(Duration.ofSeconds(5))
                                .doBeforeRetry(signal ->
                                        log.warn("Kafka publisher pipeline restarting",
                                                signal.failure()))
                )
                .subscribe(
                        ignored -> {},
                        error -> log.error("Kafka publisher pipeline terminated", error)
                );
    }

    private Flux<?> publishBatch(List<MeasurementEvent> batch) {
        return sender.send(Flux.fromIterable(batch).map(event -> {
                    try {
                        String json = mapper.writeValueAsString(event);
                        String key = event.warehouseId() + ":" + event.sensorId();
                        ProducerRecord<String, String> record =
                                new ProducerRecord<>(topic, key, json);
                        return SenderRecord.create(record, event.eventId());
                    } catch (Exception e) {
                        failed.incrementAndGet();
                        throw new IllegalArgumentException("Cannot serialize event " + event.eventId(), e);
                    }
                }))
                .doOnNext(result -> {
                    if (result.exception() == null) {
                        delivered.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                        log.error("Kafka delivery failed for event {}", result.correlationMetadata(),
                                result.exception());
                    }
                })
                .retryWhen(Retry.backoff(5, Duration.ofMillis(200))
                        .maxBackoff(Duration.ofSeconds(5))
                        .doBeforeRetry(signal ->
                                log.warn("Retrying Kafka batch after send failure (attempt {})",
                                        signal.totalRetries() + 1, signal.failure())))
                .onErrorResume(error -> {
                    // After bounded retries, record the failure and continue consuming later batches.
                    // For strict delivery requirements, route this batch to a durable retry/DLQ store.
                    failed.addAndGet(batch.size());
                    log.error("Kafka batch failed after retries; {} events could not be delivered",
                            batch.size(), error);
                    return reactor.core.publisher.Flux.empty();
                });
    }

    public long deliveredCount() { return delivered.get(); }
    public long failedCount() { return failed.get(); }

    @Override
    public synchronized void close() {
        Disposable current = subscription;
        if (current != null) {
            current.dispose();
            subscription = null;
        }
        sender.close();
        log.info("Kafka publisher stopped. delivered={}, failed={}", delivered.get(), failed.get());
    }
}

