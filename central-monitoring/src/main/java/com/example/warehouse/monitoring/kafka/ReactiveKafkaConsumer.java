package com.example.warehouse.monitoring.kafka;

import com.example.warehouse.monitoring.config.MonitoringKafkaProperties;
import com.example.warehouse.monitoring.service.MeasurementProcessor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Service
public class ReactiveKafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(ReactiveKafkaConsumer.class);

    private final MonitoringKafkaProperties properties;
    private final MeasurementProcessor processor;
    private Disposable subscription;

    public ReactiveKafkaConsumer(
            MonitoringKafkaProperties properties,
            MeasurementProcessor processor) {
        this.properties = properties;
        this.processor = processor;
    }

    @PostConstruct
    public void start() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, properties.groupId());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        ReceiverOptions<String, String> options = ReceiverOptions.<String, String>create(config)
                .subscription(Collections.singleton(properties.topic()));

        subscription = KafkaReceiver.create(options)
                .receive()
                .concatMap(this::handleRecord)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry(signal ->
                                log.error("Kafka consumer stream restarting", signal.failure())))
                .subscribe(
                        ignored -> {},
                        error -> log.error("Kafka consumer terminated", error)
                );

        log.info("Reactive Kafka consumer started topic={} group={}",
                properties.topic(), properties.groupId());
    }

    private Mono<Void> handleRecord(ReceiverRecord<String, String> record) {
        //todo validate
        return Mono.fromRunnable(() -> processor.process(record.value()))
                .doOnSuccess(ignored -> record.receiverOffset().acknowledge())
                .doOnError(error -> log.error("Failed to process Kafka record topic={} partition={} offset={}",
                        record.topic(), record.partition(), record.offset(), error))
                .onErrorResume(error -> {
                    // Avoid a poison record blocking the partition forever.
                    // For production, route persistent processing failures to a DLQ before acknowledging.
                    record.receiverOffset().acknowledge();
                    return Mono.empty();
                }).then();
    }

    @PreDestroy
    public void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }
}
