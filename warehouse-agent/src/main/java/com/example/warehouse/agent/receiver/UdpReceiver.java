package com.example.warehouse.agent.receiver;

import com.example.warehouse.agent.queue.MeasurementQueue;
import com.example.warehouse.contract.MeasurementEvent;
import com.example.warehouse.contract.MeasurementPayloadParser;
import com.example.warehouse.contract.SensorType;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class UdpReceiver implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(UdpReceiver.class);

    private final String warehouseId;
    private final int temperaturePort;
    private final int humidityPort;
    private final int maxPacketBytes;
    private final MeasurementQueue queue;
    private final MeasurementPayloadParser parser = new MeasurementPayloadParser();
    private final EventLoopGroup eventLoops = new NioEventLoopGroup();
    private final List<Channel> channels = new ArrayList<>();
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong invalid = new AtomicLong();
    private final AtomicLong overflow = new AtomicLong();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong processingErrors = new AtomicLong();
    private volatile boolean closed;

    public UdpReceiver(String warehouseId, int temperaturePort, int humidityPort,
                       int maxPacketBytes, MeasurementQueue queue) {
        if (warehouseId == null || warehouseId.isBlank()) {
            throw new IllegalArgumentException("warehouseId must not be blank");
        }
        if (temperaturePort < 1 || temperaturePort > 65535
                || humidityPort < 1 || humidityPort > 65535) {
            throw new IllegalArgumentException("UDP ports must be between 1 and 65535");
        }
        if (temperaturePort == humidityPort) {
            throw new IllegalArgumentException("UDP ports must be different");
        }
        if (maxPacketBytes <= 0) {
            throw new IllegalArgumentException("maxPacketBytes must be positive");
        }
        if (queue == null) {
            throw new IllegalArgumentException("queue must not be null");
        }
        this.warehouseId = warehouseId;
        this.temperaturePort = temperaturePort;
        this.humidityPort = humidityPort;
        this.maxPacketBytes = maxPacketBytes;
        this.queue = queue;
    }

    public synchronized void start() throws InterruptedException {
        if (closed) throw new IllegalStateException("Receiver is closed");
        log.info(
                "Starting UDP receiver: warehouseId={}, temperaturePort={}, humidityPort={}",
                warehouseId, temperaturePort, humidityPort
        );
        try {
            bind(temperaturePort, SensorType.TEMPERATURE);
            bind(humidityPort, SensorType.HUMIDITY);
            log.info(
                    "UDP receiver started successfully: temperaturePort={}, humidityPort={}",
                    temperaturePort, humidityPort
            );
        } catch (InterruptedException | RuntimeException e) {
            log.error("Failed to start UDP receiver", e);
            close();
            throw e;
        }
    }

    private void bind(int port, SensorType type) throws InterruptedException {
        log.info("Binding UDP listener: type={}, port={}", type, port);
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoops)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_REUSEADDR, false)
                    .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                        @Override
                        protected void channelRead0(
                                io.netty.channel.ChannelHandlerContext ctx,
                                DatagramPacket packet) {
                            // Only validate, decode, parse, create an event, and non-blockingly offer.
                            received.incrementAndGet();
                            ByteBuf content = packet.content();
                            int length = content.readableBytes();

                            // Log packet arrival without doing blocking work.
                            log.info(
                                    "UDP packet received: type={}, port={}, sender={}, bytes={}",
                                    type, port, packet.sender(), length
                            );
                            if (length == 0 || length > maxPacketBytes) {
                                invalid.incrementAndGet();
                                log.warn(
                                        "UDP packet rejected: reason={}, port={}, sender={}, bytes={}, maxBytes={}",
                                        length == 0 ? "EMPTY_PAYLOAD" : "PACKET_TOO_LARGE",
                                        port, packet.sender(), length, maxPacketBytes
                                );
                                return;
                            }
                            try {
                                String payload = content.toString(StandardCharsets.UTF_8);
                                MeasurementPayloadParser.ParseResult result = parser.parse(payload);
                                if (!result.valid()) {
                                    invalid.incrementAndGet();
                                    // Avoid logging untrusted raw payload contents.
                                    log.warn(
                                            "UDP payload rejected by parser: port={}, sender={}, bytes={}",
                                            port, packet.sender(), length
                                    );
                                    return;
                                }

                                MeasurementEvent event = MeasurementEvent.create(
                                        warehouseId, result.payload(), type);

                                log.debug(
                                        "UDP payload parsed successfully: type={}, port={}",
                                        type, port
                                );

                                if (!queue.offer(event)) {
                                    overflow.incrementAndGet();
                                    log.error(
                                            "Measurement dropped: queue full, warehouseId={}, type={}, port={}",
                                            warehouseId, type, port
                                    );
                                    return;
                                }
                                long acceptedCount = accepted.incrementAndGet();
                                log.info(
                                        "Measurement accepted into queue: type={}, port={}, acceptedCount={}, event={}",
                                        type, port, acceptedCount, event
                                );
                            } catch (Exception e) {
                                processingErrors.incrementAndGet();

                                log.error(
                                        "Failed to process UDP packet: type={}, port={}, sender={}",
                                        type, port, packet.sender(), e
                                );
                            }
                        }

                        @Override
                        public void exceptionCaught(
                                io.netty.channel.ChannelHandlerContext ctx, Throwable cause) {
                            log.warn("UDP handler error on port {}", port, cause);
                        }
                    });

            Channel channel = bootstrap.bind(new InetSocketAddress(port)).sync().channel();
            channels.add(channel);
            log.info(
                    "UDP listener bound successfully: type={}, port={}, localAddress={}",
                    type, port, channel.localAddress()
            );
        }catch (InterruptedException e){
            Thread.currentThread().interrupt();
            log.error("Interrupted while binding UDP listener: type={}, port={}",
                    type, port, e);
            throw e;
        }catch (RuntimeException e) {
            log.error("Failed to bind UDP listener: type={}, port={}",
                    type, port, e);
            throw e;
        }
    }

    public long receivedCount() { return received.get(); }
    public long invalidCount() { return invalid.get(); }
    public long overflowCount() { return overflow.get(); }
    public long acceptedCount() { return accepted.get(); }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        log.info("Stopping UDP receiver");
        channels.forEach(channel -> channel.close().awaitUninterruptibly());
        channels.clear();
        eventLoops.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        log.info(
                "UDP receiver stopped: received={}, accepted={}, invalid={}, overflow={}, processingErrors={}",
                received.get(),
                accepted.get(),
                invalid.get(),
                overflow.get(),
                processingErrors.get()
        );
    }
}
