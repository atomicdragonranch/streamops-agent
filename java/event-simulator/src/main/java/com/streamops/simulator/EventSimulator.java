package com.streamops.simulator;

import com.streamops.simulator.generators.MetricGenerator;
import com.streamops.simulator.generators.LogGenerator;
import com.streamops.simulator.generators.HeartbeatGenerator;
import com.streamops.simulator.generators.ScenarioRunner;
import com.streamops.proto.StreamEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone Kafka producer that generates realistic streaming infrastructure events.
 *
 * Three generators run on independent schedules to simulate a live environment:
 * metrics (1s), logs (2s), heartbeats (5s). A ScenarioRunner injects anomalies
 * (latency spikes, error bursts, backpressure) on demand via CLI arg or API.
 *
 * This is a separate process from the Flink job. In production, real infrastructure
 * generates these events; the simulator stands in during local dev and demos.
 */
public class EventSimulator {

    private static final Logger LOG = LoggerFactory.getLogger(EventSimulator.class);

    private static final String DEFAULT_TOPIC = "stream-events";
    private static final String DEFAULT_BOOTSTRAP = "localhost:9092";

    private final KafkaProducer<String, byte[]> producer;
    private final ScheduledExecutorService scheduler;
    private final MetricGenerator metricGenerator;
    private final LogGenerator logGenerator;
    private final HeartbeatGenerator heartbeatGenerator;
    private final ScenarioRunner scenarioRunner;
    private final String topic;
    private final AtomicLong eventCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    public EventSimulator() {
        this(
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP),
            System.getenv().getOrDefault("KAFKA_TOPIC", DEFAULT_TOPIC)
        );
    }

    public EventSimulator(String bootstrapServers, String topic) {
        this.topic = topic;
        this.producer = createProducer(bootstrapServers);
        this.scheduler = Executors.newScheduledThreadPool(4);
        this.metricGenerator = new MetricGenerator();
        this.logGenerator = new LogGenerator();
        this.heartbeatGenerator = new HeartbeatGenerator();
        this.scenarioRunner = new ScenarioRunner(metricGenerator, logGenerator);

        LOG.info("Simulator initialized: bootstrap={}, topic={}", bootstrapServers, topic);
    }

    private KafkaProducer<String, byte[]> createProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // acks=1: leader acknowledgement only; acceptable for simulator traffic
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        LOG.debug("Kafka producer config: bootstrap={}, acks=1, linger=5ms", bootstrapServers);
        return new KafkaProducer<>(props);
    }

    public void start() {
        MDC.put("component", "event-simulator");

        LOG.info("Starting event generators: metrics=1s, logs=2s, heartbeats=5s");

        scheduler.scheduleAtFixedRate(
            () -> safeSend(metricGenerator.generate()), 0, 1, TimeUnit.SECONDS);

        scheduler.scheduleAtFixedRate(
            () -> safeSend(logGenerator.generate()), 500, 2, TimeUnit.SECONDS);

        scheduler.scheduleAtFixedRate(
            () -> safeSend(heartbeatGenerator.generate()), 0, 5, TimeUnit.SECONDS);

        scheduler.scheduleAtFixedRate(this::logProgress, 10, 10, TimeUnit.SECONDS);

        LOG.info("Simulator running. Ctrl+C to stop.");
    }

    public void startScenario(String scenarioName) {
        LOG.info("Starting anomaly scenario: {}", scenarioName);
        scenarioRunner.run(scenarioName, this::safeSend);
    }

    private void safeSend(StreamEvent event) {
        try {
            send(event);
        } catch (Exception e) {
            LOG.error("Unexpected error generating/sending event", e);
            errorCount.incrementAndGet();
        }
    }

    private void send(StreamEvent event) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
            topic, event.getSource(), event.toByteArray());

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOG.error("Kafka send failed for event {}: {}",
                    event.getEventId(), exception.getMessage());
                errorCount.incrementAndGet();
            } else {
                long count = eventCount.incrementAndGet();
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Event sent: id={}, partition={}, offset={}",
                        event.getEventId(), metadata.partition(), metadata.offset());
                }
            }
        });
    }

    private void logProgress() {
        long sent = eventCount.get();
        long errors = errorCount.get();
        if (errors > 0) {
            LOG.warn("Progress: sent={}, errors={}, error_rate={:.2f}%",
                sent, errors, (errors * 100.0) / Math.max(1, sent + errors));
        } else {
            LOG.info("Progress: sent={}", sent);
        }
    }

    public void stop() {
        LOG.info("Shutting down simulator. Total sent={}, errors={}",
            eventCount.get(), errorCount.get());

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Scheduler did not terminate in 5s, forcing shutdown");
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted during shutdown, forcing");
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        producer.close();
        MDC.clear();
        LOG.info("Simulator stopped");
    }

    public long getEventCount() {
        return eventCount.get();
    }

    public long getErrorCount() {
        return errorCount.get();
    }

    public static void main(String[] args) {
        EventSimulator simulator = new EventSimulator();
        Runtime.getRuntime().addShutdownHook(new Thread(simulator::stop));

        simulator.start();

        if (args.length > 0) {
            simulator.startScenario(args[0]);
        }
    }
}
