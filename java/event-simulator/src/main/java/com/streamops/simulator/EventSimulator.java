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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone Kafka producer that generates realistic streaming infrastructure events.
 *
 * Three generators run on independent schedules to simulate a live environment.
 * A ScenarioRunner injects anomalies (latency spikes, error bursts, backpressure)
 * on demand via CLI arg or API.
 *
 * All timing, thresholds, and ranges are loaded from application.properties.
 * Override via environment variables: KAFKA_BOOTSTRAP, KAFKA_TOPIC.
 *
 * This is a separate process from the Flink job. In production, real infrastructure
 * generates these events; the simulator stands in during local dev and demos.
 */
public class EventSimulator {

    private static final Logger LOG = LoggerFactory.getLogger(EventSimulator.class);

    private final Properties config;
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
        this(loadProperties());
    }

    public EventSimulator(Properties config) {
        this.config = config;

        String bootstrapServers = envOrDefault("KAFKA_BOOTSTRAP",
            config.getProperty("kafka.bootstrap", "localhost:9092"));
        this.topic = envOrDefault("KAFKA_TOPIC",
            config.getProperty("kafka.topic", "stream-events"));

        this.producer = createProducer(bootstrapServers);
        int threadPoolSize = Integer.parseInt(config.getProperty("simulator.thread.pool.size", "4"));
        this.scheduler = Executors.newScheduledThreadPool(threadPoolSize);
        this.metricGenerator = new MetricGenerator(config);
        this.logGenerator = new LogGenerator(config);
        this.heartbeatGenerator = new HeartbeatGenerator();
        this.scenarioRunner = new ScenarioRunner(metricGenerator, logGenerator, config);

        LOG.info("Simulator initialized: bootstrap={}, topic={}", bootstrapServers, topic);
    }

    private KafkaProducer<String, byte[]> createProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, config.getProperty("kafka.producer.acks", "1"));
        props.put(ProducerConfig.LINGER_MS_CONFIG,
            Integer.parseInt(config.getProperty("kafka.producer.linger.ms", "5")));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,
            Integer.parseInt(config.getProperty("kafka.producer.batch.size", "16384")));

        LOG.debug("Kafka producer config: bootstrap={}, acks={}, linger={}ms",
            bootstrapServers, props.get(ProducerConfig.ACKS_CONFIG), props.get(ProducerConfig.LINGER_MS_CONFIG));
        return new KafkaProducer<>(props);
    }

    public void start() {
        MDC.put("component", "event-simulator");

        int metricInterval = Integer.parseInt(config.getProperty("simulator.metric.interval.seconds", "1"));
        int logInterval = Integer.parseInt(config.getProperty("simulator.log.interval.seconds", "2"));
        int heartbeatInterval = Integer.parseInt(config.getProperty("simulator.heartbeat.interval.seconds", "5"));
        int progressInterval = Integer.parseInt(config.getProperty("simulator.progress.interval.seconds", "10"));

        LOG.info("Starting event generators: metrics={}s, logs={}s, heartbeats={}s",
            metricInterval, logInterval, heartbeatInterval);

        scheduler.scheduleAtFixedRate(
            () -> safeSend(metricGenerator.generate()), 0, metricInterval, TimeUnit.SECONDS);

        scheduler.scheduleAtFixedRate(
            () -> safeSend(logGenerator.generate()), 500, logInterval, TimeUnit.SECONDS);

        scheduler.scheduleAtFixedRate(
            () -> safeSend(heartbeatGenerator.generate()), 0, heartbeatInterval, TimeUnit.SECONDS);

        scheduler.scheduleAtFixedRate(this::logProgress, progressInterval, progressInterval, TimeUnit.SECONDS);

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
            LOG.warn("Progress: sent={}, errors={}, error_rate={}%",
                sent, errors, String.format("%.2f", (errors * 100.0) / Math.max(1, sent + errors)));
        } else {
            LOG.info("Progress: sent={}", sent);
        }
    }

    public void stop() {
        LOG.info("Shutting down simulator. Total sent={}, errors={}",
            eventCount.get(), errorCount.get());

        int shutdownTimeout = Integer.parseInt(config.getProperty("simulator.shutdown.timeout.seconds", "5"));
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(shutdownTimeout, TimeUnit.SECONDS)) {
                LOG.warn("Scheduler did not terminate in {}s, forcing shutdown", shutdownTimeout);
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

    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream is = EventSimulator.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (is != null) {
                props.load(is);
            } else {
                LOG.warn("application.properties not found on classpath, using defaults");
            }
        } catch (IOException e) {
            LOG.warn("Failed to load application.properties: {}", e.getMessage());
        }
        return props;
    }

    private static String envOrDefault(String envKey, String defaultValue) {
        String value = System.getenv(envKey);
        return value != null && !value.isBlank() ? value : defaultValue;
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
