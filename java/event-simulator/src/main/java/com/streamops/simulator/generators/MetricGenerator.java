package com.streamops.simulator.generators;

import com.streamops.proto.MetricEvent;
import com.streamops.proto.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Generates realistic streaming infrastructure metrics. Normal operation produces
 * values within healthy ranges; anomaly injection shifts the distribution to
 * simulate degradation the Flink job should detect.
 */
public class MetricGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(MetricGenerator.class);

    private static final String[] COMPONENTS = {
        "kafka-consumer", "flink-operator", "state-backend", "kafka-producer", "checkpoint-coordinator"
    };

    private static final String[] METRIC_NAMES = {
        "records_per_second", "latency_ms", "backpressure_ratio", "checkpoint_duration_ms",
        "heap_usage_percent", "consumer_lag"
    };

    private final AtomicReference<AnomalyState> anomaly = new AtomicReference<>(AnomalyState.NONE);

    public StreamEvent generate() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        String component = COMPONENTS[rng.nextInt(COMPONENTS.length)];
        String metricName = METRIC_NAMES[rng.nextInt(METRIC_NAMES.length)];

        double value = generateValue(metricName, rng);

        MetricEvent metric = MetricEvent.newBuilder()
            .setMetricName(metricName)
            .setValue(value)
            .setUnit(unitFor(metricName))
            .setComponent(component)
            .putAllTags(Map.of("env", "dev", "pipeline", "streamops"))
            .build();

        StreamEvent event = StreamEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestampMs(System.currentTimeMillis())
            .setSource(component)
            .setMetric(metric)
            .build();

        LOG.debug("Generated metric: component={}, name={}, value={}", component, metricName, value);
        return event;
    }

    private double generateValue(String metricName, ThreadLocalRandom rng) {
        AnomalyState state = anomaly.get();
        return switch (metricName) {
            case "records_per_second" -> state == AnomalyState.THROUGHPUT_DROP
                ? rng.nextDouble(10, 100)       // ~99% drop
                : rng.nextDouble(5000, 15000);
            case "latency_ms" -> state == AnomalyState.LATENCY_SPIKE
                ? rng.nextDouble(500, 5000)     // 10-100x normal
                : rng.nextDouble(1, 50);
            case "backpressure_ratio" -> state == AnomalyState.BACKPRESSURE
                ? rng.nextDouble(0.7, 1.0)      // severe backpressure
                : rng.nextDouble(0, 0.1);
            case "checkpoint_duration_ms" -> state == AnomalyState.CHECKPOINT_SLOW
                ? rng.nextDouble(30000, 120000) // 30s-2min (timeout range)
                : rng.nextDouble(500, 5000);
            case "heap_usage_percent" -> state == AnomalyState.MEMORY_PRESSURE
                ? rng.nextDouble(85, 99)        // GC thrashing territory
                : rng.nextDouble(30, 70);
            case "consumer_lag" -> state == AnomalyState.THROUGHPUT_DROP
                ? rng.nextDouble(50000, 500000) // lag building fast
                : rng.nextDouble(0, 1000);
            default -> rng.nextDouble(0, 100);
        };
    }

    private String unitFor(String metricName) {
        return switch (metricName) {
            case "records_per_second" -> "records/s";
            case "latency_ms", "checkpoint_duration_ms" -> "ms";
            case "backpressure_ratio", "heap_usage_percent" -> "percent";
            case "consumer_lag" -> "records";
            default -> "unit";
        };
    }

    public void injectAnomaly(AnomalyState state) {
        LOG.info("Injecting anomaly: {}", state);
        anomaly.set(state);
    }

    public void clearAnomaly() {
        AnomalyState previous = anomaly.getAndSet(AnomalyState.NONE);
        if (previous != AnomalyState.NONE) {
            LOG.info("Cleared anomaly: {}", previous);
        }
    }

    public AnomalyState getCurrentAnomaly() {
        return anomaly.get();
    }

    public enum AnomalyState {
        NONE,
        LATENCY_SPIKE,
        THROUGHPUT_DROP,
        BACKPRESSURE,
        CHECKPOINT_SLOW,
        MEMORY_PRESSURE
    }
}
