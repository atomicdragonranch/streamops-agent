package com.streamops.simulator.generators;

import com.streamops.proto.LogEvent;
import com.streamops.proto.Severity;
import com.streamops.proto.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Generates structured log events mirroring real Flink/Kafka component output.
 * In normal mode, most logs are INFO/DEBUG; error burst mode shifts the distribution
 * to produce ERROR/WARN patterns the anomaly detector should flag.
 *
 * Error probabilities are loaded from application.properties.
 */
public class LogGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(LogGenerator.class);

    private static final String[] COMPONENTS = {
        "kafka-consumer", "flink-operator", "state-backend", "kafka-producer"
    };

    private static final String[] NORMAL_MESSAGES = {
        "Checkpoint completed successfully",
        "Consumer group rebalance triggered",
        "Operator restored from savepoint",
        "Records processed within SLA",
        "State snapshot persisted to RocksDB",
        "Kafka partition assignment updated",
        "Watermark advanced",
        "Window evaluation completed"
    };

    private static final String[] ERROR_MESSAGES = {
        "Checkpoint timeout exceeded 60s threshold",
        "OutOfMemoryError in state backend",
        "Kafka consumer poll timeout, possible network partition",
        "Serialization failed for state snapshot",
        "TaskManager lost heartbeat, initiating failover",
        "RocksDB compaction stalled, write amplification critical",
        "Consumer lag exceeding threshold on partition 3",
        "Deadlock detected in operator chain"
    };

    private static final String[] ERROR_STACK_TRACES = {
        "java.lang.OutOfMemoryError: Java heap space\n\tat org.apache.flink.runtime.state.heap.HeapKeyedStateBackend.put(HeapKeyedStateBackend.java:142)",
        "org.apache.kafka.common.errors.TimeoutException: Failed to update metadata after 60000 ms",
        "java.io.IOException: Checkpoint was declined (tasks not ready)\n\tat org.apache.flink.runtime.checkpoint.CheckpointCoordinator.triggerCheckpoint(CheckpointCoordinator.java:581)"
    };

    private final AtomicBoolean errorBurstMode = new AtomicBoolean(false);
    private final double errorBurstProbability;
    private final double normalErrorProbability;
    private final double stacktraceProbability;
    private final double debugProbability;

    public LogGenerator() {
        this(new Properties());
    }

    public LogGenerator(Properties config) {
        this.errorBurstProbability = Double.parseDouble(config.getProperty("log.error.burst.probability", "0.6"));
        this.normalErrorProbability = Double.parseDouble(config.getProperty("log.normal.error.probability", "0.05"));
        this.stacktraceProbability = Double.parseDouble(config.getProperty("log.stacktrace.probability", "0.5"));
        this.debugProbability = Double.parseDouble(config.getProperty("log.debug.probability", "0.3"));
    }

    public StreamEvent generate() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        String component = COMPONENTS[rng.nextInt(COMPONENTS.length)];
        boolean isError = errorBurstMode.get()
            ? rng.nextDouble() < errorBurstProbability
            : rng.nextDouble() < normalErrorProbability;

        Severity severity;
        String message;
        String stackTrace = "";

        if (isError) {
            severity = rng.nextBoolean() ? Severity.ERROR : Severity.WARN;
            message = ERROR_MESSAGES[rng.nextInt(ERROR_MESSAGES.length)];
            if (severity == Severity.ERROR && rng.nextDouble() < stacktraceProbability) {
                stackTrace = ERROR_STACK_TRACES[rng.nextInt(ERROR_STACK_TRACES.length)];
            }
        } else {
            severity = rng.nextDouble() < debugProbability ? Severity.DEBUG : Severity.INFO;
            message = NORMAL_MESSAGES[rng.nextInt(NORMAL_MESSAGES.length)];
        }

        LogEvent log = LogEvent.newBuilder()
            .setSeverity(severity)
            .setMessage(message)
            .setComponent(component)
            .setLoggerName(component + ".MainProcessor")
            .setStackTrace(stackTrace)
            .putAllContext(Map.of("pipeline", "streamops", "env", "dev"))
            .build();

        StreamEvent event = StreamEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestampMs(System.currentTimeMillis())
            .setSource(component)
            .setLog(log)
            .build();

        if (isError) {
            LOG.debug("Generated error log: component={}, severity={}, msg={}",
                component, severity, message);
        }
        return event;
    }

    public void setErrorBurstMode(boolean enabled) {
        LOG.info("Error burst mode: {}", enabled ? "ENABLED" : "DISABLED");
        errorBurstMode.set(enabled);
    }

    public boolean isErrorBurstMode() {
        return errorBurstMode.get();
    }
}
