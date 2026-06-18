package com.streamops.simulator.generators;

import com.streamops.proto.ComponentStatus;
import com.streamops.proto.HeartbeatEvent;
import com.streamops.proto.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Emits periodic heartbeats for simulated components. Each component tracks its own
 * uptime and status independently. The AI agent uses missing heartbeats to detect
 * silent failures (a component that stops heartbeating is likely dead or stuck).
 */
public class HeartbeatGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatGenerator.class);

    private static final String[] COMPONENTS = {
        "kafka-consumer", "flink-operator", "state-backend", "kafka-producer", "checkpoint-coordinator"
    };

    private final Map<String, Long> startTimes = new ConcurrentHashMap<>();
    private final Map<String, ComponentStatus> statusOverrides = new ConcurrentHashMap<>();

    public HeartbeatGenerator() {
        long now = System.currentTimeMillis();
        for (String component : COMPONENTS) {
            startTimes.put(component, now);
        }
        LOG.debug("HeartbeatGenerator initialized with {} components", COMPONENTS.length);
    }

    public StreamEvent generate() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        String component = COMPONENTS[rng.nextInt(COMPONENTS.length)];
        long uptimeMs = System.currentTimeMillis() - startTimes.getOrDefault(component, System.currentTimeMillis());

        ComponentStatus status = statusOverrides.getOrDefault(component, ComponentStatus.HEALTHY);

        HeartbeatEvent heartbeat = HeartbeatEvent.newBuilder()
            .setComponent(component)
            .setStatus(status)
            .setUptimeMs(uptimeMs)
            .putAllMetadata(Map.of(
                "jvm_version", "17",
                "flink_version", "2.0.2",
                "pid", String.valueOf(ProcessHandle.current().pid())
            ))
            .build();

        StreamEvent event = StreamEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTimestampMs(System.currentTimeMillis())
            .setSource(component)
            .setHeartbeat(heartbeat)
            .build();

        if (status != ComponentStatus.HEALTHY) {
            LOG.debug("Heartbeat: component={}, status={}, uptime={}ms", component, status, uptimeMs);
        }
        return event;
    }

    public void setComponentStatus(String component, ComponentStatus status) {
        LOG.info("Component status override: {}={}", component, status);
        statusOverrides.put(component, status);
    }

    public void clearStatusOverrides() {
        if (!statusOverrides.isEmpty()) {
            LOG.info("Clearing {} status overrides", statusOverrides.size());
            statusOverrides.clear();
        }
    }
}
