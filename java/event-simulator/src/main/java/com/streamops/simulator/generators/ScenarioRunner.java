package com.streamops.simulator.generators;

import com.streamops.proto.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Orchestrates timed anomaly injection scenarios for testing the Flink detection logic.
 *
 * Each scenario follows the same pattern: inject anomaly, hold for N seconds, clear.
 * This gives the Flink windowing functions time to observe the anomalous values
 * and (hopefully) fire an alert before the scenario self-resolves.
 *
 * Scenario durations are loaded from application.properties.
 */
public class ScenarioRunner {

    private static final Logger LOG = LoggerFactory.getLogger(ScenarioRunner.class);

    private final MetricGenerator metricGenerator;
    private final LogGenerator logGenerator;
    private final ScheduledExecutorService scheduler;
    private final Map<String, ScenarioConfig> scenarios;

    private static Map<String, ScenarioConfig> buildScenarios(Properties config) {
        return Map.of(
            "latency-spike", new ScenarioConfig(
                MetricGenerator.AnomalyState.LATENCY_SPIKE, false,
                Integer.parseInt(config.getProperty("scenario.latency-spike.duration.seconds", "30")),
                "Simulates network degradation or GC pressure causing latency to spike 10-100x"),
            "throughput-drop", new ScenarioConfig(
                MetricGenerator.AnomalyState.THROUGHPUT_DROP, false,
                Integer.parseInt(config.getProperty("scenario.throughput-drop.duration.seconds", "45")),
                "Simulates upstream partition failure causing throughput to drop ~99%"),
            "error-burst", new ScenarioConfig(
                MetricGenerator.AnomalyState.NONE, true,
                Integer.parseInt(config.getProperty("scenario.error-burst.duration.seconds", "20")),
                "Simulates cascading failure generating high error rate in logs"),
            "backpressure", new ScenarioConfig(
                MetricGenerator.AnomalyState.BACKPRESSURE, false,
                Integer.parseInt(config.getProperty("scenario.backpressure.duration.seconds", "40")),
                "Simulates slow downstream consumer causing severe backpressure"),
            "checkpoint-timeout", new ScenarioConfig(
                MetricGenerator.AnomalyState.CHECKPOINT_SLOW, true,
                Integer.parseInt(config.getProperty("scenario.checkpoint-timeout.duration.seconds", "60")),
                "Simulates large state size causing checkpoint durations to approach timeout"),
            "memory-pressure", new ScenarioConfig(
                MetricGenerator.AnomalyState.MEMORY_PRESSURE, false,
                Integer.parseInt(config.getProperty("scenario.memory-pressure.duration.seconds", "35")),
                "Simulates memory leak pushing heap usage into GC thrashing territory")
        );
    }

    public ScenarioRunner(MetricGenerator metricGenerator, LogGenerator logGenerator) {
        this(metricGenerator, logGenerator, new Properties());
    }

    public ScenarioRunner(MetricGenerator metricGenerator, LogGenerator logGenerator, Properties config) {
        this.metricGenerator = metricGenerator;
        this.logGenerator = logGenerator;
        this.scenarios = buildScenarios(config);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "scenario-runner");
            t.setDaemon(true);
            return t;
        });
    }

    public void run(String scenarioName, Consumer<StreamEvent> sender) {
        ScenarioConfig config = scenarios.get(scenarioName);
        if (config == null) {
            LOG.error("Unknown scenario '{}'. Available: {}", scenarioName, scenarios.keySet());
            return;
        }

        LOG.info("Starting scenario '{}': {} (duration={}s)", scenarioName,
            config.description(), config.durationSeconds());

        if (config.metricAnomaly() != MetricGenerator.AnomalyState.NONE) {
            metricGenerator.injectAnomaly(config.metricAnomaly());
        }
        if (config.errorBurst()) {
            logGenerator.setErrorBurstMode(true);
        }

        scheduler.schedule(() -> {
            LOG.info("Scenario '{}' complete, restoring normal operation", scenarioName);
            metricGenerator.clearAnomaly();
            logGenerator.setErrorBurstMode(false);
        }, config.durationSeconds(), TimeUnit.SECONDS);
    }

    public Map<String, ScenarioConfig> getAvailableScenarios() {
        return scenarios;
    }

    public record ScenarioConfig(
        MetricGenerator.AnomalyState metricAnomaly,
        boolean errorBurst,
        int durationSeconds,
        String description
    ) {}
}
