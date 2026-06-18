package com.streamops.simulator.generators;

import com.streamops.proto.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ScenarioRunnerTest {

    @Test
    void latencySpikeScenarioInjectsAnomaly() {
        // Arrange
        MetricGenerator metricGen = new MetricGenerator();
        LogGenerator logGen = new LogGenerator();
        ScenarioRunner runner = new ScenarioRunner(metricGen, logGen);
        AtomicInteger eventsSent = new AtomicInteger(0);

        // Act
        runner.run("latency-spike", event -> eventsSent.incrementAndGet());

        // Assert
        assertThat(metricGen.getCurrentAnomaly()).isEqualTo(MetricGenerator.AnomalyState.LATENCY_SPIKE);

        // Cleanup
        metricGen.clearAnomaly();
    }

    @Test
    void errorBurstScenarioEnablesErrorMode() {
        // Arrange
        MetricGenerator metricGen = new MetricGenerator();
        LogGenerator logGen = new LogGenerator();
        ScenarioRunner runner = new ScenarioRunner(metricGen, logGen);

        // Act
        runner.run("error-burst", event -> {});

        // Assert
        assertThat(logGen.isErrorBurstMode()).isTrue();

        // Cleanup
        logGen.setErrorBurstMode(false);
    }

    @Test
    void unknownScenarioDoesNotCrash() {
        // Arrange
        MetricGenerator metricGen = new MetricGenerator();
        LogGenerator logGen = new LogGenerator();
        ScenarioRunner runner = new ScenarioRunner(metricGen, logGen);

        // Act
        runner.run("nonexistent-scenario", event -> {});

        // Assert: no exception thrown, state unchanged
        assertThat(metricGen.getCurrentAnomaly()).isEqualTo(MetricGenerator.AnomalyState.NONE);
    }

    @Test
    void availableScenariosReturnsSixEntries() {
        // Arrange
        MetricGenerator metricGen = new MetricGenerator();
        LogGenerator logGen = new LogGenerator();
        ScenarioRunner runner = new ScenarioRunner(metricGen, logGen);

        // Act
        var scenarios = runner.getAvailableScenarios();

        // Assert
        assertThat(scenarios).hasSize(6);
        assertThat(scenarios).containsKeys(
            "latency-spike", "throughput-drop", "error-burst",
            "backpressure", "checkpoint-timeout", "memory-pressure"
        );
    }

    @Test
    void customDurationFromConfig() {
        // Arrange
        Properties config = new Properties();
        config.setProperty("scenario.latency-spike.duration.seconds", "99");
        MetricGenerator metricGen = new MetricGenerator();
        LogGenerator logGen = new LogGenerator();
        ScenarioRunner runner = new ScenarioRunner(metricGen, logGen, config);

        // Act
        var scenarios = runner.getAvailableScenarios();

        // Assert
        assertThat(scenarios.get("latency-spike").durationSeconds()).isEqualTo(99);
    }
}
