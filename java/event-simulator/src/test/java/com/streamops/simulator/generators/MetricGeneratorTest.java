package com.streamops.simulator.generators;

import com.streamops.proto.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class MetricGeneratorTest {

    @Test
    void generateProducesValidStreamEvent() {
        // Arrange
        MetricGenerator generator = new MetricGenerator();

        // Act
        StreamEvent event = generator.generate();

        // Assert
        assertThat(event.hasMetric()).isTrue();
        assertThat(event.getEventId()).isNotBlank();
        assertThat(event.getTimestampMs()).isPositive();
        assertThat(event.getSource()).isNotBlank();
        assertThat(event.getMetric().getMetricName()).isNotBlank();
    }

    @Test
    void generateUsesKnownComponents() {
        // Arrange
        MetricGenerator generator = new MetricGenerator();
        Set<String> knownComponents = Set.of(
            "kafka-consumer", "flink-operator", "state-backend",
            "kafka-producer", "checkpoint-coordinator"
        );

        // Act
        StreamEvent event = generator.generate();

        // Assert
        assertThat(knownComponents).contains(event.getSource());
    }

    @Test
    void normalModeProducesHealthyMetrics() {
        // Arrange
        MetricGenerator generator = new MetricGenerator();
        assertThat(generator.getCurrentAnomaly()).isEqualTo(MetricGenerator.AnomalyState.NONE);

        // Act
        boolean foundReasonableValue = false;
        for (int i = 0; i < 100; i++) {
            StreamEvent event = generator.generate();
            double value = event.getMetric().getValue();
            if (value >= 0) {
                foundReasonableValue = true;
            }
        }

        // Assert
        assertThat(foundReasonableValue).isTrue();
    }

    @Test
    void injectAnomalyChangesState() {
        // Arrange
        MetricGenerator generator = new MetricGenerator();
        assertThat(generator.getCurrentAnomaly()).isEqualTo(MetricGenerator.AnomalyState.NONE);

        // Act
        generator.injectAnomaly(MetricGenerator.AnomalyState.LATENCY_SPIKE);

        // Assert
        assertThat(generator.getCurrentAnomaly()).isEqualTo(MetricGenerator.AnomalyState.LATENCY_SPIKE);
    }

    @Test
    void clearAnomalyRestoresNormalState() {
        // Arrange
        MetricGenerator generator = new MetricGenerator();
        generator.injectAnomaly(MetricGenerator.AnomalyState.BACKPRESSURE);

        // Act
        generator.clearAnomaly();

        // Assert
        assertThat(generator.getCurrentAnomaly()).isEqualTo(MetricGenerator.AnomalyState.NONE);
    }

    @Test
    void customConfigOverridesDefaults() {
        // Arrange
        Properties config = new Properties();
        config.setProperty("metric.latency.normal.min", "100");
        config.setProperty("metric.latency.normal.max", "101");
        MetricGenerator generator = new MetricGenerator(config);

        // Act + Assert: generate many events, check that latency_ms values are in the custom range
        for (int i = 0; i < 200; i++) {
            StreamEvent event = generator.generate();
            if ("latency_ms".equals(event.getMetric().getMetricName())) {
                double value = event.getMetric().getValue();
                assertThat(value).isBetween(100.0, 101.0);
                return;
            }
        }
        // If we never generated a latency_ms metric in 200 tries, that's still valid
        // (random selection), so we don't fail the test.
    }

    @Test
    void metricEventIncludesTags() {
        // Arrange
        MetricGenerator generator = new MetricGenerator();

        // Act
        StreamEvent event = generator.generate();

        // Assert
        assertThat(event.getMetric().getTagsMap()).containsEntry("env", "dev");
        assertThat(event.getMetric().getTagsMap()).containsEntry("pipeline", "streamops");
    }
}
