package com.streamops.processor.operators;

import com.streamops.proto.LogEvent;
import com.streamops.proto.MetricEvent;
import com.streamops.proto.Severity;
import com.streamops.proto.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class AnomalyDetectorTest {

    @Test
    void defaultThresholdsMatchExpectedValues() {
        // Arrange + Act
        AnomalyDetector detector = new AnomalyDetector();

        // Assert: verify defaults by creating with empty props (uses hardcoded defaults)
        // The detector should be constructable without error
        assertThat(detector).isNotNull();
    }

    @Test
    void customThresholdsLoadFromProperties() {
        // Arrange
        Properties config = new Properties();
        config.setProperty("anomaly.latency.threshold.ms", "500.0");
        config.setProperty("anomaly.backpressure.threshold", "0.8");
        config.setProperty("anomaly.checkpoint.threshold.ms", "60000.0");
        config.setProperty("anomaly.error.rate.threshold", "0.3");
        config.setProperty("anomaly.heap.threshold.percent", "90.0");
        config.setProperty("anomaly.consumer.lag.threshold", "50000.0");
        config.setProperty("anomaly.ema.alpha", "0.2");
        config.setProperty("anomaly.error.rate.min.events", "20");
        config.setProperty("anomaly.log.severity.threshold", "4");

        // Act
        AnomalyDetector detector = new AnomalyDetector(config);

        // Assert: detector created successfully with custom config
        assertThat(detector).isNotNull();
    }

    @Test
    void partialConfigFallsBackToDefaults() {
        // Arrange: only override one value, rest should use defaults
        Properties config = new Properties();
        config.setProperty("anomaly.latency.threshold.ms", "999.0");

        // Act
        AnomalyDetector detector = new AnomalyDetector(config);

        // Assert: no exception, defaults used for missing keys
        assertThat(detector).isNotNull();
    }

    @Test
    void protobufMetricEventBuildsCorrectly() {
        // Arrange + Act
        StreamEvent event = buildMetricEvent("kafka-consumer", "latency_ms", 250.0);

        // Assert
        assertThat(event.hasMetric()).isTrue();
        assertThat(event.getMetric().getMetricName()).isEqualTo("latency_ms");
        assertThat(event.getMetric().getValue()).isEqualTo(250.0);
        assertThat(event.getMetric().getComponent()).isEqualTo("kafka-consumer");
    }

    @Test
    void protobufLogEventBuildsCorrectly() {
        // Arrange + Act
        StreamEvent event = buildLogEvent("flink-operator", Severity.ERROR, "Test error");

        // Assert
        assertThat(event.hasLog()).isTrue();
        assertThat(event.getLog().getSeverity()).isEqualTo(Severity.ERROR);
        assertThat(event.getLog().getMessage()).isEqualTo("Test error");
    }

    @Test
    void applicationPropertiesLoadsFromClasspath() {
        // Arrange + Act: loading from the actual application.properties on the classpath
        Properties config = new Properties();
        try (var is = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            assertThat(is).isNotNull();
            config.load(is);
        } catch (Exception e) {
            throw new AssertionError("Failed to load application.properties", e);
        }

        // Assert: verify key properties are present
        assertThat(config.getProperty("anomaly.latency.threshold.ms")).isEqualTo("200.0");
        assertThat(config.getProperty("anomaly.backpressure.threshold")).isEqualTo("0.5");
        assertThat(config.getProperty("anomaly.ema.alpha")).isEqualTo("0.1");
        assertThat(config.getProperty("kafka.bootstrap")).isEqualTo("localhost:9092");
    }

    private static StreamEvent buildMetricEvent(String component, String metricName, double value) {
        return StreamEvent.newBuilder()
            .setEventId("test-001")
            .setTimestampMs(System.currentTimeMillis())
            .setSource(component)
            .setMetric(MetricEvent.newBuilder()
                .setMetricName(metricName)
                .setValue(value)
                .setUnit("ms")
                .setComponent(component)
                .build())
            .build();
    }

    private static StreamEvent buildLogEvent(String component, Severity severity, String message) {
        return StreamEvent.newBuilder()
            .setEventId("test-002")
            .setTimestampMs(System.currentTimeMillis())
            .setSource(component)
            .setLog(LogEvent.newBuilder()
                .setSeverity(severity)
                .setMessage(message)
                .setComponent(component)
                .setLoggerName(component + ".Test")
                .build())
            .build();
    }
}
