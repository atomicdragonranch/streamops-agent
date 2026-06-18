package com.streamops.processor;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class StreamProcessorAppTest {

    @Test
    void applicationPropertiesExistsOnClasspath() {
        // Arrange + Act
        InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties");

        // Assert
        assertThat(is).isNotNull();
    }

    @Test
    void applicationPropertiesContainsAllRequiredKeys() throws Exception {
        // Arrange
        Properties config = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            config.load(is);
        }

        // Assert: Kafka connectivity
        assertThat(config.getProperty("kafka.bootstrap")).isNotBlank();
        assertThat(config.getProperty("kafka.input.topic")).isNotBlank();
        assertThat(config.getProperty("kafka.alert.topic")).isNotBlank();
        assertThat(config.getProperty("kafka.group.id")).isNotBlank();

        // Assert: Flink settings
        assertThat(config.getProperty("flink.checkpoint.interval.ms")).isNotBlank();
        assertThat(config.getProperty("flink.watermark.max.out.of.orderness.seconds")).isNotBlank();

        // Assert: Anomaly thresholds
        assertThat(config.getProperty("anomaly.latency.threshold.ms")).isNotBlank();
        assertThat(config.getProperty("anomaly.backpressure.threshold")).isNotBlank();
        assertThat(config.getProperty("anomaly.checkpoint.threshold.ms")).isNotBlank();
        assertThat(config.getProperty("anomaly.error.rate.threshold")).isNotBlank();
        assertThat(config.getProperty("anomaly.heap.threshold.percent")).isNotBlank();
        assertThat(config.getProperty("anomaly.consumer.lag.threshold")).isNotBlank();
        assertThat(config.getProperty("anomaly.ema.alpha")).isNotBlank();
        assertThat(config.getProperty("anomaly.error.rate.min.events")).isNotBlank();
        assertThat(config.getProperty("anomaly.log.severity.threshold")).isNotBlank();
    }

    @Test
    void thresholdValuesAreValidDoubles() throws Exception {
        // Arrange
        Properties config = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            config.load(is);
        }

        // Act + Assert: all threshold values parse without error
        assertThat(Double.parseDouble(config.getProperty("anomaly.latency.threshold.ms"))).isPositive();
        assertThat(Double.parseDouble(config.getProperty("anomaly.backpressure.threshold"))).isBetween(0.0, 1.0);
        assertThat(Double.parseDouble(config.getProperty("anomaly.error.rate.threshold"))).isBetween(0.0, 1.0);
        assertThat(Double.parseDouble(config.getProperty("anomaly.heap.threshold.percent"))).isBetween(0.0, 100.0);
        assertThat(Double.parseDouble(config.getProperty("anomaly.ema.alpha"))).isBetween(0.0, 1.0);
    }

    @Test
    void checkpointIntervalIsReasonable() throws Exception {
        // Arrange
        Properties config = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            config.load(is);
        }

        // Act
        long intervalMs = Long.parseLong(config.getProperty("flink.checkpoint.interval.ms"));

        // Assert: between 1 second and 10 minutes
        assertThat(intervalMs).isBetween(1000L, 600_000L);
    }
}
