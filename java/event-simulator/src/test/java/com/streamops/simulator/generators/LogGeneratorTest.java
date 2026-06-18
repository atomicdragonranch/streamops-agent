package com.streamops.simulator.generators;

import com.streamops.proto.Severity;
import com.streamops.proto.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class LogGeneratorTest {

    @Test
    void generateProducesValidLogEvent() {
        // Arrange
        LogGenerator generator = new LogGenerator();

        // Act
        StreamEvent event = generator.generate();

        // Assert
        assertThat(event.hasLog()).isTrue();
        assertThat(event.getEventId()).isNotBlank();
        assertThat(event.getLog().getMessage()).isNotBlank();
        assertThat(event.getLog().getComponent()).isNotBlank();
        assertThat(event.getLog().getLoggerName()).isNotBlank();
    }

    @Test
    void normalModeProducesMostlyInfoDebug() {
        // Arrange
        LogGenerator generator = new LogGenerator();
        List<Severity> severities = new ArrayList<>();

        // Act
        for (int i = 0; i < 200; i++) {
            severities.add(generator.generate().getLog().getSeverity());
        }

        // Assert: at 5% error rate, vast majority should be INFO or DEBUG
        long infoDebug = severities.stream()
            .filter(s -> s == Severity.INFO || s == Severity.DEBUG)
            .count();
        assertThat(infoDebug).isGreaterThan(150);
    }

    @Test
    void errorBurstModeProducesMoreErrors() {
        // Arrange
        LogGenerator generator = new LogGenerator();
        generator.setErrorBurstMode(true);
        List<Severity> severities = new ArrayList<>();

        // Act
        for (int i = 0; i < 200; i++) {
            severities.add(generator.generate().getLog().getSeverity());
        }

        // Assert: at 60% error rate, we should see plenty of ERROR/WARN
        long errors = severities.stream()
            .filter(s -> s == Severity.ERROR || s == Severity.WARN)
            .count();
        assertThat(errors).isGreaterThan(80);

        generator.setErrorBurstMode(false);
    }

    @Test
    void errorBurstModeToggles() {
        // Arrange
        LogGenerator generator = new LogGenerator();

        // Assert
        assertThat(generator.isErrorBurstMode()).isFalse();

        // Act
        generator.setErrorBurstMode(true);
        assertThat(generator.isErrorBurstMode()).isTrue();

        // Act
        generator.setErrorBurstMode(false);
        assertThat(generator.isErrorBurstMode()).isFalse();
    }

    @Test
    void customConfigOverridesErrorProbability() {
        // Arrange
        Properties config = new Properties();
        config.setProperty("log.normal.error.probability", "1.0");
        LogGenerator generator = new LogGenerator(config);

        // Act
        StreamEvent event = generator.generate();

        // Assert: with 100% error probability, every event should be ERROR or WARN
        Severity severity = event.getLog().getSeverity();
        assertThat(severity).isIn(Severity.ERROR, Severity.WARN);
    }

    @Test
    void logContextIncludesEnvironmentTags() {
        // Arrange
        LogGenerator generator = new LogGenerator();

        // Act
        StreamEvent event = generator.generate();

        // Assert
        assertThat(event.getLog().getContextMap()).containsEntry("pipeline", "streamops");
        assertThat(event.getLog().getContextMap()).containsEntry("env", "dev");
    }
}
