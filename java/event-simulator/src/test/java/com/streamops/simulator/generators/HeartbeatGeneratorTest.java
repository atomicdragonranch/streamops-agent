package com.streamops.simulator.generators;

import com.streamops.proto.ComponentStatus;
import com.streamops.proto.StreamEvent;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HeartbeatGeneratorTest {

    @Test
    void generateProducesValidHeartbeat() {
        // Arrange
        HeartbeatGenerator generator = new HeartbeatGenerator();

        // Act
        StreamEvent event = generator.generate();

        // Assert
        assertThat(event.hasHeartbeat()).isTrue();
        assertThat(event.getEventId()).isNotBlank();
        assertThat(event.getHeartbeat().getComponent()).isNotBlank();
        assertThat(event.getHeartbeat().getUptimeMs()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void defaultStatusIsHealthy() {
        // Arrange
        HeartbeatGenerator generator = new HeartbeatGenerator();

        // Act
        StreamEvent event = generator.generate();

        // Assert
        assertThat(event.getHeartbeat().getStatus()).isEqualTo(ComponentStatus.HEALTHY);
    }

    @Test
    void statusOverrideApplies() {
        // Arrange
        HeartbeatGenerator generator = new HeartbeatGenerator();
        generator.setComponentStatus("kafka-consumer", ComponentStatus.DEGRADED);

        // Act: generate until we hit kafka-consumer
        ComponentStatus status = null;
        for (int i = 0; i < 100; i++) {
            StreamEvent event = generator.generate();
            if ("kafka-consumer".equals(event.getHeartbeat().getComponent())) {
                status = event.getHeartbeat().getStatus();
                break;
            }
        }

        // Assert
        assertThat(status).isEqualTo(ComponentStatus.DEGRADED);
    }

    @Test
    void clearOverridesRestoresHealthy() {
        // Arrange
        HeartbeatGenerator generator = new HeartbeatGenerator();
        generator.setComponentStatus("kafka-consumer", ComponentStatus.UNHEALTHY);

        // Act
        generator.clearStatusOverrides();

        // Assert: find kafka-consumer, should be back to HEALTHY
        for (int i = 0; i < 100; i++) {
            StreamEvent event = generator.generate();
            if ("kafka-consumer".equals(event.getHeartbeat().getComponent())) {
                assertThat(event.getHeartbeat().getStatus()).isEqualTo(ComponentStatus.HEALTHY);
                return;
            }
        }
    }

    @Test
    void heartbeatIncludesMetadata() {
        // Arrange
        HeartbeatGenerator generator = new HeartbeatGenerator();

        // Act
        StreamEvent event = generator.generate();

        // Assert
        assertThat(event.getHeartbeat().getMetadataMap()).containsKey("jvm_version");
        assertThat(event.getHeartbeat().getMetadataMap()).containsKey("flink_version");
        assertThat(event.getHeartbeat().getMetadataMap()).containsKey("pid");
    }
}
