package com.streamops.processor.functions;

import com.streamops.proto.MetricEvent;
import com.streamops.proto.StreamEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StreamEventDeserializerTest {

    @Test
    void validProtobufDeserializesSuccessfully() throws Exception {
        // Arrange
        StreamEvent original = StreamEvent.newBuilder()
            .setEventId("test-001")
            .setTimestampMs(1718712000000L)
            .setSource("kafka-consumer")
            .setMetric(MetricEvent.newBuilder()
                .setMetricName("latency_ms")
                .setValue(42.5)
                .setUnit("ms")
                .setComponent("kafka-consumer")
                .build())
            .build();

        byte[] serialized = original.toByteArray();
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 0L, null, serialized);

        StreamEventDeserializer deserializer = new StreamEventDeserializer();
        List<StreamEvent> collected = new ArrayList<>();

        // Act
        deserializer.deserialize(record, new ListCollector<>(collected));

        // Assert
        assertThat(collected).hasSize(1);
        StreamEvent result = collected.get(0);
        assertThat(result.getEventId()).isEqualTo("test-001");
        assertThat(result.getMetric().getMetricName()).isEqualTo("latency_ms");
        assertThat(result.getMetric().getValue()).isEqualTo(42.5);
    }

    @Test
    void invalidBytesAreSkippedGracefully() throws Exception {
        // Arrange
        byte[] garbage = "this is not protobuf".getBytes();
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 0L, null, garbage);

        StreamEventDeserializer deserializer = new StreamEventDeserializer();
        List<StreamEvent> collected = new ArrayList<>();

        // Act
        deserializer.deserialize(record, new ListCollector<>(collected));

        // Assert: garbage is dropped, not propagated
        assertThat(collected).isEmpty();
    }

    @Test
    void emptyBytesAreHandled() throws Exception {
        // Arrange
        byte[] empty = new byte[0];
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 0L, null, empty);

        StreamEventDeserializer deserializer = new StreamEventDeserializer();
        List<StreamEvent> collected = new ArrayList<>();

        // Act
        deserializer.deserialize(record, new ListCollector<>(collected));

        // Assert: empty protobuf parses as default instance (all fields unset but valid)
        assertThat(collected).hasSize(1);
        assertThat(collected.get(0).getEventId()).isEmpty();
    }

    @Test
    void producedTypeIsStreamEvent() {
        // Arrange
        StreamEventDeserializer deserializer = new StreamEventDeserializer();

        // Act
        var typeInfo = deserializer.getProducedType();

        // Assert
        assertThat(typeInfo.getTypeClass()).isEqualTo(StreamEvent.class);
    }

    /**
     * Simple Collector implementation for unit tests.
     */
    private static class ListCollector<T> implements Collector<T> {
        private final List<T> list;

        ListCollector(List<T> list) {
            this.list = list;
        }

        @Override
        public void collect(T record) {
            list.add(record);
        }

        @Override
        public void close() {}
    }
}
