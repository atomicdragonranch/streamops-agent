package com.streamops.processor.functions;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.streamops.proto.MetricEvent;
import com.streamops.proto.StreamEvent;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class ProtobufKryoSerializerTest {

    @Test
    void roundTripPreservesMessageIncludingMapField() {
        // Arrange: a StreamEvent with a map field (the case that breaks Kryo's default serializer)
        StreamEvent original = StreamEvent.newBuilder()
            .setEventId("evt-42")
            .setTimestampMs(1718712000000L)
            .setSource("flink-operator")
            .setMetric(MetricEvent.newBuilder()
                .setMetricName("consumer_lag")
                .setValue(45000.0)
                .setUnit("records")
                .setComponent("kafka-consumer")
                .putTags("partition", "2")
                .build())
            .build();
        ProtobufKryoSerializer<StreamEvent> serializer = new ProtobufKryoSerializer<>();

        // Act: write then read back through Kryo I/O
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        serializer.write(null, output, original);
        output.flush();
        StreamEvent restored;
        try (Input input = new Input(baos.toByteArray())) {
            restored = serializer.read(null, input, StreamEvent.class);
        }

        // Assert: the message survives protobuf-over-Kryo intact, map field included
        assertThat(restored).isEqualTo(original);
        assertThat(restored.getMetric().getTagsMap()).containsEntry("partition", "2");
    }

    @Test
    void twoMessagesInSequenceRoundTrip() {
        // Arrange: length-prefixing must let consecutive messages be read back in order
        StreamEvent a = StreamEvent.newBuilder().setEventId("a").setSource("s1").build();
        StreamEvent b = StreamEvent.newBuilder().setEventId("b").setSource("s2").build();
        ProtobufKryoSerializer<StreamEvent> serializer = new ProtobufKryoSerializer<>();

        // Act
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        serializer.write(null, output, a);
        serializer.write(null, output, b);
        output.flush();
        StreamEvent ra;
        StreamEvent rb;
        try (Input input = new Input(baos.toByteArray())) {
            ra = serializer.read(null, input, StreamEvent.class);
            rb = serializer.read(null, input, StreamEvent.class);
        }

        // Assert
        assertThat(ra.getEventId()).isEqualTo("a");
        assertThat(rb.getEventId()).isEqualTo("b");
    }
}
