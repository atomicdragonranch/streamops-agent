package com.streamops.processor.functions;

import com.streamops.proto.StreamEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes raw Kafka bytes into Protobuf StreamEvent objects.
 *
 * Using Protobuf's parseFrom directly rather than Confluent's Schema Registry
 * deserializer. The schema is compiled into both the simulator and processor,
 * so there's no need for a registry in the dev stack. A production deployment
 * would swap this for a registry-backed deserializer.
 */
public class StreamEventDeserializer implements KafkaRecordDeserializationSchema<StreamEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamEventDeserializer.class);

    private transient long deserializationErrors = 0;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<StreamEvent> out) throws IOException {
        try {
            StreamEvent event = StreamEvent.parseFrom(record.value());
            out.collect(event);
        } catch (Exception e) {
            deserializationErrors++;
            LOG.warn("Failed to deserialize record from partition={} offset={}: {}",
                record.partition(), record.offset(), e.getMessage());
            if (deserializationErrors % 100 == 0) {
                LOG.error("Deserialization error count reached {}, possible schema mismatch",
                    deserializationErrors);
            }
        }
    }

    @Override
    public TypeInformation<StreamEvent> getProducedType() {
        return TypeInformation.of(StreamEvent.class);
    }
}
