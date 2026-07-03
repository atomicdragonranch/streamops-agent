package com.streamops.processor.functions;

import com.streamops.proto.StreamEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.Counter;
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
    private static final long ERROR_LOG_EVERY = 100;

    // Registered as a Flink metric in open() so deserialization failures are visible in
    // the Flink UI and metrics pipeline (and alertable), not just buried in worker logs.
    private transient Counter deserializationErrors;

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        deserializationErrors = context.getMetricGroup().counter("deserializationErrors");
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<StreamEvent> out) throws IOException {
        try {
            out.collect(StreamEvent.parseFrom(record.value()));
        } catch (Exception e) {
            long count = 0;
            if (deserializationErrors != null) {
                deserializationErrors.inc();
                count = deserializationErrors.getCount();
            }
            LOG.warn("Failed to deserialize record from partition={} offset={}: {}",
                record.partition(), record.offset(), e.getMessage());
            if (count > 0 && count % ERROR_LOG_EVERY == 0) {
                LOG.error("Deserialization error count reached {}, possible schema mismatch", count);
            }
        }
    }

    @Override
    public TypeInformation<StreamEvent> getProducedType() {
        return TypeInformation.of(StreamEvent.class);
    }
}
