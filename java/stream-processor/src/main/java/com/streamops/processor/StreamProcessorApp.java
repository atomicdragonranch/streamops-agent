package com.streamops.processor;

import com.streamops.processor.functions.ProtobufKryoSerializer;
import com.streamops.processor.functions.StreamEventDeserializer;
import com.streamops.processor.operators.AnomalyDetector;
import com.streamops.proto.StreamEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

/**
 * Flink job entry point. Consumes StreamEvents from Kafka and runs the
 * AnomalyDetector: a keyed process function that fires alerts on absolute
 * thresholds and on EWMA-baseline deviation, keyed per (source, metric) so each
 * metric keeps its own baseline. Alerts flow to a separate Kafka topic for the
 * AI agent to consume.
 *
 * This job is submitted to a Flink cluster (not run standalone). The Flink runtime
 * provides the execution environment; dependencies are "provided" scope in Maven.
 *
 * Configuration: application.properties on classpath, overridable via env vars.
 */
public class StreamProcessorApp {

    private static final Logger LOG = LoggerFactory.getLogger(StreamProcessorApp.class);

    public static void main(String[] args) throws Exception {
        Properties config = loadConfig();

        String bootstrap = resolve(config, "kafka.bootstrap", "KAFKA_BOOTSTRAP");
        String inputTopic = resolve(config, "kafka.input.topic", "KAFKA_INPUT_TOPIC");
        String alertTopic = resolve(config, "kafka.alert.topic", "KAFKA_ALERT_TOPIC");
        String groupId = resolve(config, "kafka.group.id", "KAFKA_GROUP_ID");
        long checkpointInterval = Long.parseLong(config.getProperty("flink.checkpoint.interval.ms", "30000"));
        int watermarkTolerance = Integer.parseInt(config.getProperty("flink.watermark.max.out.of.orderness.seconds", "5"));

        LOG.info("Configuring StreamProcessor: bootstrap={}, input={}, alerts={}, group={}, checkpoint={}ms",
            bootstrap, inputTopic, alertTopic, groupId, checkpointInterval);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);
        ((SerializerConfigImpl) env.getConfig().getSerializerConfig())
                .registerTypeWithKryoSerializer(StreamEvent.class, ProtobufKryoSerializer.class);

        KafkaSource<StreamEvent> source = KafkaSource.<StreamEvent>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(inputTopic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new StreamEventDeserializer())
            .build();

        // Streaming infrastructure metrics can arrive slightly late due to
        // batching in the simulator or network jitter.
        WatermarkStrategy<StreamEvent> watermarkStrategy = WatermarkStrategy
            .<StreamEvent>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkTolerance))
            .withTimestampAssigner((event, timestamp) -> event.getTimestampMs());

        DataStream<StreamEvent> events = env
            .fromSource(source, watermarkStrategy, "kafka-source")
            .uid("kafka-source")
            .name("StreamEvents from Kafka");

        LOG.info("Building processing topology: anomaly detection");

        // Detect anomalies and emit alerts. Key per (source, metric) so each metric
        // maintains its own baseline; logs are keyed per source for error-rate tracking.
        DataStream<String> alerts = events
            .keyBy(AnomalyDetector::keyFor)
            .process(new AnomalyDetector(config))
            .uid("anomaly-detector")
            .name("Anomaly Detector");

        KafkaSink<String> alertSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            // At-least-once so alerts aren't dropped on failure/restart (checkpointing is on).
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(alertTopic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .build();

        alerts
            .sinkTo(alertSink)
            .uid("alert-sink")
            .name("Alerts to Kafka");

        LOG.info("Submitting job: StreamOps Processor");
        env.execute("StreamOps Processor");
    }

    private static Properties loadConfig() {
        Properties props = new Properties();
        try (InputStream is = StreamProcessorApp.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (is != null) {
                props.load(is);
            } else {
                LOG.warn("application.properties not found on classpath, using defaults");
            }
        } catch (IOException e) {
            LOG.warn("Failed to load application.properties: {}", e.getMessage());
        }
        return props;
    }

    /**
     * Resolve a config value: env var takes precedence over properties file.
     */
    private static String resolve(Properties config, String propKey, String envKey) {
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }
        return config.getProperty(propKey);
    }
}
