package com.streamops.processor;

import com.streamops.processor.functions.StreamEventDeserializer;
import com.streamops.processor.operators.AnomalyDetector;
import com.streamops.processor.operators.MetricAggregator;
import com.streamops.proto.AlertEvent;
import com.streamops.proto.StreamEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Flink job entry point. Consumes StreamEvents from Kafka, runs two parallel
 * processing branches:
 *
 *   1. MetricAggregator: 30s tumbling windows, computes per-component stats
 *   2. AnomalyDetector:  10s sliding windows, threshold-based anomaly detection
 *
 * Alerts flow to a separate Kafka topic for the AI agent to consume.
 *
 * This job is submitted to a Flink cluster (not run standalone). The Flink runtime
 * provides the execution environment; dependencies are "provided" scope in Maven.
 */
public class StreamProcessorApp {

    private static final Logger LOG = LoggerFactory.getLogger(StreamProcessorApp.class);

    private static final String DEFAULT_INPUT_TOPIC = "stream-events";
    private static final String DEFAULT_ALERT_TOPIC = "stream-alerts";
    private static final String DEFAULT_BOOTSTRAP = "localhost:9092";
    private static final String DEFAULT_GROUP_ID = "streamops-processor";

    public static void main(String[] args) throws Exception {
        String bootstrap = envOrDefault("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP);
        String inputTopic = envOrDefault("KAFKA_INPUT_TOPIC", DEFAULT_INPUT_TOPIC);
        String alertTopic = envOrDefault("KAFKA_ALERT_TOPIC", DEFAULT_ALERT_TOPIC);
        String groupId = envOrDefault("KAFKA_GROUP_ID", DEFAULT_GROUP_ID);

        LOG.info("Configuring StreamProcessor: bootstrap={}, input={}, alerts={}, group={}",
            bootstrap, inputTopic, alertTopic, groupId);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        KafkaSource<StreamEvent> source = KafkaSource.<StreamEvent>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(inputTopic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new StreamEventDeserializer())
            .build();

        // Event-time watermarks with 5s tolerance for out-of-order events.
        // Streaming infrastructure metrics can arrive slightly late due to
        // batching in the simulator or network jitter.
        WatermarkStrategy<StreamEvent> watermarkStrategy = WatermarkStrategy
            .<StreamEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> event.getTimestampMs());

        DataStream<StreamEvent> events = env
            .fromSource(source, watermarkStrategy, "kafka-source")
            .uid("kafka-source")
            .name("StreamEvents from Kafka");

        LOG.info("Building processing topology: aggregation (30s windows) + anomaly detection (10s windows)");

        // Branch 1: Aggregate metrics per component in 30s windows
        events
            .filter(e -> e.hasMetric())
            .uid("metric-filter")
            .name("Filter Metrics")
            .keyBy(e -> e.getMetric().getComponent())
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(30)))
            .process(new MetricAggregator())
            .uid("metric-aggregator")
            .name("30s Metric Aggregation");

        // Branch 2: Detect anomalies and emit alerts
        DataStream<String> alerts = events
            .keyBy(StreamEvent::getSource)
            .process(new AnomalyDetector())
            .uid("anomaly-detector")
            .name("Anomaly Detector");

        KafkaSink<String> alertSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
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

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null && !value.isBlank() ? value : defaultValue;
    }
}
