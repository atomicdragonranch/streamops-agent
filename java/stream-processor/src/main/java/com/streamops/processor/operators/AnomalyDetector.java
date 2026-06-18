package com.streamops.processor.operators;

import com.streamops.proto.StreamEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Stateful anomaly detection using Flink keyed state. Tracks per-component
 * running statistics and fires alerts when values exceed thresholds.
 *
 * Uses ValueState to maintain running averages per component/metric pair.
 * When a new metric deviates significantly from the running average, an alert
 * is emitted to the alert topic for the AI agent to investigate.
 *
 * All thresholds are loaded from application.properties, passed in via constructor.
 * Override per-environment using env vars or properties file swap.
 *
 * Thresholds are intentionally simple (static multipliers) because the real
 * intelligence lives in the AI agent layer, not here. This detector catches
 * obvious anomalies; the agent decides what to do about them.
 */
public class AnomalyDetector extends KeyedProcessFunction<String, StreamEvent, String> {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetector.class);

    private final double latencyThresholdMs;
    private final double backpressureThreshold;
    private final double checkpointThresholdMs;
    private final double errorRateThreshold;
    private final double heapThresholdPercent;
    private final double consumerLagThreshold;
    private final double emaAlpha;
    private final long errorRateMinEvents;
    private final int logSeverityThreshold;

    private transient ValueState<Double> runningAvgState;
    private transient ValueState<Long> eventCountState;
    private transient ValueState<Long> errorCountState;

    public AnomalyDetector() {
        this(new Properties());
    }

    public AnomalyDetector(Properties config) {
        this.latencyThresholdMs = doubleProp(config, "anomaly.latency.threshold.ms", 200.0);
        this.backpressureThreshold = doubleProp(config, "anomaly.backpressure.threshold", 0.5);
        this.checkpointThresholdMs = doubleProp(config, "anomaly.checkpoint.threshold.ms", 30000.0);
        this.errorRateThreshold = doubleProp(config, "anomaly.error.rate.threshold", 0.2);
        this.heapThresholdPercent = doubleProp(config, "anomaly.heap.threshold.percent", 85.0);
        this.consumerLagThreshold = doubleProp(config, "anomaly.consumer.lag.threshold", 10000.0);
        this.emaAlpha = doubleProp(config, "anomaly.ema.alpha", 0.1);
        this.errorRateMinEvents = Long.parseLong(config.getProperty("anomaly.error.rate.min.events", "10"));
        this.logSeverityThreshold = Integer.parseInt(config.getProperty("anomaly.log.severity.threshold", "3"));
    }

    private static double doubleProp(Properties config, String key, double defaultValue) {
        return Double.parseDouble(config.getProperty(key, String.valueOf(defaultValue)));
    }

    @Override
    public void open(OpenContext openContext) {
        runningAvgState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("running-avg", Types.DOUBLE));
        eventCountState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("event-count", Types.LONG));
        errorCountState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("error-count", Types.LONG));

        LOG.info("AnomalyDetector initialized: latency={}ms, backpressure={}, checkpoint={}ms, "
                + "errorRate={}, heap={}%, consumerLag={}, emaAlpha={}",
            latencyThresholdMs, backpressureThreshold, checkpointThresholdMs,
            errorRateThreshold, heapThresholdPercent, consumerLagThreshold, emaAlpha);
    }

    @Override
    public void processElement(StreamEvent event, Context ctx, Collector<String> out) throws Exception {
        if (event.hasMetric()) {
            processMetric(event, out);
        } else if (event.hasLog()) {
            processLog(event, out);
        }
    }

    private void processMetric(StreamEvent event, Collector<String> out) throws Exception {
        String metricName = event.getMetric().getMetricName();
        double value = event.getMetric().getValue();
        String component = event.getMetric().getComponent();

        Double runningAvg = runningAvgState.value();
        Long count = eventCountState.value();

        if (runningAvg == null) {
            runningAvgState.update(value);
            eventCountState.update(1L);
            return;
        }

        double newAvg = runningAvg * (1.0 - emaAlpha) + value * emaAlpha;
        runningAvgState.update(newAvg);
        eventCountState.update(count + 1);

        String alert = checkThresholds(metricName, value, newAvg, component, event.getTimestampMs());
        if (alert != null) {
            LOG.info("Anomaly detected: component={}, metric={}, value={}, avg={}",
                component, metricName, value, newAvg);
            out.collect(alert);
        }
    }

    private void processLog(StreamEvent event, Collector<String> out) throws Exception {
        Long total = eventCountState.value();
        Long errors = errorCountState.value();
        if (total == null) total = 0L;
        if (errors == null) errors = 0L;

        total++;
        eventCountState.update(total);

        int severity = event.getLog().getSeverityValue();
        if (severity >= logSeverityThreshold) {
            errors++;
            errorCountState.update(errors);
        }

        if (total > errorRateMinEvents) {
            double errorRate = (double) errors / total;
            if (errorRate > errorRateThreshold) {
                LOG.info("Error rate anomaly: component={}, rate={}, errors={}/{}",
                    event.getSource(), errorRate, errors, total);
                out.collect(buildAlert("error_rate_high", event.getSource(),
                    errorRateThreshold, errorRate, event.getTimestampMs()));
            }
        }
    }

    private String checkThresholds(String metricName, double value, double avg, String component, long timestampMs) {
        return switch (metricName) {
            case "latency_ms" -> value > latencyThresholdMs
                ? buildAlert("latency_spike", component, latencyThresholdMs, value, timestampMs) : null;
            case "backpressure_ratio" -> value > backpressureThreshold
                ? buildAlert("backpressure_high", component, backpressureThreshold, value, timestampMs) : null;
            case "checkpoint_duration_ms" -> value > checkpointThresholdMs
                ? buildAlert("checkpoint_slow", component, checkpointThresholdMs, value, timestampMs) : null;
            case "heap_usage_percent" -> value > heapThresholdPercent
                ? buildAlert("memory_pressure", component, heapThresholdPercent, value, timestampMs) : null;
            case "consumer_lag" -> value > consumerLagThreshold
                ? buildAlert("consumer_lag_high", component, consumerLagThreshold, value, timestampMs) : null;
            default -> null;
        };
    }

    private String buildAlert(String ruleName, String component, double threshold, double actual, long timestampMs) {
        return String.format(
            "{\"rule\":\"%s\",\"component\":\"%s\",\"threshold\":%.2f,\"actual\":%.2f,\"timestamp_ms\":%d,\"severity\":\"CRITICAL\"}",
            ruleName, component, threshold, actual, timestampMs
        );
    }
}
