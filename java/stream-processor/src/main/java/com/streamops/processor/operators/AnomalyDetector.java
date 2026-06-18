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

/**
 * Stateful anomaly detection using Flink keyed state. Tracks per-component
 * running statistics and fires alerts when values exceed thresholds.
 *
 * Uses ValueState to maintain running averages per component/metric pair.
 * When a new metric deviates significantly from the running average, an alert
 * is emitted to the alert topic for the AI agent to investigate.
 *
 * Thresholds are intentionally simple (static multipliers) because the real
 * intelligence lives in the AI agent layer, not here. This detector catches
 * obvious anomalies; the agent decides what to do about them.
 */
public class AnomalyDetector extends KeyedProcessFunction<String, StreamEvent, String> {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetector.class);

    private static final double LATENCY_THRESHOLD_MS = 200.0;
    private static final double BACKPRESSURE_THRESHOLD = 0.5;
    private static final double CHECKPOINT_THRESHOLD_MS = 30000.0;
    private static final double ERROR_RATE_THRESHOLD = 0.2;
    private static final double HEAP_THRESHOLD_PERCENT = 85.0;
    private static final double CONSUMER_LAG_THRESHOLD = 10000.0;

    private transient ValueState<Double> runningAvgState;
    private transient ValueState<Long> eventCountState;
    private transient ValueState<Long> errorCountState;

    @Override
    public void open(OpenContext openContext) {
        runningAvgState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("running-avg", Types.DOUBLE));
        eventCountState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("event-count", Types.LONG));
        errorCountState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("error-count", Types.LONG));

        LOG.info("AnomalyDetector initialized with thresholds: latency={}ms, backpressure={}, checkpoint={}ms",
            LATENCY_THRESHOLD_MS, BACKPRESSURE_THRESHOLD, CHECKPOINT_THRESHOLD_MS);
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

        // Exponential moving average (alpha=0.1) to smooth out noise
        double newAvg = runningAvg * 0.9 + value * 0.1;
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
        if (severity >= 3) { // WARN or above
            errors++;
            errorCountState.update(errors);
        }

        if (total > 10) {
            double errorRate = (double) errors / total;
            if (errorRate > ERROR_RATE_THRESHOLD) {
                LOG.info("Error rate anomaly: component={}, rate={}, errors={}/{}",
                    event.getSource(), errorRate, errors, total);
                out.collect(buildAlert("error_rate_high", event.getSource(),
                    ERROR_RATE_THRESHOLD, errorRate, event.getTimestampMs()));
            }
        }
    }

    private String checkThresholds(String metricName, double value, double avg, String component, long timestampMs) {
        return switch (metricName) {
            case "latency_ms" -> value > LATENCY_THRESHOLD_MS
                ? buildAlert("latency_spike", component, LATENCY_THRESHOLD_MS, value, timestampMs) : null;
            case "backpressure_ratio" -> value > BACKPRESSURE_THRESHOLD
                ? buildAlert("backpressure_high", component, BACKPRESSURE_THRESHOLD, value, timestampMs) : null;
            case "checkpoint_duration_ms" -> value > CHECKPOINT_THRESHOLD_MS
                ? buildAlert("checkpoint_slow", component, CHECKPOINT_THRESHOLD_MS, value, timestampMs) : null;
            case "heap_usage_percent" -> value > HEAP_THRESHOLD_PERCENT
                ? buildAlert("memory_pressure", component, HEAP_THRESHOLD_PERCENT, value, timestampMs) : null;
            case "consumer_lag" -> value > CONSUMER_LAG_THRESHOLD
                ? buildAlert("consumer_lag_high", component, CONSUMER_LAG_THRESHOLD, value, timestampMs) : null;
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
