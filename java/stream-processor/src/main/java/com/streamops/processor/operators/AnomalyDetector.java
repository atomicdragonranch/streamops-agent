package com.streamops.processor.operators;

import com.streamops.proto.StreamEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Properties;

/**
 * Stateful anomaly detection over Flink keyed state.
 *
 * The stream is keyed by (source, metric-name) for metrics and (source, "log") for
 * logs, so every metric keeps its OWN baseline instead of blending; latency (~200ms)
 * and consumer-lag (~10k) no longer share a single running average.
 *
 * Two complementary signals fire an alert:
 *   1. Absolute threshold - a known-bad value regardless of history (e.g. latency > 200ms).
 *      Severity CRITICAL.
 *   2. Baseline deviation - a value more than {@code sigma} standard deviations from the
 *      metric's exponentially-weighted moving average, after a warm-up. Catches relative
 *      spikes that stay under the absolute threshold. Severity WARNING.
 *
 * The EWMA mean and variance are maintained incrementally (alpha-weighted update). The
 * heavy reasoning lives in the AI agent that consumes these alerts, not here; this
 * operator surfaces the obvious signals cheaply and in real time.
 */
public class AnomalyDetector extends KeyedProcessFunction<String, StreamEvent, String> {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetector.class);
    private static final double MIN_STDDEV = 1e-9;

    private final double latencyThresholdMs;
    private final double backpressureThreshold;
    private final double checkpointThresholdMs;
    private final double errorRateThreshold;
    private final double heapThresholdPercent;
    private final double consumerLagThreshold;
    private final double emaAlpha;
    private final double baselineSigma;
    private final long warmupSamples;
    private final long errorRateMinEvents;
    private final int logSeverityThreshold;

    private transient ValueState<Double> meanState;
    private transient ValueState<Double> varianceState;
    private transient ValueState<Long> countState;
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
        this.baselineSigma = doubleProp(config, "anomaly.baseline.deviation.sigma", 3.0);
        this.warmupSamples = Long.parseLong(config.getProperty("anomaly.warmup.samples", "20"));
        this.errorRateMinEvents = Long.parseLong(config.getProperty("anomaly.error.rate.min.events", "10"));
        this.logSeverityThreshold = Integer.parseInt(config.getProperty("anomaly.log.severity.threshold", "3"));
    }

    private static double doubleProp(Properties config, String key, double defaultValue) {
        return Double.parseDouble(config.getProperty(key, String.valueOf(defaultValue)));
    }

    /**
     * Partition key: per (source, metric-name) for metrics and per (source, "log") for
     * logs, so each metric keeps its own baseline. Shared by the job wiring and tests.
     */
    public static String keyFor(StreamEvent event) {
        if (event.hasMetric()) {
            return event.getSource() + "|" + event.getMetric().getMetricName();
        }
        if (event.hasLog()) {
            return event.getSource() + "|log";
        }
        return event.getSource() + "|other";
    }

    @Override
    public void open(OpenContext openContext) {
        meanState = getRuntimeContext().getState(new ValueStateDescriptor<>("metric-mean", Types.DOUBLE));
        varianceState = getRuntimeContext().getState(new ValueStateDescriptor<>("metric-variance", Types.DOUBLE));
        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("event-count", Types.LONG));
        errorCountState = getRuntimeContext().getState(new ValueStateDescriptor<>("error-count", Types.LONG));

        LOG.info("AnomalyDetector initialized: latency={}ms, backpressure={}, checkpoint={}ms, "
                + "errorRate={}, heap={}%, consumerLag={}, emaAlpha={}, sigma={}, warmup={}",
            latencyThresholdMs, backpressureThreshold, checkpointThresholdMs,
            errorRateThreshold, heapThresholdPercent, consumerLagThreshold,
            emaAlpha, baselineSigma, warmupSamples);
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
        long ts = event.getTimestampMs();

        // (1) Absolute threshold: known-bad values regardless of the running baseline.
        Double threshold = staticThreshold(metricName);
        if (threshold != null && value > threshold) {
            LOG.info("Threshold anomaly: component={}, metric={}, value={} > {}",
                component, metricName, value, threshold);
            out.collect(buildAlert(metricName + "_threshold", component, threshold, value, ts, "CRITICAL"));
        }

        // (2) Baseline deviation against the metric's own EWMA.
        Double mean = meanState.value();
        long count = orZero(countState.value());

        if (mean == null) {
            // First sample for this (source, metric): seed the baseline, nothing to compare to yet.
            meanState.update(value);
            varianceState.update(0.0);
            countState.update(1L);
            return;
        }

        Double varObj = varianceState.value();
        double variance = varObj == null ? 0.0 : varObj;
        double stddev = Math.sqrt(variance);
        if (count >= warmupSamples && stddev > MIN_STDDEV
                && Math.abs(value - mean) > baselineSigma * stddev) {
            LOG.info("Baseline-deviation anomaly: component={}, metric={}, value={}, mean={}, stddev={}",
                component, metricName, value, mean, stddev);
            out.collect(buildAlert(metricName + "_baseline_deviation", component, mean, value, ts, "WARNING"));
        }

        // Incremental EWMA mean + variance update (alpha-weighted).
        double delta = value - mean;
        meanState.update(mean + emaAlpha * delta);
        varianceState.update((1.0 - emaAlpha) * (variance + emaAlpha * delta * delta));
        countState.update(count + 1);
    }

    private void processLog(StreamEvent event, Collector<String> out) throws Exception {
        long total = orZero(countState.value()) + 1;
        countState.update(total);

        long errors = orZero(errorCountState.value());
        if (event.getLog().getSeverityValue() >= logSeverityThreshold) {
            errors++;
            errorCountState.update(errors);
        }

        if (total > errorRateMinEvents) {
            double errorRate = (double) errors / total;
            if (errorRate > errorRateThreshold) {
                LOG.info("Error-rate anomaly: component={}, rate={}, errors={}/{}",
                    event.getSource(), errorRate, errors, total);
                out.collect(buildAlert("error_rate_high", event.getSource(),
                    errorRateThreshold, errorRate, event.getTimestampMs(), "CRITICAL"));
            }
        }
    }

    private Double staticThreshold(String metricName) {
        return switch (metricName) {
            case "latency_ms" -> latencyThresholdMs;
            case "backpressure_ratio" -> backpressureThreshold;
            case "checkpoint_duration_ms" -> checkpointThresholdMs;
            case "heap_usage_percent" -> heapThresholdPercent;
            case "consumer_lag" -> consumerLagThreshold;
            default -> null;
        };
    }

    private static long orZero(Long v) {
        return v == null ? 0L : v;
    }

    private static String buildAlert(String rule, String component, double threshold,
                                     double actual, long timestampMs, String severity) {
        return String.format(Locale.ROOT,
            "{\"rule\":\"%s\",\"component\":\"%s\",\"threshold\":%.2f,\"actual\":%.2f,"
                + "\"timestamp_ms\":%d,\"severity\":\"%s\"}",
            jsonEscape(rule), jsonEscape(component), threshold, actual, timestampMs, severity);
    }

    // Escape the only two characters that can break a JSON string literal, so a component
    // or rule name containing a quote/backslash can't produce malformed alert JSON.
    private static String jsonEscape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
