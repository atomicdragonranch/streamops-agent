package com.streamops.processor.operators;

import com.streamops.proto.LogEvent;
import com.streamops.proto.MetricEvent;
import com.streamops.proto.Severity;
import com.streamops.proto.StreamEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Behavioral tests for the AnomalyDetector keyed process function, driven through
 * Flink's operator test harness so keyed state, keying, and alert emission are all
 * exercised for real (not just construction).
 */
class AnomalyDetectorHarnessTest {

    private static final KeySelector<StreamEvent, String> KEY = AnomalyDetector::keyFor;

    private KeyedOneInputStreamOperatorTestHarness<String, StreamEvent, String> open(Properties props)
            throws Exception {
        KeyedProcessFunction<String, StreamEvent, String> fn = new AnomalyDetector(props);
        var harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(fn, KEY, Types.STRING);
        harness.open();
        return harness;
    }

    private static Properties fastWarmup() {
        Properties p = new Properties();
        p.setProperty("anomaly.warmup.samples", "5");
        p.setProperty("anomaly.baseline.deviation.sigma", "3.0");
        return p;
    }

    private static StreamEvent metric(String source, String name, double value) {
        return StreamEvent.newBuilder().setSource(source).setTimestampMs(1000L)
            .setMetric(MetricEvent.newBuilder()
                .setMetricName(name).setValue(value).setComponent(source).build())
            .build();
    }

    private static StreamEvent log(String source, Severity severity) {
        return StreamEvent.newBuilder().setSource(source).setTimestampMs(1000L)
            .setLog(LogEvent.newBuilder().setSeverity(severity).setMessage("m").setComponent(source).build())
            .build();
    }

    @Test
    void absoluteThresholdBreachEmitsCriticalAlert() throws Exception {
        // Arrange
        var harness = open(new Properties());

        // Act: latency far above the 200ms absolute threshold
        harness.processElement(metric("kafka-consumer", "latency_ms", 5000.0), 1L);

        // Assert
        List<String> out = harness.extractOutputValues();
        assertThat(out).hasSize(1);
        assertThat(out.get(0)).contains("latency_ms_threshold").contains("\"severity\":\"CRITICAL\"");
        harness.close();
    }

    @Test
    void valueBelowThresholdButFarFromBaselineEmitsDeviationWarning() throws Exception {
        // Arrange: warm a stable-ish baseline near 100, all well under the 200ms threshold
        var harness = open(fastWarmup());
        for (double v : new double[]{98, 102, 99, 101, 100, 98, 102, 100}) {
            harness.processElement(metric("op", "latency_ms", v), 1L);
        }
        int before = harness.extractOutputValues().size();

        // Act: a spike still under 200ms but far above the ~100 baseline
        harness.processElement(metric("op", "latency_ms", 199.0), 1L);

        // Assert: a baseline-deviation WARNING (not a threshold alert), and only one new alert
        List<String> out = harness.extractOutputValues();
        assertThat(out).hasSize(before + 1);
        assertThat(out.get(out.size() - 1))
            .contains("latency_ms_baseline_deviation").contains("\"severity\":\"WARNING\"");
        harness.close();
    }

    @Test
    void valueNearBaselineDoesNotAlert() throws Exception {
        // Arrange
        var harness = open(fastWarmup());
        for (double v : new double[]{98, 102, 99, 101, 100, 98, 102, 100}) {
            harness.processElement(metric("op", "latency_ms", v), 1L);
        }
        int before = harness.extractOutputValues().size();

        // Act: a value right at the baseline
        harness.processElement(metric("op", "latency_ms", 100.0), 1L);

        // Assert: nothing new
        assertThat(harness.extractOutputValues()).hasSize(before);
        harness.close();
    }

    @Test
    void perMetricBaselinesAreIsolated() throws Exception {
        // Arrange: one source emits two metrics at very different scales, interleaved.
        // Under the old source-only keying their baselines blended into one average.
        var harness = open(fastWarmup());
        for (int i = 0; i < 8; i++) {
            harness.processElement(metric("op", "latency_ms", 100.0 + (i % 2)), 1L);        // ~100
            harness.processElement(metric("op", "consumer_lag", 5000.0 + (i % 2) * 10), 1L); // ~5000
        }
        int before = harness.extractOutputValues().size();

        // Act: a consumer_lag value normal for ITS OWN baseline (~5000) and under its 10k threshold
        harness.processElement(metric("op", "consumer_lag", 5010.0), 1L);

        // Assert: no alert; the lag baseline was not polluted by the latency values
        assertThat(harness.extractOutputValues()).hasSize(before);
        harness.close();
    }

    @Test
    void logErrorRateAboveThresholdEmitsAlert() throws Exception {
        // Arrange: defaults (min.events 10, error.rate.threshold 0.2)
        var harness = open(new Properties());

        // Act: 12 ERROR logs -> total > 10 and error rate 1.0 > 0.2
        for (int i = 0; i < 12; i++) {
            harness.processElement(log("worker", Severity.ERROR), 1L);
        }

        // Assert
        List<String> out = harness.extractOutputValues();
        assertThat(out).isNotEmpty();
        assertThat(out.get(out.size() - 1)).contains("error_rate_high").contains("\"severity\":\"CRITICAL\"");
        harness.close();
    }
}
