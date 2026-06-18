package com.streamops.processor.operators;

import com.streamops.proto.StreamEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregates metrics within 30s tumbling windows, grouped by component.
 * Outputs per-metric-name statistics (min, max, avg, count) for each window.
 *
 * This is the "steady state" processing branch. The output feeds dashboards
 * and gives the AI agent baseline data to compare against anomaly alerts.
 */
public class MetricAggregator extends ProcessWindowFunction<StreamEvent, String, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(MetricAggregator.class);

    @Override
    public void process(String component, Context context, Iterable<StreamEvent> events, Collector<String> out) {
        Map<String, DoubleSummaryStatistics> statsByMetric = new HashMap<>();
        int eventCount = 0;

        for (StreamEvent event : events) {
            if (event.hasMetric()) {
                String metricName = event.getMetric().getMetricName();
                statsByMetric.computeIfAbsent(metricName, k -> new DoubleSummaryStatistics())
                    .accept(event.getMetric().getValue());
                eventCount++;
            }
        }

        long windowStart = context.window().getStart();
        long windowEnd = context.window().getEnd();

        LOG.debug("Window [{}, {}] component={}: {} events across {} metric types",
            windowStart, windowEnd, component, eventCount, statsByMetric.size());

        for (Map.Entry<String, DoubleSummaryStatistics> entry : statsByMetric.entrySet()) {
            DoubleSummaryStatistics stats = entry.getValue();
            String result = String.format(
                "{\"window_start\":%d,\"window_end\":%d,\"component\":\"%s\",\"metric\":\"%s\"," +
                "\"count\":%d,\"min\":%.2f,\"max\":%.2f,\"avg\":%.2f}",
                windowStart, windowEnd, component, entry.getKey(),
                stats.getCount(), stats.getMin(), stats.getMax(), stats.getAverage()
            );
            out.collect(result);
        }
    }
}
