---
anomaly_type: error_burst
title: Out-of-Order Events / Error Burst Investigation
---

## Symptoms

- Late events dropped or sent to side output
- Window results incorrect or incomplete
- Error rate spikes in event processing
- Watermark advancing past expected event timestamps

## Common Causes

1. **Clock skew**: event producers have unsynchronized clocks
2. **Reprocessing**: replay or rewind causes old events to arrive after watermark
3. **Network delays**: variable latency between producer and Kafka broker
4. **Partition reassignment**: consumer rebalance causes out-of-order delivery
5. **Watermark misconfiguration**: allowed lateness too low for actual event distribution

## Diagnostic Steps

1. Check error rate and type via `query_flink_jobs` (exceptions endpoint)
2. Query late event count: `flink_taskmanager_job_task_operator_numLateRecordsDropped`
3. Compare event timestamps to processing timestamps to measure skew
4. Check if errors are concentrated on specific partitions or time windows
5. Review watermark configuration (allowed lateness, idle source detection)
6. Check Kafka consumer group for recent rebalance events

## Resolution Options

- **Clock skew**: enforce NTP sync on producers, use ingestion time as fallback
- **Reprocessing**: increase allowed lateness window during replay scenarios
- **Network delays**: increase max out-of-orderness in watermark strategy
- **Partition reassignment**: use sticky partition assignment, increase session timeout
- **Watermark config**: tune BoundedOutOfOrdernessWatermarks, add idle source detection
