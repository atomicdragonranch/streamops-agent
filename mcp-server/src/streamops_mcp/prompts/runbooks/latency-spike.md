---
anomaly_type: latency_spike
title: Latency Spike Investigation
---

## Symptoms

- Processing latency exceeds SLA thresholds
- End-to-end event delivery time increases
- Watermark lag grows

## Common Causes

1. **GC pressure**: TaskManager heap near capacity causes stop-the-world pauses
2. **Backpressure propagation**: downstream operator bottleneck backs up the pipeline
3. **Skewed partitions**: one partition carries disproportionate load
4. **External dependency slowdown**: sink database or API response time degraded
5. **Checkpoint interference**: large state checkpoints competing for I/O

## Diagnostic Steps

1. Check Flink job status and task metrics via `query_flink_jobs`
2. Query Prometheus for `flink_taskmanager_job_task_operator_processing_latency`
3. Check GC metrics: `jvm_gc_pause_seconds_sum`, heap utilization
4. Inspect backpressure ratios per operator via Flink REST API
5. Check consumer lag via `get_consumer_lag` to identify partition skew
6. Review checkpoint duration and size via `get_checkpoint_stats`

## Resolution Options

- **GC pressure**: increase TaskManager heap or tune GC (G1GC region size)
- **Backpressure**: scale out the bottleneck operator (increase parallelism)
- **Partition skew**: rekey the stream with better distribution
- **External dependency**: add circuit breaker or increase sink timeout
- **Checkpoint interference**: enable incremental checkpoints, tune interval
