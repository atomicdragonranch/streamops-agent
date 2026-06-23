---
anomaly_type: throughput_drop
title: Consumer Lag / Throughput Drop Investigation
---

## Symptoms

- Consumer group lag increasing over time
- Throughput (records/sec) drops below baseline
- Partition offsets falling behind high watermark

## Common Causes

1. **Insufficient parallelism**: consumer count lower than partition count
2. **Slow processing**: per-record processing time increased (new logic, external calls)
3. **Producer burst**: upstream spike in event production rate
4. **Rebalance storm**: frequent consumer group rebalances causing stop-the-world pauses
5. **Deserialization errors**: bad records causing retries or dead-letter routing

## Diagnostic Steps

1. Check consumer lag per partition via `get_consumer_lag`
2. Compare current throughput to baseline via `query_metrics`
3. Check if lag is uniform or concentrated on specific partitions
4. Look for consumer group rebalance events in Kafka logs
5. Check Flink source operator backpressure
6. Review event deserialization error rate

## Resolution Options

- **Parallelism**: increase Flink source parallelism to match partition count
- **Slow processing**: profile the pipeline, optimize hot path, add async I/O
- **Producer burst**: scale consumers or add buffering (increase max.poll.records)
- **Rebalance storm**: increase session.timeout.ms, use static group membership
- **Deserialization**: fix schema, add dead-letter queue for poison pills
