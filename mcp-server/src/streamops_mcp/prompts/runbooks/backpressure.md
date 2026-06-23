---
anomaly_type: backpressure
title: Backpressure Investigation
---

## Symptoms

- Backpressure ratio > 0.5 on one or more operators
- Upstream operators idle while downstream is saturated
- Checkpoint duration increases (barrier alignment delayed)

## Common Causes

1. **Slow operator**: one operator in the DAG is the bottleneck (e.g., windowed aggregation, external lookup)
2. **Sink saturation**: target system (database, API, Kafka topic) cannot keep up
3. **Data skew**: one subtask receives disproportionate traffic
4. **Insufficient resources**: CPU or memory constraint on TaskManager
5. **Serialization overhead**: large or complex record serialization consuming CPU

## Diagnostic Steps

1. Query Flink REST API for per-operator backpressure ratios
2. Identify the first operator in the chain showing high backpressure (the bottleneck)
3. Check if backpressure is uniform across subtasks or skewed
4. Query `flink_taskmanager_job_task_busyTimeMsPerSecond` per operator
5. Check sink throughput and error rates
6. Review TaskManager CPU and memory via Prometheus

## Resolution Options

- **Slow operator**: optimize logic, increase parallelism for that operator only
- **Sink saturation**: batch writes, add connection pooling, scale sink target
- **Data skew**: rekey with better hash, use rebalance() before bottleneck
- **Resources**: increase TaskManager slots, CPU, or memory allocation
- **Serialization**: use more efficient serializer (Avro, Protobuf over JSON)
