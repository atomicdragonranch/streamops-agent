---
anomaly_type: checkpoint_failure
title: Checkpoint Failure Investigation
---

## Symptoms

- Checkpoint status shows FAILED or timeout
- Checkpoint duration exceeds configured interval
- State size growing unbounded
- Recovery time after restarts increasing

## Common Causes

1. **State size explosion**: unbounded state from missing TTL or retention config
2. **I/O bottleneck**: disk throughput insufficient for state backend writes
3. **Barrier alignment timeout**: slow subtask holds up checkpoint barrier
4. **Network partition**: TaskManagers unable to reach JobManager during snapshot
5. **RocksDB compaction**: background compaction competing with checkpoint I/O

## Diagnostic Steps

1. Get checkpoint history via `get_checkpoint_stats` to identify failure pattern
2. Check checkpoint size trend: is state growing monotonically?
3. Query `flink_jobmanager_job_lastCheckpointDuration` and `lastCheckpointSize`
4. Identify the slowest subtask in checkpoint acknowledgment
5. Check TaskManager disk I/O metrics and available space
6. Review RocksDB metrics if using RocksDB state backend

## Resolution Options

- **State size**: configure state TTL, add cleanup timers to stateful operators
- **I/O bottleneck**: move state backend to SSD, increase I/O threads
- **Barrier alignment**: enable unaligned checkpoints (Flink 1.11+)
- **Network issues**: check connectivity, increase checkpoint timeout
- **RocksDB**: tune compaction, increase write buffer count, enable incremental checkpoints
