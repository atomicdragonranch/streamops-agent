# Example Scenario Outputs

These are representative examples of what the StreamOps Agent produces for each anomaly scenario. Actual outputs vary based on the agent's investigation and the state of the infrastructure at the time.

| Scenario | Severity | Description |
|----------|----------|-------------|
| latency-spike | HIGH | Processing latency 10-100x above threshold due to GC pressure |
| throughput-drop | HIGH | 99% throughput reduction from upstream producer failure |
| error-burst | MEDIUM | 60% error rate from cascading exceptions |
| backpressure | HIGH | Sink bottleneck causing upstream throttling |
| checkpoint-timeout | CRITICAL | Checkpoint failures with data loss risk |
| memory-pressure | HIGH | Heap at 94% with OOM risk |

To generate live outputs, run:

```bash
python scripts/demo_scenario.py latency-spike
python scripts/demo_scenario.py --all
```
