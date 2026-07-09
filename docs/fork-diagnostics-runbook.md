# Fork-style parallel diagnostic exploration: manual test runbook

Companion to the fork-diagnostics feature (issue #67). This walks through
running the coordinator in multi-fork mode, what to expect in the logs and
audit trail, and how to prove the diagnosis actually fanned out in parallel
rather than running one agent.

## What the feature does

By default the coordinator runs a single diagnostic sub-agent per cycle
(`detect -> diagnose -> report`). One agent is a single line of reasoning,
prone to tunnel vision on an ambiguous anomaly with several plausible causes.

In multi-fork mode the coordinator spawns **N diagnostic sub-agents
concurrently** from the same detected anomaly, each seeded with a distinct
hypothesis, then **merges the survivors** into one diagnosis. Forks that reach a
different `anomaly_type` than the primary (highest-confidence) fork produce a
cross-fork `ConflictRecord` that is surfaced and escalated, never silently
resolved.

Fork count is config-gated. Default (`agent_diagnostic_forks = 1`) preserves
single-agent behavior, so nothing changes until you opt in.

## How to run it locally

Prerequisites (see the main README): Docker Compose stack up, Flink job
submitted, Java simulator JAR built.

1. Bring up the stack:

   ```
   docker compose up -d
   ```

2. Enable fan-out via the environment (the knob is `agent_diagnostic_forks`,
   overridden with the `STREAMOPS_` prefix). 3 forks is a good demo value; the
   cap is the number of defined hypotheses (currently 4):

   ```
   export STREAMOPS_AGENT_DIAGNOSTIC_FORKS=3
   ```

3. Trigger a cycle. Either inject a scenario end-to-end with the demo runner:

   ```
   python scripts/demo_scenario.py latency-spike
   ```

   or run the agent directly for one cycle against whatever the stack is
   currently emitting:

   ```
   cd mcp-server
   STREAMOPS_AGENT_DIAGNOSTIC_FORKS=3 uv run python -m streamops_mcp.agent.main --single-cycle
   ```

To get the **single-agent baseline** to diff against, run the same command with
`STREAMOPS_AGENT_DIAGNOSTIC_FORKS=1` (or unset).

## What to expect in the logs

Every line from one cycle shares a correlation id (`cid=cyc-...`, see the
correlated-logs feature, issue #84). The log format is:

```
<time> [<logger>] [cid=<correlation-id>] <LEVEL> <message>
```

In fork mode you will see the fan-out, one spawn line per fork with its
hypothesis, and the merge. By default (`agent_hypothesis_mode = map`) the
hypotheses are tailored to the anomaly type; a `latency_spike` fans out into its
latency-specific candidate causes:

```
[streamops-mcp.monitor] [cid=cyc-a1b2c3d4e5f6] INFO Fanning out 3 diagnostic forks
[streamops-mcp.monitor] [cid=cyc-a1b2c3d4e5f6] INFO Spawning Diagnostic Agent (hypothesis: GC/heap pressure or long stop-the-world pauses on the TaskManager.)
[streamops-mcp.monitor] [cid=cyc-a1b2c3d4e5f6] INFO Spawning Diagnostic Agent (hypothesis: Serialization/deserialization cost or an expensive operator on the hot path.)
[streamops-mcp.monitor] [cid=cyc-a1b2c3d4e5f6] INFO Spawning Diagnostic Agent (hypothesis: Latency in an external call (enrichment, sink, or lookup) blocking the pipeline.)
[streamops-mcp.monitor] [cid=cyc-a1b2c3d4e5f6] INFO Monitor->Diagnostic handoff validated (type=latency_spike, 412 chars)
...
[streamops-mcp.monitor] [cid=cyc-a1b2c3d4e5f6] INFO Claim confidence distribution: 4 HIGH, 2 MEDIUM, 1 LOW, 0 UNSOURCED
```

An ambiguous anomaly (`type=unknown`) instead fans out into the generic
investigative angles. See "Choosing hypotheses" below.

If the forks disagree on the root cause, the coordinator logs the cross-fork
conflict (note the `xf-` conflict id and the `f{i}:` namespaced claim ids), and
escalation surfaces it:

```
[streamops-mcp.monitor]     [cid=cyc-a1b2c3d4e5f6] WARNING Coordinator received 1 unresolved conflict(s) from Diagnostic Agent
[streamops-mcp.monitor]     [cid=cyc-a1b2c3d4e5f6] WARNING Conflict xf-0-1 [cross-fork root-cause disagreement]: claims f0:C01 vs f1:C02
[streamops-mcp.escalation]  [cid=cyc-a1b2c3d4e5f6] ERROR   Incident '...' carries 1 UNRESOLVED diagnostic conflict(s); forcing review
```

In a non-interactive run (`--single-cycle`) the human acknowledgment gate has no
stdin, so it records the conflict for manual review rather than blocking:

```
[streamops-mcp.escalation] [cid=cyc-a1b2c3d4e5f6] WARNING No human input available for unresolved conflicts on incident ...; logged for manual review
```

If some forks fail after retries, the survivors are still aggregated:

```
[streamops-mcp.monitor] [cid=cyc-a1b2c3d4e5f6] WARNING 1 of 3 diagnostic forks failed; aggregating 2 survivor(s)
```

The cycle only aborts if **every** fork fails.

## What to expect in the audit trail

Each incident is appended to `data/audit/incidents.jsonl` (one JSON object per
line). The fork run shows more sources and claims than a single-agent run (the
union across forks, id-namespaced), and any cross-fork disagreement in the
conflict count:

```json
{
  "incident_id": "…",
  "severity": "MEDIUM",
  "unresolved_conflict_count": 1,
  "conflicts_acknowledged": null,
  "diagnosis": {
    "sources_consulted": ["query_flink_jobs", "query_prometheus", "query_flink_jobs"],
    "claim_count": 7,
    "conflict_count": 1,
    "tools_used": ["query_flink_jobs", "query_prometheus", "query_kafka_lag"]
  }
}
```

## How to prove it actually forked

1. **Shared correlation id + interleaved spawn lines.** All three
   `Spawning Diagnostic Agent (hypothesis: ...)` lines share one `cid=` and
   appear back-to-back *before* any fork completes its investigation. Sequential
   execution would show fork 0 spawn, then its whole tool loop and completion,
   *then* fork 1 spawn. Concurrent execution interleaves them. (The unit test
   `test_forks_run_concurrently_with_distinct_hypotheses` proves this
   structurally with an `asyncio.Barrier`: if the forks ran sequentially the
   barrier would never release and the test would time out.)

2. **Distinct hypotheses per fork.** The three spawn lines carry three different
   hypothesis strings, visible in the logs and in the diagnostic prompt.

3. **Namespaced ids in the merged diagnosis.** The merged claims and sources are
   prefixed per fork (`f0:`, `f1:`, `f2:`), so the merged report carries every
   fork's evidence without collision. A single-agent run has no such prefixes.

4. **Cross-fork conflict ids.** Disagreements across forks appear as conflicts
   with `xf-<primary>-<index>` ids and the topic
   `cross-fork root-cause disagreement`, distinct from intra-fork conflicts.

## Choosing hypotheses

How the fork hypotheses are chosen is set by `agent_hypothesis_mode` (issue #91),
and only matters when `agent_diagnostic_forks > 1`:

- **`map`** (default): hypotheses tailored to the `anomaly_type` (latency_spike,
  throughput_drop, backpressure, checkpoint_failure, memory_pressure,
  error_burst), falling back to generic angles for an unknown type. No extra
  LLM call.
- **`static`**: the fixed generic investigative angles regardless of type.
- **`llm`**: a cheap pre-fan-out call generates candidate hypotheses for the
  specific anomaly; on any failure it falls back to `map`.

The fork count is **adaptive**: it follows the number of hypotheses the anomaly
warrants (bounded by `agent_diagnostic_forks`), so a clear-cut anomaly with a
single plausible cause runs one agent even when the cap is higher.

```
export STREAMOPS_AGENT_HYPOTHESIS_MODE=llm   # or map (default), or static
```

## Baseline to diff against

Run the same scenario with `STREAMOPS_AGENT_DIAGNOSTIC_FORKS=1`. You should see:

- a single `Spawning Diagnostic Agent` line with **no** hypothesis suffix,
- no `Fanning out N diagnostic forks` line,
- no `f{i}:` id prefixes and no `xf-` conflicts in the diagnosis,
- a smaller `claim_count` / `sources_consulted` in the audit entry.

That difference (concurrent multi-hypothesis fan-out with a merged, still-fully-
attributed diagnosis vs a single line of reasoning) is the feature.

## Notes

- Hypotheses are derived from the anomaly and the fork count adapts to
  ambiguity (issue #91); see "Choosing hypotheses" above.
- Cross-cycle change awareness ("what changed since last cycle") is tracked
  separately in issue #77.
