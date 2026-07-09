"""Monitor Agent: the coordinator in the hub-and-spoke topology.

Runs the agentic loop: polls infrastructure via MCP tools, detects anomalies,
spawns diagnostic/report sub-agents, and routes incidents through escalation.

The loop is driven by Claude's stop_reason:
  - "tool_use": Claude wants to call a tool, keep going
  - "end_turn": Claude is done, check if there's an incident to report

Sub-agents start blank; the coordinator injects all context via structured
prompts to maintain session isolation. Transient API failures (timeouts,
rate limits, 5xx) are retried with exponential backoff; permanent failures
trigger graceful degradation.
"""

import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import anthropic

from streamops_mcp.agent.escalation import escalate
from streamops_mcp.agent.executor import execute_tool
from streamops_mcp.agent.schemas import (
    Confidence,
    ConflictRecord,
    DetectedAnomaly,
    DiagnosisReport,
    DiagnosticToReportHandoff,
    IncidentReport,
    MonitorToDiagnosticHandoff,
    RootCause,
    Severity,
)
from streamops_mcp.agent.tools import ALL_TOOLS, DIAGNOSTIC_TOOLS
from streamops_mcp.config import config
from streamops_mcp.logging_setup import (
    new_correlation_id,
    reset_correlation_id,
    set_correlation_id,
)
from streamops_mcp.prompts import load_prompt, load_runbook

logger = logging.getLogger("streamops-mcp.monitor")

MONITOR_SYSTEM_PROMPT = load_prompt("monitor")
DIAGNOSTIC_SYSTEM_PROMPT = load_prompt("diagnostic")
REPORT_SYSTEM_PROMPT = load_prompt("report")

# Generic investigative angles, used for ambiguous (unknown-type) anomalies and
# for "static" hypothesis mode. Each fork explores a different candidate cause
# instead of one line of reasoning tunnel-visioning (issue #67).
DIAGNOSTIC_HYPOTHESES = [
    "Resource saturation: CPU, memory/heap, or GC pressure on the affected component.",
    "Data-side cause: partition skew, hot keys, or a surge in input volume.",
    "External dependency: a downstream sink, source, or coordination service degrading.",
    "Configuration or deployment change: a recent config, scaling, or version change.",
]

# Hypotheses tailored per anomaly_type (issue #91): forks investigate the causes
# actually plausible for this symptom rather than generic angles. The number of
# entries drives the adaptive fork count for a typed anomaly (bounded by the cap).
ANOMALY_HYPOTHESES = {
    "latency_spike": [
        "GC/heap pressure or long stop-the-world pauses on the TaskManager.",
        "Serialization/deserialization cost or an expensive operator on the hot path.",
        "Latency in an external call (enrichment, sink, or lookup) blocking the pipeline.",
    ],
    "throughput_drop": [
        "A slow or stuck consumer/operator reducing end-to-end throughput.",
        "A partition rebalance or reassignment interrupting consumption.",
        "An upstream volume surge or source slowdown changing input rate.",
    ],
    "backpressure": [
        "Downstream sink saturation (slow writes) propagating backpressure upstream.",
        "Operator skew or a hot key overloading one subtask.",
        "Insufficient parallelism for the current load.",
    ],
    "checkpoint_failure": [
        "State size growth making checkpoints exceed their timeout.",
        "Checkpoint timeout/interval configuration too tight for the workload.",
        "State-backend or durable-storage I/O errors or slowness.",
    ],
    "memory_pressure": [
        "Heap exhaustion or GC thrash from object churn.",
        "Unbounded state growth (missing TTL or retention).",
        "Data skew concentrating memory on one subtask.",
    ],
    "error_burst": [
        "Bad or poison input (malformed events, schema drift) triggering exceptions.",
        "A downstream dependency failing and surfacing as errors.",
        "A recent deploy or config change regressing behavior.",
    ],
}

# Confidence ordering for picking a fork's representative claim and the primary fork.
_CONFIDENCE_RANK = {
    Confidence.HIGH: 3,
    Confidence.MEDIUM: 2,
    Confidence.LOW: 1,
    Confidence.UNSOURCED: 0,
}


class MonitorAgent:
    """Coordinator agent that runs the monitoring loop.

    In single-agent mode, this does everything: poll, detect, diagnose, report.
    In multi-agent mode, it delegates diagnosis and reporting to sub-agents.
    """

    def __init__(self, model: str | None = None, multi_agent: bool = True):
        self.client = anthropic.AsyncAnthropic()
        self.model = model or config.agent_model
        self.multi_agent = multi_agent
        self.max_tool_rounds = config.agent_max_tool_rounds

    @staticmethod
    def _is_retryable(exc: Exception) -> bool:
        """Classify whether an API error is transient and worth retrying."""
        if isinstance(exc, anthropic.APITimeoutError):
            return True
        if isinstance(exc, anthropic.RateLimitError):
            return True
        if isinstance(exc, anthropic.InternalServerError):
            return True
        if isinstance(exc, anthropic.APIConnectionError):
            return True
        return False

    async def _retry_subagent(self, name: str, coro_factory):
        """Retry a subagent call with exponential backoff on transient failures.

        Returns the subagent result on success, or raises on permanent failure
        or after all retries are exhausted.
        """
        max_retries = config.agent_max_retries
        base_delay = config.agent_retry_base_delay
        last_exc: Exception | None = None

        for attempt in range(1 + max_retries):
            try:
                return await coro_factory()
            except Exception as exc:
                last_exc = exc
                retryable = self._is_retryable(exc)
                logger.error(
                    "%s failed (attempt %d/%d, retryable=%s): %s",
                    name,
                    attempt + 1,
                    1 + max_retries,
                    retryable,
                    exc,
                )
                if not retryable or attempt >= max_retries:
                    raise
                delay = base_delay * (2**attempt)
                logger.info("Retrying %s in %.1fs", name, delay)
                await asyncio.sleep(delay)

        assert last_exc is not None  # pragma: no cover
        raise last_exc  # pragma: no cover

    async def run_cycle(self) -> IncidentReport | None:
        """Run one monitoring cycle: poll, detect, diagnose, report.

        Returns an IncidentReport if an anomaly was found, None if healthy.

        A correlation id is bound for the whole cycle so every log line the
        cycle emits (this coordinator, both sub-agents, the tool executor, and
        escalation) shares one greppable id. See issue #84.
        """
        cycle_id = new_correlation_id()
        token = set_correlation_id(cycle_id)
        try:
            logger.info(
                "Starting monitoring cycle (cycle_id=%s, multi_agent=%s)",
                cycle_id,
                self.multi_agent,
            )

            detection = await self._detect_anomalies()
            if detection is None:
                logger.info("No anomalies detected, infrastructure healthy")
                return None

            if self.multi_agent:
                diagnosis = await self._run_diagnostics(detection)
                if diagnosis is None:
                    return None

                self._log_confidence_distribution(diagnosis)

                # Fail-fast: verify the diagnosis is substantive before delegating to
                # the Report agent, so it never synthesizes an incident from thin or
                # degraded context (cert Ep 04 pattern). See issue #94.
                gate_reason = self._verify_diagnosis_precondition(diagnosis)
                if gate_reason is not None:
                    logger.warning(
                        "Skipping Report Agent, diagnosis not reportable: %s", gate_reason
                    )
                    return None

                low_claims = self._extract_low_confidence_claims(diagnosis)

                try:
                    report = await self._retry_subagent(
                        "Report Agent",
                        lambda: self._spawn_report_agent(diagnosis),
                    )
                except Exception as exc:
                    logger.warning(
                        "Report Agent failed after retries, producing fallback report: %s",
                        exc,
                    )
                    report = self._fallback_report(diagnosis)

                if low_claims and not report.low_confidence_claims:
                    report.low_confidence_claims = low_claims
            else:
                diagnosis = self._extract_diagnosis_from_detection(detection)
                report = await self._spawn_report_agent(diagnosis)

            if diagnosis.conflicts:
                logger.warning(
                    "Coordinator received %d unresolved conflict(s) from Diagnostic Agent",
                    len(diagnosis.conflicts),
                )
                for conflict in diagnosis.conflicts:
                    logger.warning(
                        "Conflict %s [%s]: claims %s vs %s",
                        conflict.conflict_id,
                        conflict.topic,
                        conflict.claim_a_id,
                        conflict.claim_b_id,
                    )

            await escalate(report, diagnosis=diagnosis)
            return report
        finally:
            reset_correlation_id(token)

    async def _run_diagnostics(self, anomaly: DetectedAnomaly) -> DiagnosisReport | None:
        """Diagnose the anomaly, single-agent or fork-style (issues #67, #91).

        The hypotheses are planned from the anomaly (see ``_plan_hypotheses``),
        which also sets the fork count adaptively: a clear-cut anomaly runs one
        agent, an ambiguous one fans out, never above ``agent_diagnostic_forks``.
        With 0 or 1 planned hypotheses this is the original single Diagnostic
        agent (seeded with the one hypothesis if there is one). Returns None only
        if the diagnosis could not be produced at all (the single agent failed,
        or every fork failed after retries).
        """
        hypotheses = await self._plan_hypotheses(anomaly)

        if len(hypotheses) <= 1:
            seed = hypotheses[0] if hypotheses else None
            try:
                return await self._retry_subagent(
                    "Diagnostic Agent",
                    lambda: self._spawn_diagnostic_agent(anomaly, hypothesis=seed),
                )
            except Exception as exc:
                logger.error("Diagnostic Agent failed after retries, cycle aborted: %s", exc)
                return None

        fork_count = len(hypotheses)
        logger.info("Fanning out %d diagnostic forks", fork_count)

        async def _fork(index: int, hypothesis: str) -> DiagnosisReport:
            return await self._retry_subagent(
                f"Diagnostic Agent fork {index}",
                lambda: self._spawn_diagnostic_agent(anomaly, hypothesis=hypothesis),
            )

        # gather (not a loop) so the forks truly run concurrently on a shared baseline.
        results = await asyncio.gather(
            *(_fork(i, h) for i, h in enumerate(hypotheses)),
            return_exceptions=True,
        )
        survivors = [r for r in results if isinstance(r, DiagnosisReport)]
        failed = fork_count - len(survivors)

        if not survivors:
            logger.error("All %d diagnostic forks failed after retries, cycle aborted", fork_count)
            return None
        if failed:
            logger.warning(
                "%d of %d diagnostic forks failed; aggregating %d survivor(s)",
                failed,
                fork_count,
                len(survivors),
            )
        return self._merge_fork_diagnoses(survivors)

    async def _plan_hypotheses(self, anomaly: DetectedAnomaly) -> list[str]:
        """Choose the diagnostic hypotheses (and thus the fork count) for an anomaly.

        Bounded by ``agent_diagnostic_forks``: <= 1 means single-agent (empty
        list). Otherwise the pool depends on ``agent_hypothesis_mode``:
          - "static": generic investigative angles.
          - "map": angles tailored to the anomaly_type, generic for unknown types
            (issue #91). The size of the type's entry adapts the fork count, so a
            clear-cut type with one plausible cause runs one agent.
          - "llm": angles generated per-anomaly, falling back to the map on failure.
        """
        cap = config.agent_diagnostic_forks
        if cap <= 1:
            return []

        mode = config.agent_hypothesis_mode
        if mode == "llm":
            generated = await self._generate_hypotheses_llm(anomaly, cap)
            pool = generated or self._mapped_hypotheses(anomaly)
        elif mode == "static":
            pool = DIAGNOSTIC_HYPOTHESES
        else:  # "map"
            pool = self._mapped_hypotheses(anomaly)

        return pool[:cap]

    @staticmethod
    def _mapped_hypotheses(anomaly: DetectedAnomaly) -> list[str]:
        """Per-type hypotheses, or the generic angles for an unknown/unmapped type."""
        return ANOMALY_HYPOTHESES.get(anomaly.anomaly_type) or DIAGNOSTIC_HYPOTHESES

    async def _generate_hypotheses_llm(self, anomaly: DetectedAnomaly, k: int) -> list[str] | None:
        """Ask the model for up to k candidate root-cause hypotheses for this anomaly.

        Returns None on any failure so the caller falls back to the static map;
        a diagnosis must never hinge on this optional pre-step succeeding.
        """
        prompt = (
            f"Given this streaming-pipeline anomaly, list up to {k} distinct candidate "
            "root-cause hypotheses worth investigating in parallel, most likely first, "
            "one per line, no numbering or commentary.\n\n"
            f"{anomaly.model_dump_json(indent=2)}"
        )
        try:
            response = await self.client.messages.create(
                model=self.model,
                max_tokens=config.agent_max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as exc:
            logger.warning("Hypothesis generation failed (%s); falling back to the map", exc)
            return None

        text = "".join(b.text for b in response.content if b.type == "text")
        lines = [line.strip(" -*\t").strip() for line in text.splitlines()]
        hypotheses = [line for line in lines if line][:k]
        if not hypotheses:
            logger.warning("Hypothesis generation returned nothing usable; falling back to the map")
            return None
        return hypotheses

    @classmethod
    def _merge_fork_diagnoses(cls, diagnoses: list[DiagnosisReport]) -> DiagnosisReport:
        """Merge parallel fork diagnoses into one, preserving attribution.

        Each fork's ids are namespaced (``f{i}:``) so merged sources, claims, and
        conflicts never collide. Forks that concluded a different anomaly_type
        than the primary (highest-confidence) fork produce a cross-fork
        ConflictRecord: annotated, unresolved, and left for the coordinator's
        escalation path (issue #82) rather than silently picking a winner.
        """
        if len(diagnoses) == 1:
            return diagnoses[0]

        merged_sources = []
        merged_claims = []
        merged_conflicts = []
        reps: list[tuple[int, str | None, str, str]] = []  # (fork, rep_claim_id, type, summary)

        for i, d in enumerate(diagnoses):
            prefix = f"f{i}:"
            for s in d.sources:
                merged_sources.append(s.model_copy(update={"source_id": prefix + s.source_id}))
            for c in d.claims:
                merged_claims.append(
                    c.model_copy(
                        update={"claim_id": prefix + c.claim_id, "source_id": prefix + c.source_id}
                    )
                )
            for cf in d.conflicts:
                merged_conflicts.append(
                    cf.model_copy(
                        update={
                            "conflict_id": prefix + cf.conflict_id,
                            "claim_a_id": prefix + cf.claim_a_id,
                            "claim_b_id": prefix + cf.claim_b_id,
                        }
                    )
                )
            reps.append(
                (i, cls._representative_claim_id(d, prefix), d.anomaly_type, d.root_cause.summary)
            )

        primary = cls._primary_fork_index(diagnoses)
        merged_conflicts.extend(cls._cross_fork_conflicts(reps, primary))

        chosen = diagnoses[primary]
        return DiagnosisReport(
            anomaly_type=chosen.anomaly_type,
            detected_at=min(d.detected_at for d in diagnoses),
            sources=merged_sources,
            claims=merged_claims,
            conflicts=merged_conflicts,
            affected_components=[c for d in diagnoses for c in d.affected_components],
            root_cause=chosen.root_cause,
            tools_used=sorted({t for d in diagnoses for t in d.tools_used}),
            raw_evidence=[e for d in diagnoses for e in d.raw_evidence],
        )

    @classmethod
    def _representative_claim_id(cls, diagnosis: DiagnosisReport, prefix: str) -> str | None:
        """The namespaced id of a fork's highest-confidence claim, or None if it has none."""
        if not diagnosis.claims:
            return None
        best = max(diagnosis.claims, key=lambda c: _CONFIDENCE_RANK[c.confidence])
        return prefix + best.claim_id

    @classmethod
    def _primary_fork_index(cls, diagnoses: list[DiagnosisReport]) -> int:
        """Index of the fork whose best claim has the highest confidence (ties: lowest index)."""

        def fork_rank(i: int) -> int:
            claims = diagnoses[i].claims
            return max((_CONFIDENCE_RANK[c.confidence] for c in claims), default=-1)

        return max(range(len(diagnoses)), key=fork_rank)

    @staticmethod
    def _cross_fork_conflicts(
        reps: list[tuple[int, str | None, str, str]], primary: int
    ) -> list[ConflictRecord]:
        """Pair the primary fork against each fork that concluded a different anomaly_type.

        anomaly_type is the deterministic, structured disagreement signal (comparing
        free-text root causes would flag every fork as different). Semantic
        claim-level reconciliation would need an LLM and is out of scope here.
        Only forks that produced at least one claim can be referenced.
        """
        by_index = {r[0]: r for r in reps}
        p = by_index.get(primary)
        if p is None or p[1] is None:
            return []
        _, p_claim, p_type, p_summary = p
        assert p_claim is not None  # guarded by p[1] is None check above

        conflicts = []
        for index, rep_claim, anomaly_type, summary in reps:
            if index == primary or rep_claim is None:
                continue
            if anomaly_type != p_type:
                conflicts.append(
                    ConflictRecord(
                        conflict_id=f"xf-{primary}-{index}",
                        topic="cross-fork root-cause disagreement",
                        claim_a_id=p_claim,
                        claim_b_id=rep_claim,
                        resolution="unresolved",
                        notes=(
                            f"Fork {primary} concluded '{p_type}' ({p_summary}); "
                            f"fork {index} concluded '{anomaly_type}' ({summary})"
                        ),
                    )
                )
        return conflicts

    @staticmethod
    def _fallback_report(diagnosis: DiagnosisReport) -> IncidentReport:
        """Produce a fallback IncidentReport when the Report Agent fails."""
        return IncidentReport(
            incident_id=str(uuid.uuid4())[:8],
            title=f"Anomaly: {diagnosis.anomaly_type} (report agent unavailable)",
            severity=Severity.MEDIUM,
            summary=diagnosis.root_cause.summary,
            anomaly_type=diagnosis.anomaly_type,
            root_cause=diagnosis.root_cause.summary,
            affected_components=[c.name for c in diagnosis.affected_components],
            timeline=[f"Detected at {diagnosis.detected_at}"],
            recommended_actions=[],
            monitoring_notes="Report agent was unavailable; review diagnosis data directly",
            # Even on the fallback path the incident stays traceable (issue #88).
            sources=diagnosis.sources,
            supporting_claims=diagnosis.claims,
        )

    @staticmethod
    def _log_confidence_distribution(diagnosis: DiagnosisReport) -> None:
        """Log a summary of claim confidence levels for coordinator awareness."""
        counts = {level: 0 for level in Confidence}
        for claim in diagnosis.claims:
            counts[claim.confidence] = counts.get(claim.confidence, 0) + 1
        summary = ", ".join(f"{counts[level]} {level.value}" for level in Confidence)
        logger.info("Claim confidence distribution: %s", summary)

    @staticmethod
    def _all_claims_low_confidence(diagnosis: DiagnosisReport) -> bool:
        """Return True if every claim is LOW or UNSOURCED (or there are no claims)."""
        if not diagnosis.claims:
            return False
        low_levels = {Confidence.LOW, Confidence.UNSOURCED}
        return all(c.confidence in low_levels for c in diagnosis.claims)

    def _verify_diagnosis_precondition(self, diagnosis: DiagnosisReport) -> str | None:
        """Check the diagnosis is substantive enough to delegate to the Report agent.

        Returns a short reason string when the diagnosis is NOT fit to delegate,
        or None when it is. Delegating a thin or degraded finding invites the
        downstream Report agent to synthesize an incident from nothing (cert Ep 04
        fail-fast delegation). Gated cases:
          - a parse-error fallback diagnosis (unstructured, produced when the
            Diagnostic agent's output could not be parsed),
          - a diagnosis with no claims (no structured finding to report on),
          - all claims LOW or UNSOURCED (no confident finding).
        """
        if diagnosis.anomaly_type == "parse_error":
            return "diagnosis is a parse-error fallback (unstructured)"
        if not diagnosis.claims:
            return "diagnosis has no claims to report on"
        if self._all_claims_low_confidence(diagnosis):
            return f"all {len(diagnosis.claims)} claims are LOW or UNSOURCED confidence"
        return None

    @staticmethod
    def _extract_low_confidence_claims(diagnosis: DiagnosisReport) -> list[str]:
        """Extract claim text for LOW and UNSOURCED claims."""
        low_levels = {Confidence.LOW, Confidence.UNSOURCED}
        return [c.text for c in diagnosis.claims if c.confidence in low_levels]

    async def _detect_anomalies(self) -> DetectedAnomaly | None:
        """Run the agentic loop to poll infrastructure and detect anomalies.

        Returns a structured DetectedAnomaly if an anomaly was found, None if
        healthy. The agent explores with tools, then, on concluding an anomaly
        exists, emits a DetectedAnomaly JSON so the handoff to the Diagnostic
        agent carries typed context instead of prose.
        """
        detection_schema = DetectedAnomaly.model_json_schema()
        messages: list[dict[str, Any]] = [
            {
                "role": "user",
                "content": (
                    "Run a health check on the streaming infrastructure. Check Flink jobs, "
                    "consumer lag, and recent events. If everything is healthy, say so briefly. "
                    "If you detect an anomaly, respond with ONLY a JSON object matching this "
                    f"DetectedAnomaly schema:\n{detection_schema}"
                ),
            }
        ]

        for round_num in range(self.max_tool_rounds):
            logger.debug("Detection loop round %d", round_num + 1)

            # The Anthropic SDK accepts our dict-built tool/message payloads at runtime; its
            # TypedDict params are stricter than our dynamic construction, hence the ignores.
            response = await self.client.messages.create(
                model=self.model,
                max_tokens=config.agent_max_tokens,
                system=MONITOR_SYSTEM_PROMPT,
                tools=ALL_TOOLS,  # type: ignore[arg-type]
                messages=messages,  # type: ignore[arg-type]
            )

            assistant_text = ""
            tool_calls = []

            for block in response.content:
                if block.type == "text":
                    assistant_text += block.text
                elif block.type == "tool_use":
                    tool_calls.append(block)

            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                logger.info("Detection complete after %d rounds", round_num + 1)
                if self._mentions_anomaly(assistant_text):
                    anomaly = self._parse_detection(assistant_text)
                    logger.info("Anomaly detected: %s", anomaly.summary[:300])
                    return anomaly
                logger.info("No anomalies found in detection response")
                return None

            if response.stop_reason == "tool_use":
                tool_results = []
                for tool_call in tool_calls:
                    result = await execute_tool(tool_call.name, tool_call.input)
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": tool_call.id,
                            "content": result,
                        }
                    )
                messages.append({"role": "user", "content": tool_results})

            else:
                logger.warning("Unexpected stop_reason: %s", response.stop_reason)
                break

        logger.warning("Detection loop hit max rounds (%d)", self.max_tool_rounds)
        return (
            self._parse_detection(assistant_text)
            if self._mentions_anomaly(assistant_text)
            else None
        )

    async def _spawn_diagnostic_agent(
        self, anomaly: DetectedAnomaly, hypothesis: str | None = None
    ) -> DiagnosisReport:
        """Spawn a Diagnostic sub-agent with scoped context and tools.

        The sub-agent starts with a blank context. All relevant information
        must be injected explicitly via the prompt (not inherited from the
        coordinator's conversation history), as a typed DetectedAnomaly rather
        than a prose string. When a hypothesis is given (fork-style exploration,
        issue #67), the agent is steered to investigate that angle first.
        """
        logger.info(
            "Spawning Diagnostic Agent%s",
            f" (hypothesis: {hypothesis})" if hypothesis else "",
        )

        schema_hint = DiagnosisReport.model_json_schema()
        handoff = MonitorToDiagnosticHandoff(
            anomaly=anomaly,
            schema_hint=schema_hint,
        )
        anomaly_json = handoff.anomaly.model_dump_json(indent=2)
        logger.info(
            "Monitor->Diagnostic handoff validated (type=%s, %d chars)",
            handoff.anomaly.anomaly_type,
            len(anomaly_json),
        )

        system_prompt = DIAGNOSTIC_SYSTEM_PROMPT
        runbook_section = self._resolve_runbooks(handoff.anomaly.summary)
        if runbook_section:
            system_prompt = system_prompt + "\n\n" + runbook_section

        hypothesis_line = (
            f"\nPrioritize investigating this hypothesis before others: {hypothesis}\n"
            if hypothesis
            else ""
        )
        messages: list[dict[str, Any]] = [
            {
                "role": "user",
                "content": f"""Investigate the following anomaly detected by the monitoring system:

{anomaly_json}
{hypothesis_line}
Use the available tools to determine the root cause. Respond with a JSON object matching the DiagnosisReport schema:
{handoff.schema_hint}""",
            }
        ]

        for round_num in range(self.max_tool_rounds):
            if not messages or messages[-1]["role"] != "user":
                logger.warning("Diagnostic Agent: messages ended on non-user role, ending early")
                break

            # See the note in _detect_anomalies: dict payloads are runtime-valid for the SDK.
            response = await self.client.messages.create(
                model=self.model,
                max_tokens=config.agent_max_tokens,
                system=system_prompt,
                tools=DIAGNOSTIC_TOOLS,  # type: ignore[arg-type]
                messages=messages,  # type: ignore[arg-type]
            )

            assistant_text = ""
            tool_calls = []

            for block in response.content:
                if block.type == "text":
                    assistant_text += block.text
                elif block.type == "tool_use":
                    tool_calls.append(block)

            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                logger.info("Diagnostic Agent completed after %d rounds", round_num + 1)
                return self._parse_diagnosis(assistant_text)

            if response.stop_reason == "tool_use" and tool_calls:
                tool_results = []
                for tool_call in tool_calls:
                    result = await execute_tool(tool_call.name, tool_call.input)
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": tool_call.id,
                            "content": result,
                        }
                    )
                messages.append({"role": "user", "content": tool_results})

        logger.warning("Diagnostic Agent hit max rounds")
        return self._parse_diagnosis(assistant_text)

    async def _spawn_report_agent(self, diagnosis: DiagnosisReport) -> IncidentReport:
        """Spawn a Report sub-agent to produce the final incident report.

        No tools needed; the Report agent synthesizes from the diagnosis.
        The full DiagnosisReport (including sources, claims, and conflicts)
        is passed as structured JSON, preserving attribution end-to-end.
        """
        logger.info("Spawning Report Agent")

        diagnosis_json = diagnosis.model_dump_json(indent=2)
        schema_hint = IncidentReport.model_json_schema()
        handoff = DiagnosticToReportHandoff(
            diagnosis_json=diagnosis_json,
            schema_hint=schema_hint,
        )
        logger.info(
            "Diagnostic->Report handoff validated (%d chars)",
            len(handoff.diagnosis_json),
        )

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=config.agent_max_tokens,
            system=REPORT_SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": f"""Produce an incident report from this diagnosis:

{handoff.diagnosis_json}

Respond with a JSON object matching the IncidentReport schema:
{handoff.schema_hint}""",
                }
            ],
        )

        text = "".join(b.text for b in response.content if b.type == "text")
        return self._parse_incident(text, diagnosis)

    @staticmethod
    def _resolve_runbooks(anomaly_context: str) -> str:
        """Match anomaly context against available runbooks and return combined content."""
        context_lower = anomaly_context.lower()
        keyword_map = {
            "latency_spike": ["latency", "slow", "delay", "processing time"],
            "checkpoint_failure": ["checkpoint", "savepoint", "state snapshot"],
            "throughput_drop": ["lag", "throughput", "consumer lag", "behind"],
            "backpressure": ["backpressure", "back pressure", "saturated"],
            "error_burst": ["error", "exception", "out of order", "late event"],
        }

        matched = []
        for anomaly_type, keywords in keyword_map.items():
            if any(kw in context_lower for kw in keywords):
                content = load_runbook(anomaly_type)
                if content:
                    matched.append(f"## Runbook: {anomaly_type}\n\n{content}")

        if not matched:
            return ""

        logger.info("Injecting %d runbook(s) into diagnostic context", len(matched))
        return "---\nRelevant runbooks for this investigation:\n\n" + "\n\n".join(matched)

    def _mentions_anomaly(self, text: str) -> bool:
        """Simple heuristic: does the text suggest an anomaly was found?"""
        anomaly_keywords = [
            "anomaly",
            "spike",
            "degraded",
            "failing",
            "exceeded",
            "threshold",
            "timeout",
            "error",
            "critical",
            "backpressure",
            "lag",
            "pressure",
        ]
        text_lower = text.lower()
        return any(kw in text_lower for kw in anomaly_keywords)

    def _extract_diagnosis_from_detection(self, anomaly: DetectedAnomaly) -> DiagnosisReport:
        """In single-agent mode, build a diagnosis from the detected anomaly."""
        return DiagnosisReport(
            anomaly_type=anomaly.anomaly_type,
            detected_at=anomaly.detected_at,
            affected_components=[],
            root_cause=RootCause(
                summary=anomaly.summary,
                confidence="medium",
                reasoning=anomaly.summary,
                supporting_metrics=[
                    m for m in (anomaly.metric, anomaly.observed_value) if m is not None
                ],
            ),
            tools_used=[],
            raw_evidence=[anomaly.summary],
        )

    def _parse_detection(self, text: str) -> DetectedAnomaly:
        """Extract a DetectedAnomaly from the monitor's response.

        Falls back to wrapping the raw text in ``summary`` when the model
        emitted prose instead of JSON, so detection always yields typed context.
        """
        try:
            json_str = self._extract_json(text)
            return DetectedAnomaly.model_validate_json(json_str)
        except Exception as e:
            logger.warning("Failed to parse DetectedAnomaly: %s, using prose fallback", e)
            return DetectedAnomaly(
                anomaly_type="unknown",
                summary=text[:1000],
                detected_at=datetime.now(UTC).isoformat(),
            )

    def _parse_diagnosis(self, text: str) -> DiagnosisReport:
        """Extract a DiagnosisReport from the agent's text response."""
        try:
            json_str = self._extract_json(text)
            return DiagnosisReport.model_validate_json(json_str)
        except Exception as e:
            logger.warning("Failed to parse DiagnosisReport: %s, using fallback", e)
            return DiagnosisReport(
                anomaly_type="parse_error",
                detected_at=datetime.now(UTC).isoformat(),
                affected_components=[],
                root_cause=RootCause(
                    summary="Agent response could not be parsed as structured output",
                    confidence="low",
                    reasoning=text[:500],
                    supporting_metrics=[],
                ),
                tools_used=[],
                raw_evidence=[text[:500]],
            )

    def _parse_incident(self, text: str, diagnosis: DiagnosisReport) -> IncidentReport:
        """Extract an IncidentReport from the report agent's response.

        The report is always re-attributed from the diagnosis so the incident
        stays traceable to its evidence (see issue #88).
        """
        try:
            json_str = self._extract_json(text)
            report = IncidentReport.model_validate_json(json_str)
        except Exception as e:
            logger.warning("Failed to parse IncidentReport: %s, using fallback", e)
            report = IncidentReport(
                incident_id=str(uuid.uuid4())[:8],
                title=f"Anomaly: {diagnosis.anomaly_type}",
                severity=Severity.MEDIUM,
                summary=diagnosis.root_cause.summary,
                anomaly_type=diagnosis.anomaly_type,
                root_cause=diagnosis.root_cause.summary,
                affected_components=[c.name for c in diagnosis.affected_components],
                timeline=[f"Detected at {diagnosis.detected_at}"],
                recommended_actions=[],
                monitoring_notes="Monitor after remediation",
            )
        return self._attach_attribution(report, diagnosis)

    @staticmethod
    def _attach_attribution(report: IncidentReport, diagnosis: DiagnosisReport) -> IncidentReport:
        """Carry the diagnosis's sources and claims into the report.

        Deterministic (not LLM-supplied), so no hallucinated source_ids; the
        merged report is re-validated so the attribution invariant holds on what
        the pipeline actually emits. The diagnosis is already validated, so this
        never fails in the normal path. See issue #88.
        """
        merged = report.model_dump()
        merged["sources"] = [s.model_dump() for s in diagnosis.sources]
        merged["supporting_claims"] = [c.model_dump() for c in diagnosis.claims]
        return IncidentReport.model_validate(merged)

    def _extract_json(self, text: str) -> str:
        """Extract JSON from text that may contain markdown code blocks."""
        if "```json" in text:
            start = text.index("```json") + 7
            end = text.index("```", start)
            return text[start:end].strip()
        if "```" in text:
            start = text.index("```") + 3
            end = text.index("```", start)
            return text[start:end].strip()
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            return text[start:end]
        return text
