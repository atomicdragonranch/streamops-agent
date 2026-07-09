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
                try:
                    diagnosis = await self._retry_subagent(
                        "Diagnostic Agent",
                        lambda: self._spawn_diagnostic_agent(detection),
                    )
                except Exception as exc:
                    logger.error(
                        "Diagnostic Agent failed after retries, cycle aborted: %s",
                        exc,
                    )
                    return None

                self._log_confidence_distribution(diagnosis)

                if self._all_claims_low_confidence(diagnosis):
                    logger.warning(
                        "All %d claims are LOW or UNSOURCED confidence; "
                        "downgrading to warning-level log, skipping report agent",
                        len(diagnosis.claims),
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

    async def _spawn_diagnostic_agent(self, anomaly: DetectedAnomaly) -> DiagnosisReport:
        """Spawn a Diagnostic sub-agent with scoped context and tools.

        The sub-agent starts with a blank context. All relevant information
        must be injected explicitly via the prompt (not inherited from the
        coordinator's conversation history), as a typed DetectedAnomaly rather
        than a prose string.
        """
        logger.info("Spawning Diagnostic Agent")

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

        messages: list[dict[str, Any]] = [
            {
                "role": "user",
                "content": f"""Investigate the following anomaly detected by the monitoring system:

{anomaly_json}

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
        """Extract an IncidentReport from the report agent's response."""
        try:
            json_str = self._extract_json(text)
            return IncidentReport.model_validate_json(json_str)
        except Exception as e:
            logger.warning("Failed to parse IncidentReport: %s, using fallback", e)
            return IncidentReport(
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
