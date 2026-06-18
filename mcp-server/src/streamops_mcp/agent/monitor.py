"""Monitor Agent: the coordinator in the hub-and-spoke topology.

Runs the agentic loop: polls infrastructure via MCP tools, detects anomalies,
spawns diagnostic/report sub-agents, and routes incidents through escalation.

The loop is driven by Claude's stop_reason:
  - "tool_use": Claude wants to call a tool, keep going
  - "end_turn": Claude is done, check if there's an incident to report

Cert ref: Domain 1 (agentic loop, tool use, coordinator pattern).
Cert ref: Domain 1.3 (sub-agent invocation, context injection, conflict escalation).
Cert ref: Domain 1.7 (session state: sub-agents start blank, coordinator injects all context).
"""

import json
import logging
import uuid
from datetime import datetime, timezone

import anthropic

from streamops_mcp.agent.executor import execute_tool
from streamops_mcp.agent.tools import ALL_TOOLS, DIAGNOSTIC_TOOLS, REPORT_TOOLS
from streamops_mcp.agent.schemas import DiagnosisReport, IncidentReport, Severity
from streamops_mcp.agent.escalation import escalate
from streamops_mcp.config import config

logger = logging.getLogger("streamops-mcp.monitor")

MONITOR_SYSTEM_PROMPT = """You are a streaming infrastructure operations agent monitoring an Apache Flink + Kafka pipeline.

Your job:
1. Poll the infrastructure using the available tools
2. Detect anomalies (latency spikes, throughput drops, backpressure, checkpoint failures, memory pressure, error bursts)
3. When you detect an anomaly, investigate it thoroughly using multiple tools
4. Produce a structured diagnosis

Start by checking: Flink job status, consumer lag, and recent events. If everything looks healthy, say so briefly and stop. If you detect a problem, investigate it using all relevant tools before concluding.

Be specific. Cite actual metric values, not vague descriptions. "Latency is 2,340ms (threshold: 200ms)" is useful. "Latency is high" is not."""

DIAGNOSTIC_SYSTEM_PROMPT = """You are a streaming infrastructure diagnostic specialist. You have been given an anomaly detected by the monitoring system.

Your job:
1. Use the available tools to investigate the root cause
2. Check related components for cascading effects
3. Produce a structured DiagnosisReport with full claim-source attribution

Attribution rules (critical):
- For every tool you call, create a SourceRecord with a unique source_id, the tool name, timestamp, and the raw output.
- For every factual finding, create a ClaimRecord with a unique claim_id, the finding text, and the source_id of the tool that produced it.
- If two sources report contradictory data, create a ConflictRecord referencing both claim IDs. Set resolution to "unresolved". Do NOT silently pick one side; the coordinator will decide.

Be thorough. Check at least 3 different data sources before concluding. Correlation is not causation; look for the actual root cause, not just symptoms.

You MUST respond with a valid JSON object matching the DiagnosisReport schema."""

REPORT_SYSTEM_PROMPT = """You are a streaming infrastructure incident reporter. You receive a diagnosis and produce a structured incident report for the on-call team.

Your job:
1. Classify severity based on impact (LOW: cosmetic, MEDIUM: degraded, HIGH: SLA at risk, CRITICAL: data loss or complete outage)
2. Write a clear executive summary
3. Recommend specific, actionable remediation steps
4. Note what to monitor after remediation
5. If the diagnosis contains unresolved conflicts, flag them prominently in the summary so the on-call team is aware of contradictory data

You MUST respond with a valid JSON object matching the IncidentReport schema."""


class MonitorAgent:
    """Coordinator agent that runs the monitoring loop.

    In single-agent mode, this does everything: poll, detect, diagnose, report.
    In multi-agent mode, it delegates diagnosis and reporting to sub-agents.
    """

    def __init__(self, model: str | None = None, multi_agent: bool = True):
        self.client = anthropic.Anthropic()
        self.model = model or config.agent_model
        self.multi_agent = multi_agent
        self.max_tool_rounds = config.agent_max_tool_rounds

    async def run_cycle(self) -> IncidentReport | None:
        """Run one monitoring cycle: poll, detect, diagnose, report.

        Returns an IncidentReport if an anomaly was found, None if healthy.
        """
        logger.info("Starting monitoring cycle (multi_agent=%s)", self.multi_agent)

        detection = await self._detect_anomalies()
        if detection is None:
            logger.info("No anomalies detected, infrastructure healthy")
            return None

        if self.multi_agent:
            diagnosis = await self._spawn_diagnostic_agent(detection)
            report = await self._spawn_report_agent(diagnosis)
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
                    conflict.conflict_id, conflict.topic,
                    conflict.claim_a_id, conflict.claim_b_id,
                )

        await escalate(report)
        return report

    async def _detect_anomalies(self) -> str | None:
        """Run the agentic loop to poll infrastructure and detect anomalies.

        Returns the assistant's final text if anomalies were found, None if healthy.

        Cert ref: Domain 1 (agentic loop driven by stop_reason; tool_use = continue,
        end_turn = done).
        """
        messages = [{"role": "user", "content": "Run a health check on the streaming infrastructure. Check Flink jobs, consumer lag, and recent events. Report any anomalies you find."}]

        for round_num in range(self.max_tool_rounds):
            logger.debug("Detection loop round %d", round_num + 1)

            response = self.client.messages.create(
                model=self.model,
                max_tokens=config.agent_max_tokens,
                system=MONITOR_SYSTEM_PROMPT,
                tools=ALL_TOOLS,
                messages=messages,
            )

            # Process response content blocks
            assistant_text = ""
            tool_calls = []

            for block in response.content:
                if block.type == "text":
                    assistant_text += block.text
                elif block.type == "tool_use":
                    tool_calls.append(block)

            messages.append({"role": "assistant", "content": response.content})

            # stop_reason drives the loop
            if response.stop_reason == "end_turn":
                logger.info("Detection complete after %d rounds", round_num + 1)
                if self._mentions_anomaly(assistant_text):
                    return assistant_text
                return None

            if response.stop_reason == "tool_use":
                tool_results = []
                for tool_call in tool_calls:
                    result = await execute_tool(tool_call.name, tool_call.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_call.id,
                        "content": result,
                    })
                messages.append({"role": "user", "content": tool_results})

            else:
                logger.warning("Unexpected stop_reason: %s", response.stop_reason)
                break

        logger.warning("Detection loop hit max rounds (%d)", self.max_tool_rounds)
        return assistant_text if self._mentions_anomaly(assistant_text) else None

    async def _spawn_diagnostic_agent(self, anomaly_context: str) -> DiagnosisReport:
        """Spawn a Diagnostic sub-agent with scoped context and tools.

        The sub-agent starts with a blank context. All relevant information
        must be injected explicitly via the prompt (not inherited from the
        coordinator's conversation history).

        Cert ref: Domain 1.3 (sub-agent starts blank, structured context injection).
        Cert ref: Domain 1.7 (context isolation between coordinator and sub-agents).
        """
        logger.info("Spawning Diagnostic Agent")

        messages = [{
            "role": "user",
            "content": f"""Investigate the following anomaly detected by the monitoring system:

{anomaly_context}

Use the available tools to determine the root cause. Respond with a JSON object matching the DiagnosisReport schema:
{DiagnosisReport.model_json_schema()}""",
        }]

        for round_num in range(self.max_tool_rounds):
            response = self.client.messages.create(
                model=self.model,
                max_tokens=config.agent_max_tokens,
                system=DIAGNOSTIC_SYSTEM_PROMPT,
                tools=DIAGNOSTIC_TOOLS,
                messages=messages,
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

            if response.stop_reason == "tool_use":
                tool_results = []
                for tool_call in tool_calls:
                    result = await execute_tool(tool_call.name, tool_call.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_call.id,
                        "content": result,
                    })
                messages.append({"role": "user", "content": tool_results})

        logger.warning("Diagnostic Agent hit max rounds")
        return self._parse_diagnosis(assistant_text)

    async def _spawn_report_agent(self, diagnosis: DiagnosisReport) -> IncidentReport:
        """Spawn a Report sub-agent to produce the final incident report.

        No tools needed; the Report agent synthesizes from the diagnosis.
        The full DiagnosisReport (including sources, claims, and conflicts)
        is passed as structured JSON, preserving attribution end-to-end.

        Cert ref: Domain 1.3 (structured context passing with claim-source attribution).
        """
        logger.info("Spawning Report Agent")

        diagnosis_json = diagnosis.model_dump_json(indent=2)

        response = self.client.messages.create(
            model=self.model,
            max_tokens=config.agent_max_tokens,
            system=REPORT_SYSTEM_PROMPT,
            messages=[{
                "role": "user",
                "content": f"""Produce an incident report from this diagnosis:

{diagnosis_json}

Respond with a JSON object matching the IncidentReport schema:
{IncidentReport.model_json_schema()}""",
            }],
        )

        text = "".join(b.text for b in response.content if b.type == "text")
        return self._parse_incident(text, diagnosis)

    def _mentions_anomaly(self, text: str) -> bool:
        """Simple heuristic: does the text suggest an anomaly was found?"""
        anomaly_keywords = [
            "anomaly", "spike", "degraded", "failing", "exceeded", "threshold",
            "timeout", "error", "critical", "backpressure", "lag", "pressure",
        ]
        text_lower = text.lower()
        return any(kw in text_lower for kw in anomaly_keywords)

    def _extract_diagnosis_from_detection(self, detection_text: str) -> DiagnosisReport:
        """In single-agent mode, build a diagnosis from the detection text."""
        return DiagnosisReport(
            anomaly_type="unknown",
            detected_at=datetime.now(timezone.utc).isoformat(),
            affected_components=[],
            root_cause={
                "summary": "See detection notes below",
                "confidence": "medium",
                "reasoning": detection_text[:1000],
                "supporting_metrics": [],
            },
            tools_used=[],
            raw_evidence=[detection_text[:500]],
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
                detected_at=datetime.now(timezone.utc).isoformat(),
                affected_components=[],
                root_cause={
                    "summary": "Agent response could not be parsed as structured output",
                    "confidence": "low",
                    "reasoning": text[:500],
                    "supporting_metrics": [],
                },
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
