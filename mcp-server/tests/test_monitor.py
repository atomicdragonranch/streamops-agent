"""Tests for the MonitorAgent.

Tests the agent's internal logic (anomaly detection heuristic, JSON extraction,
parsing, retry/fallback behavior) without making actual Claude API calls.
"""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import anthropic
import httpx
import pytest

from streamops_mcp.agent.monitor import MonitorAgent
from streamops_mcp.agent.schemas import (
    ClaimRecord,
    Confidence,
    DiagnosisReport,
    IncidentReport,
    Severity,
    SourceRecord,
)


def _fake_response(status_code: int) -> httpx.Response:
    """Build a minimal httpx.Response that anthropic exception constructors accept."""
    req = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    resp = httpx.Response(status_code=status_code, request=req)
    return resp


@pytest.fixture
def agent():
    return MonitorAgent(multi_agent=True)


class TestAnomalyDetection:

    def test_detects_anomaly_keywords(self, agent):
        # Arrange
        text = "Consumer lag spike detected on partition 3, lag is 450,000 records"

        # Act
        result = agent._mentions_anomaly(text)

        # Assert
        assert result is True

    def test_healthy_system_no_anomaly(self, agent):
        # Arrange
        text = "All systems healthy. Flink job running, no issues detected."

        # Act
        result = agent._mentions_anomaly(text)

        # Assert
        assert result is False

    def test_case_insensitive(self, agent):
        # Arrange
        text = "CRITICAL BACKPRESSURE on operator chain"

        # Act
        result = agent._mentions_anomaly(text)

        # Assert
        assert result is True


class TestJsonExtraction:

    def test_extracts_from_code_block(self, agent):
        # Arrange
        text = 'Here is the report:\n```json\n{"key": "value"}\n```\nDone.'

        # Act
        result = agent._extract_json(text)

        # Assert
        assert result == '{"key": "value"}'

    def test_extracts_from_bare_json(self, agent):
        # Arrange
        text = 'The diagnosis is {"anomaly_type": "latency_spike"}'

        # Act
        result = agent._extract_json(text)

        # Assert
        assert '"anomaly_type": "latency_spike"' in result

    def test_extracts_from_generic_code_block(self, agent):
        # Arrange
        text = '```\n{"key": "val"}\n```'

        # Act
        result = agent._extract_json(text)

        # Assert
        assert result == '{"key": "val"}'


class TestDiagnosisParsing:

    def test_valid_json_parses(self, agent):
        # Arrange
        text = '''```json
{
    "anomaly_type": "latency_spike",
    "detected_at": "2026-06-18T15:00:00Z",
    "affected_components": [],
    "root_cause": {
        "summary": "GC pressure",
        "confidence": "high",
        "reasoning": "Heap at 92%"
    },
    "tools_used": ["query_flink_jobs"]
}
```'''

        # Act
        result = agent._parse_diagnosis(text)

        # Assert
        assert isinstance(result, DiagnosisReport)
        assert result.anomaly_type == "latency_spike"
        assert result.root_cause.confidence == "high"

    def test_invalid_json_returns_fallback(self, agent):
        # Arrange
        text = "This is not valid JSON at all"

        # Act
        result = agent._parse_diagnosis(text)

        # Assert
        assert isinstance(result, DiagnosisReport)
        assert result.anomaly_type == "parse_error"
        assert result.root_cause.confidence == "low"


class TestIncidentParsing:

    def test_valid_json_parses(self, agent):
        # Arrange
        diagnosis = DiagnosisReport(
            anomaly_type="test",
            detected_at="2026-06-18T15:00:00Z",
            affected_components=[],
            root_cause={"summary": "test", "confidence": "low", "reasoning": "test"},
            tools_used=[],
        )
        text = '''{
    "incident_id": "inc-001",
    "title": "Test incident",
    "severity": "HIGH",
    "summary": "Test summary",
    "anomaly_type": "test",
    "root_cause": "Test root cause",
    "affected_components": ["comp-a"],
    "timeline": ["event 1"],
    "recommended_actions": [{"action": "fix", "rationale": "why", "risk": "low", "requires_downtime": false}],
    "monitoring_notes": "watch it"
}'''

        # Act
        result = agent._parse_incident(text, diagnosis)

        # Assert
        assert isinstance(result, IncidentReport)
        assert result.severity == Severity.HIGH
        assert result.incident_id == "inc-001"

    def test_invalid_json_returns_fallback(self, agent):
        # Arrange
        diagnosis = DiagnosisReport(
            anomaly_type="backpressure",
            detected_at="2026-06-18T15:00:00Z",
            affected_components=[],
            root_cause={"summary": "slow sink", "confidence": "medium", "reasoning": "test"},
            tools_used=[],
        )

        # Act
        result = agent._parse_incident("not json", diagnosis)

        # Assert
        assert isinstance(result, IncidentReport)
        assert result.severity == Severity.MEDIUM
        assert "backpressure" in result.anomaly_type


class TestIsRetryable:

    def test_timeout_is_retryable(self, agent):
        # Arrange
        exc = anthropic.APITimeoutError(request=None)

        # Act / Assert
        assert agent._is_retryable(exc) is True

    def test_rate_limit_is_retryable(self, agent):
        # Arrange
        exc = anthropic.RateLimitError(
            message="rate limited",
            response=_fake_response(429),
            body=None,
        )

        # Act / Assert
        assert agent._is_retryable(exc) is True

    def test_internal_server_error_is_retryable(self, agent):
        # Arrange
        exc = anthropic.InternalServerError(
            message="internal error",
            response=_fake_response(500),
            body=None,
        )

        # Act / Assert
        assert agent._is_retryable(exc) is True

    def test_auth_error_is_not_retryable(self, agent):
        # Arrange
        exc = anthropic.AuthenticationError(
            message="invalid key",
            response=_fake_response(401),
            body=None,
        )

        # Act / Assert
        assert agent._is_retryable(exc) is False

    def test_bad_request_is_not_retryable(self, agent):
        # Arrange
        exc = anthropic.BadRequestError(
            message="bad request",
            response=_fake_response(400),
            body=None,
        )

        # Act / Assert
        assert agent._is_retryable(exc) is False

    def test_value_error_is_not_retryable(self, agent):
        # Arrange
        exc = ValueError("bad parse")

        # Act / Assert
        assert agent._is_retryable(exc) is False


class TestRetrySubagent:

    @pytest.mark.asyncio
    async def test_succeeds_on_first_try(self, agent):
        # Arrange
        factory = AsyncMock(return_value="result")

        # Act
        result = await agent._retry_subagent("TestAgent", factory)

        # Assert
        assert result == "result"
        assert factory.call_count == 1

    @pytest.mark.asyncio
    async def test_retries_on_transient_then_succeeds(self, agent, monkeypatch):
        # Arrange
        monkeypatch.setattr("streamops_mcp.agent.monitor.config.agent_max_retries", 2)
        monkeypatch.setattr("streamops_mcp.agent.monitor.config.agent_retry_base_delay", 0.0)
        call_count = 0

        async def factory():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise anthropic.APITimeoutError(request=None)
            return "recovered"

        # Act
        result = await agent._retry_subagent("TestAgent", factory)

        # Assert
        assert result == "recovered"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_raises_after_all_retries_exhausted(self, agent, monkeypatch):
        # Arrange
        monkeypatch.setattr("streamops_mcp.agent.monitor.config.agent_max_retries", 1)
        monkeypatch.setattr("streamops_mcp.agent.monitor.config.agent_retry_base_delay", 0.0)

        async def factory():
            raise anthropic.APITimeoutError(request=None)

        # Act / Assert
        with pytest.raises(anthropic.APITimeoutError):
            await agent._retry_subagent("TestAgent", factory)

    @pytest.mark.asyncio
    async def test_no_retry_on_permanent_error(self, agent, monkeypatch):
        # Arrange
        monkeypatch.setattr("streamops_mcp.agent.monitor.config.agent_max_retries", 2)
        monkeypatch.setattr("streamops_mcp.agent.monitor.config.agent_retry_base_delay", 0.0)
        call_count = 0

        async def factory():
            nonlocal call_count
            call_count += 1
            raise anthropic.AuthenticationError(
                message="invalid key",
                response=_fake_response(401),
                body=None,
            )

        # Act / Assert
        with pytest.raises(anthropic.AuthenticationError):
            await agent._retry_subagent("TestAgent", factory)
        assert call_count == 1


class TestFallbackReport:

    def test_produces_valid_incident_report(self):
        # Arrange
        diagnosis = DiagnosisReport(
            anomaly_type="latency_spike",
            detected_at="2026-06-23T12:00:00Z",
            affected_components=[
                {"name": "flink-job", "role": "processor", "status": "degraded", "evidence": "p99 > 5s"},
            ],
            root_cause={"summary": "GC pressure on TaskManager", "confidence": "high", "reasoning": "heap at 95%"},
            tools_used=["query_flink_jobs"],
        )

        # Act
        result = MonitorAgent._fallback_report(diagnosis)

        # Assert
        assert isinstance(result, IncidentReport)
        assert result.severity == Severity.MEDIUM
        assert "report agent unavailable" in result.title
        assert result.anomaly_type == "latency_spike"
        assert "flink-job" in result.affected_components
        assert result.requires_human_approval is True

    def test_handles_empty_components(self):
        # Arrange
        diagnosis = DiagnosisReport(
            anomaly_type="unknown",
            detected_at="2026-06-23T12:00:00Z",
            affected_components=[],
            root_cause={"summary": "unclear", "confidence": "low", "reasoning": "insufficient data"},
            tools_used=[],
        )

        # Act
        result = MonitorAgent._fallback_report(diagnosis)

        # Assert
        assert isinstance(result, IncidentReport)
        assert result.affected_components == []
        assert result.recommended_actions == []


def _make_diagnosis_with_claims(claim_confidences: list[Confidence]) -> DiagnosisReport:
    """Helper: build a DiagnosisReport with claims at specified confidence levels."""
    sources = [
        SourceRecord(
            source_id="src-001",
            tool_name="query_flink_jobs",
            retrieved_at="2026-06-23T12:00:00Z",
            raw_output="{}",
        ),
    ]
    claims = [
        ClaimRecord(
            claim_id=f"C{i:02d}",
            text=f"Claim {i} at {conf.value}",
            source_id="src-001",
            confidence=conf,
        )
        for i, conf in enumerate(claim_confidences, start=1)
    ]
    return DiagnosisReport(
        anomaly_type="latency_spike",
        detected_at="2026-06-23T12:00:00Z",
        sources=sources,
        claims=claims,
        affected_components=[],
        root_cause={"summary": "test", "confidence": "high", "reasoning": "test"},
        tools_used=["query_flink_jobs"],
    )


class TestConfidenceDistribution:

    def test_logs_distribution(self, agent, caplog):
        # Arrange
        diagnosis = _make_diagnosis_with_claims([
            Confidence.HIGH, Confidence.HIGH, Confidence.MEDIUM, Confidence.LOW,
        ])

        # Act
        with caplog.at_level("INFO", logger="streamops-mcp.monitor"):
            MonitorAgent._log_confidence_distribution(diagnosis)

        # Assert
        assert "2 HIGH" in caplog.text
        assert "1 MEDIUM" in caplog.text
        assert "1 LOW" in caplog.text
        assert "0 UNSOURCED" in caplog.text


class TestAllClaimsLowConfidence:

    def test_all_low_returns_true(self):
        # Arrange
        diagnosis = _make_diagnosis_with_claims([Confidence.LOW, Confidence.LOW])

        # Act / Assert
        assert MonitorAgent._all_claims_low_confidence(diagnosis) is True

    def test_all_unsourced_returns_true(self):
        # Arrange
        diagnosis = _make_diagnosis_with_claims([Confidence.UNSOURCED])

        # Act / Assert
        assert MonitorAgent._all_claims_low_confidence(diagnosis) is True

    def test_mixed_low_unsourced_returns_true(self):
        # Arrange
        diagnosis = _make_diagnosis_with_claims([Confidence.LOW, Confidence.UNSOURCED])

        # Act / Assert
        assert MonitorAgent._all_claims_low_confidence(diagnosis) is True

    def test_one_medium_returns_false(self):
        # Arrange
        diagnosis = _make_diagnosis_with_claims([
            Confidence.LOW, Confidence.MEDIUM, Confidence.UNSOURCED,
        ])

        # Act / Assert
        assert MonitorAgent._all_claims_low_confidence(diagnosis) is False

    def test_no_claims_returns_false(self):
        # Arrange
        diagnosis = _make_diagnosis_with_claims([])

        # Act / Assert
        assert MonitorAgent._all_claims_low_confidence(diagnosis) is False


class TestExtractLowConfidenceClaims:

    def test_extracts_low_and_unsourced(self):
        # Arrange
        diagnosis = _make_diagnosis_with_claims([
            Confidence.HIGH, Confidence.LOW, Confidence.MEDIUM, Confidence.UNSOURCED,
        ])

        # Act
        result = MonitorAgent._extract_low_confidence_claims(diagnosis)

        # Assert
        assert len(result) == 2
        assert "Claim 2 at LOW" in result
        assert "Claim 4 at UNSOURCED" in result

    def test_returns_empty_when_all_high(self):
        # Arrange
        diagnosis = _make_diagnosis_with_claims([Confidence.HIGH, Confidence.HIGH])

        # Act
        result = MonitorAgent._extract_low_confidence_claims(diagnosis)

        # Assert
        assert result == []

    def test_returns_empty_when_no_claims(self):
        # Arrange
        diagnosis = _make_diagnosis_with_claims([])

        # Act
        result = MonitorAgent._extract_low_confidence_claims(diagnosis)

        # Assert
        assert result == []


def _api_response(text: str, stop_reason: str = "end_turn") -> SimpleNamespace:
    """Build a fake Claude messages.create response with a single text block."""
    return SimpleNamespace(
        content=[SimpleNamespace(type="text", text=text)],
        stop_reason=stop_reason,
    )


def _mock_client(*responses) -> SimpleNamespace:
    """A stand-in Anthropic async client whose messages.create yields the given
    responses (or raises, if a response is an Exception) in order."""
    return SimpleNamespace(
        messages=SimpleNamespace(create=AsyncMock(side_effect=list(responses)))
    )


_DIAGNOSIS_JSON = json.dumps({
    "anomaly_type": "throughput_drop",
    "detected_at": "2026-07-03T12:00:00Z",
    "sources": [
        {"source_id": "src-001", "tool_name": "query_flink_jobs",
         "retrieved_at": "2026-07-03T12:00:00Z", "raw_output": "{}"},
    ],
    "claims": [
        {"claim_id": "C01", "text": "Consumer lag is 45000 on partition 2",
         "source_id": "src-001", "confidence": "HIGH"},
    ],
    "affected_components": [
        {"name": "kafka-consumer", "role": "consumer", "status": "degraded",
         "evidence": "lag 45000"},
    ],
    "root_cause": {"summary": "slow downstream sink", "confidence": "high",
                   "reasoning": "lag climbing steadily", "supporting_metrics": []},
    "tools_used": ["query_flink_jobs"],
    "raw_evidence": [],
})

_LOW_DIAGNOSIS_JSON = json.dumps({
    "anomaly_type": "latency_spike",
    "detected_at": "2026-07-03T12:00:00Z",
    "sources": [
        {"source_id": "src-001", "tool_name": "query_prometheus",
         "retrieved_at": "2026-07-03T12:00:00Z", "raw_output": "{}"},
    ],
    "claims": [
        {"claim_id": "C01", "text": "Possibly elevated latency",
         "source_id": "src-001", "confidence": "LOW"},
    ],
    "affected_components": [],
    "root_cause": {"summary": "unclear", "confidence": "low",
                   "reasoning": "single weak signal", "supporting_metrics": []},
    "tools_used": ["query_prometheus"],
    "raw_evidence": [],
})

_REPORT_JSON = json.dumps({
    "incident_id": "inc-001",
    "title": "Consumer lag spike on partition 2",
    "severity": "LOW",
    "summary": "Consumer lag elevated but within recoverable range.",
    "anomaly_type": "throughput_drop",
    "root_cause": "slow downstream sink",
    "affected_components": ["kafka-consumer"],
    "timeline": ["Lag began climbing at 12:00"],
    "recommended_actions": [],
    "monitoring_notes": "Watch consumer lag over the next 15 minutes.",
    "requires_human_approval": False,
})


class TestRunCycleOrchestration:
    """Drive the coordinator run_cycle end to end with a mocked Claude client.

    Covers the Monitor->Diagnostic->Report handoff, the all-low-confidence skip,
    the report-agent fallback, and the diagnostic-failure abort. escalate() is
    patched so this stays a coordinator test (escalation has its own suite)."""

    @pytest.mark.asyncio
    async def test_full_handoff_produces_incident_and_escalates(self, agent):
        # Arrange: detection finds an anomaly, diagnostic + report agents respond
        agent.client = _mock_client(
            _api_response("Anomaly detected: consumer lag spike on partition 2"),
            _api_response(_DIAGNOSIS_JSON),
            _api_response(_REPORT_JSON),
        )

        # Act
        with patch(
            "streamops_mcp.agent.monitor.escalate", new_callable=AsyncMock,
        ) as mock_escalate:
            result = await agent.run_cycle()

        # Assert: the handoff produced the report and routed it to escalation
        assert isinstance(result, IncidentReport)
        assert result.incident_id == "inc-001"
        assert agent.client.messages.create.call_count == 3  # detect -> diagnose -> report
        mock_escalate.assert_awaited_once()
        assert mock_escalate.await_args.args[0] is result

    @pytest.mark.asyncio
    async def test_no_anomaly_returns_none_and_skips_escalation(self, agent):
        # Arrange: detection reports a healthy system (no anomaly keywords)
        agent.client = _mock_client(
            _api_response("All systems healthy. Flink jobs running normally, no issues found."),
        )

        # Act
        with patch(
            "streamops_mcp.agent.monitor.escalate", new_callable=AsyncMock,
        ) as mock_escalate:
            result = await agent.run_cycle()

        # Assert: no diagnosis/report/escalation happens
        assert result is None
        assert agent.client.messages.create.call_count == 1
        mock_escalate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_all_low_confidence_skips_report_agent(self, agent):
        # Arrange: diagnosis returns only LOW-confidence claims
        agent.client = _mock_client(
            _api_response("Anomaly: latency spike detected"),
            _api_response(_LOW_DIAGNOSIS_JSON),
        )

        # Act
        with patch(
            "streamops_mcp.agent.monitor.escalate", new_callable=AsyncMock,
        ) as mock_escalate:
            result = await agent.run_cycle()

        # Assert: coordinator downgrades, never spawns the report agent or escalates
        assert result is None
        assert agent.client.messages.create.call_count == 2  # no third (report) call
        mock_escalate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_report_agent_failure_uses_fallback(self, agent, monkeypatch):
        # Arrange: report agent errors on every attempt
        monkeypatch.setattr("streamops_mcp.agent.monitor.config.agent_max_retries", 0)
        monkeypatch.setattr("streamops_mcp.agent.monitor.config.agent_retry_base_delay", 0.0)
        agent.client = _mock_client(
            _api_response("Anomaly: backpressure on operator chain"),
            _api_response(_DIAGNOSIS_JSON),
            anthropic.APITimeoutError(request=None),
        )

        # Act
        with patch(
            "streamops_mcp.agent.monitor.escalate", new_callable=AsyncMock,
        ) as mock_escalate:
            result = await agent.run_cycle()

        # Assert: a fallback incident is produced and still escalated
        assert isinstance(result, IncidentReport)
        assert "report agent unavailable" in result.title
        mock_escalate.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_diagnostic_failure_aborts_cycle(self, agent, monkeypatch):
        # Arrange: diagnostic agent errors on every attempt
        monkeypatch.setattr("streamops_mcp.agent.monitor.config.agent_max_retries", 0)
        monkeypatch.setattr("streamops_mcp.agent.monitor.config.agent_retry_base_delay", 0.0)
        agent.client = _mock_client(
            _api_response("Anomaly: checkpoint failure detected"),
            anthropic.APITimeoutError(request=None),
        )

        # Act
        with patch(
            "streamops_mcp.agent.monitor.escalate", new_callable=AsyncMock,
        ) as mock_escalate:
            result = await agent.run_cycle()

        # Assert: cycle aborts cleanly, nothing escalated
        assert result is None
        mock_escalate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cycle_binds_correlation_id_and_resets_after(self, agent):
        # Arrange: capture the correlation id as seen deep in the cycle (at escalation)
        from streamops_mcp.logging_setup import get_correlation_id

        agent.client = _mock_client(
            _api_response("Anomaly detected: consumer lag spike on partition 2"),
            _api_response(_DIAGNOSIS_JSON),
            _api_response(_REPORT_JSON),
        )
        seen: dict[str, str] = {}

        async def _capture(*args, **kwargs):
            seen["cid"] = get_correlation_id()

        # Act
        with patch(
            "streamops_mcp.agent.monitor.escalate", new=_capture,
        ):
            await agent.run_cycle()

        # Assert: a real cycle id was bound during the cycle, and it is cleared after
        assert seen["cid"].startswith("cyc-")
        assert get_correlation_id() == "-"

    @pytest.mark.asyncio
    async def test_two_cycles_get_distinct_correlation_ids(self, agent):
        # Arrange
        from streamops_mcp.logging_setup import get_correlation_id

        seen: list[str] = []

        async def _capture(*args, **kwargs):
            seen.append(get_correlation_id())

        def _fresh_client():
            return _mock_client(
                _api_response("Anomaly detected: consumer lag spike on partition 2"),
                _api_response(_DIAGNOSIS_JSON),
                _api_response(_REPORT_JSON),
            )

        # Act: run two independent cycles
        with patch("streamops_mcp.agent.monitor.escalate", new=_capture):
            agent.client = _fresh_client()
            await agent.run_cycle()
            agent.client = _fresh_client()
            await agent.run_cycle()

        # Assert: each cycle got its own id (no cross-cycle bleed)
        assert len(seen) == 2
        assert seen[0] != seen[1]
