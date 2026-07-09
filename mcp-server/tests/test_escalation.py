"""Tests for escalation logic."""

from unittest.mock import MagicMock

import pytest

from streamops_mcp.agent import escalation
from streamops_mcp.agent.escalation import escalate
from streamops_mcp.agent.schemas import (
    ClaimRecord,
    Confidence,
    ConflictRecord,
    DiagnosisReport,
    IncidentReport,
    RootCause,
    Severity,
    SourceRecord,
)
from streamops_mcp.agent.schemas.diagnosis import AffectedComponent
from streamops_mcp.agent.schemas.incident import RecommendedAction


def _make_report(severity: Severity) -> IncidentReport:
    return IncidentReport(
        incident_id="test-001",
        title="Test incident",
        severity=severity,
        summary="Test summary",
        anomaly_type="test",
        root_cause="Test root cause",
        affected_components=["test-component"],
        timeline=["Event occurred"],
        recommended_actions=[
            RecommendedAction(
                action="Fix it",
                rationale="Because",
                risk="low",
                requires_downtime=False,
            )
        ],
        monitoring_notes="Watch the thing",
    )


def _make_diagnosis(*, conflict_resolution: str | None = "unresolved") -> DiagnosisReport:
    """Build a valid diagnosis with two attributed claims.

    When conflict_resolution is given, adds a conflict between the two claims
    with that resolution state; None means no conflict at all.
    """
    conflicts = []
    if conflict_resolution is not None:
        conflicts = [
            ConflictRecord(
                conflict_id="conf-001",
                topic="consumer lag value",
                claim_a_id="C01",
                claim_b_id="C02",
                resolution=conflict_resolution,
                notes="Prometheus and Kafka disagree on the lag",
            )
        ]
    return DiagnosisReport(
        anomaly_type="latency_spike",
        detected_at="2026-07-09T12:00:00Z",
        sources=[
            SourceRecord(
                source_id="src-001",
                tool_name="query_flink_jobs",
                retrieved_at="2026-07-09T12:00:00Z",
                raw_output="{}",
            )
        ],
        claims=[
            ClaimRecord(
                claim_id="C01", text="Lag is 45000", source_id="src-001", confidence=Confidence.HIGH
            ),
            ClaimRecord(
                claim_id="C02",
                text="Lag is 12000",
                source_id="src-001",
                confidence=Confidence.MEDIUM,
            ),
        ],
        conflicts=conflicts,
        affected_components=[
            AffectedComponent(
                name="kafka-consumer", role="consumes", status="degraded", evidence="lag rising"
            )
        ],
        root_cause=RootCause(summary="Backpressure", confidence="medium", reasoning="chain"),
        tools_used=["query_flink_jobs"],
    )


@pytest.fixture
def mock_audit(monkeypatch):
    """Replace the module-level audit logger so tests inspect its call, not disk."""
    fake = MagicMock()
    fake.log_incident = MagicMock(return_value={})
    monkeypatch.setattr(escalation, "_audit", fake)
    return fake


class TestEscalation:
    @pytest.mark.asyncio
    async def test_low_severity_logs_only(self, caplog):
        # Arrange
        report = _make_report(Severity.LOW)

        # Act
        with caplog.at_level("INFO"):
            await escalate(report)

        # Assert
        assert "[LOW]" in caplog.text

    @pytest.mark.asyncio
    async def test_medium_severity_logs_warning(self, caplog):
        # Arrange
        report = _make_report(Severity.MEDIUM)

        # Act
        with caplog.at_level("WARNING"):
            await escalate(report)

        # Assert
        assert "[MEDIUM]" in caplog.text

    @pytest.mark.asyncio
    async def test_high_severity_logs_error(self, caplog):
        # Arrange
        report = _make_report(Severity.HIGH)

        # Act
        with caplog.at_level("ERROR"):
            await escalate(report)

        # Assert
        assert "[HIGH]" in caplog.text

    @pytest.mark.asyncio
    async def test_critical_severity_prompts_human(self, caplog, monkeypatch):
        # Arrange
        report = _make_report(Severity.CRITICAL)
        monkeypatch.setattr("builtins.input", lambda: "y")

        # Act
        with caplog.at_level("INFO"):
            await escalate(report)

        # Assert
        assert "[CRITICAL]" in caplog.text
        assert "approved" in caplog.text

    @pytest.mark.asyncio
    async def test_critical_human_rejects(self, caplog, monkeypatch):
        # Arrange
        report = _make_report(Severity.CRITICAL)
        monkeypatch.setattr("builtins.input", lambda: "n")

        # Act
        with caplog.at_level("INFO"):
            await escalate(report)

        # Assert
        assert "rejected" in caplog.text

    @pytest.mark.asyncio
    async def test_critical_no_stdin(self, caplog, monkeypatch):
        # Arrange
        report = _make_report(Severity.CRITICAL)
        monkeypatch.setattr("builtins.input", lambda: (_ for _ in ()).throw(EOFError))

        # Act
        await escalate(report)

        # Assert
        assert "No human input" in caplog.text


class TestUnresolvedConflictEscalation:
    """A diagnosis with unresolved conflicts must never pass silently."""

    @pytest.mark.asyncio
    async def test_unresolved_conflict_is_surfaced_and_gated(self, caplog, monkeypatch, mock_audit):
        # Arrange: a MEDIUM incident whose diagnosis has an unresolved conflict
        report = _make_report(Severity.MEDIUM)
        diagnosis = _make_diagnosis(conflict_resolution="unresolved")
        monkeypatch.setattr("builtins.input", lambda: "y")

        # Act
        with caplog.at_level("INFO"):
            await escalate(report, diagnosis=diagnosis)

        # Assert: surfaced at ERROR, human gate ran, and the audit records it
        assert "UNRESOLVED" in caplog.text
        assert "accepted diagnosis" in caplog.text
        kwargs = mock_audit.log_incident.call_args.kwargs
        assert kwargs["unresolved_conflict_count"] == 1
        assert kwargs["conflicts_acknowledged"] is True

    @pytest.mark.asyncio
    async def test_unresolved_conflict_human_rejects(self, caplog, monkeypatch, mock_audit):
        # Arrange
        report = _make_report(Severity.MEDIUM)
        diagnosis = _make_diagnosis(conflict_resolution="unresolved")
        monkeypatch.setattr("builtins.input", lambda: "n")

        # Act
        with caplog.at_level("INFO"):
            await escalate(report, diagnosis=diagnosis)

        # Assert
        assert "rejected diagnosis" in caplog.text
        assert mock_audit.log_incident.call_args.kwargs["conflicts_acknowledged"] is False

    @pytest.mark.asyncio
    async def test_unresolved_conflict_no_stdin_still_not_dropped(
        self, caplog, monkeypatch, mock_audit
    ):
        # Arrange: automated run, no human available
        report = _make_report(Severity.LOW)
        diagnosis = _make_diagnosis(conflict_resolution="unresolved")
        monkeypatch.setattr("builtins.input", lambda: (_ for _ in ()).throw(EOFError))

        # Act
        with caplog.at_level("WARNING"):
            await escalate(report, diagnosis=diagnosis)

        # Assert: surfaced and audited even though nobody could acknowledge
        assert "UNRESOLVED" in caplog.text
        kwargs = mock_audit.log_incident.call_args.kwargs
        assert kwargs["unresolved_conflict_count"] == 1
        assert kwargs["conflicts_acknowledged"] is None

    @pytest.mark.asyncio
    async def test_resolved_conflict_is_not_gated(self, monkeypatch, mock_audit):
        # Arrange: the conflict was resolved, so no gate and no prompt
        report = _make_report(Severity.MEDIUM)
        diagnosis = _make_diagnosis(conflict_resolution="resolved_a")

        def _fail_if_called():
            raise AssertionError("input() must not be called for a resolved conflict")

        monkeypatch.setattr("builtins.input", _fail_if_called)

        # Act
        await escalate(report, diagnosis=diagnosis)

        # Assert
        assert mock_audit.log_incident.call_args.kwargs["unresolved_conflict_count"] == 0

    @pytest.mark.asyncio
    async def test_no_conflicts_unchanged(self, monkeypatch, mock_audit):
        # Arrange: diagnosis with no conflicts at all
        report = _make_report(Severity.MEDIUM)
        diagnosis = _make_diagnosis(conflict_resolution=None)

        def _fail_if_called():
            raise AssertionError("input() must not be called with no conflicts")

        monkeypatch.setattr("builtins.input", _fail_if_called)

        # Act
        await escalate(report, diagnosis=diagnosis)

        # Assert
        kwargs = mock_audit.log_incident.call_args.kwargs
        assert kwargs["unresolved_conflict_count"] == 0
        assert kwargs["conflicts_acknowledged"] is None

    @pytest.mark.asyncio
    async def test_critical_with_conflict_prompts_once_not_twice(
        self, caplog, monkeypatch, mock_audit
    ):
        # Arrange: CRITICAL already gates; conflicts are surfaced but not double-prompted
        report = _make_report(Severity.CRITICAL)
        diagnosis = _make_diagnosis(conflict_resolution="unresolved")
        calls = {"n": 0}

        def _count_input():
            calls["n"] += 1
            return "y"

        monkeypatch.setattr("builtins.input", _count_input)

        # Act
        with caplog.at_level("ERROR"):
            await escalate(report, diagnosis=diagnosis)

        # Assert: conflicts surfaced, single human prompt (the CRITICAL one)
        assert "UNRESOLVED" in caplog.text
        assert calls["n"] == 1
        kwargs = mock_audit.log_incident.call_args.kwargs
        assert kwargs["unresolved_conflict_count"] == 1
        assert kwargs["conflicts_acknowledged"] is None  # gated by severity, not separately
