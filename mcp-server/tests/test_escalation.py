"""Tests for escalation logic."""

import pytest

from streamops_mcp.agent.escalation import escalate
from streamops_mcp.agent.schemas import IncidentReport, Severity
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
