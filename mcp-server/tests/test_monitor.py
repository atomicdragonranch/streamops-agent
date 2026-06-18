"""Tests for the MonitorAgent.

Tests the agent's internal logic (anomaly detection heuristic, JSON extraction,
parsing) without making actual Claude API calls.
"""

import pytest

from streamops_mcp.agent.monitor import MonitorAgent
from streamops_mcp.agent.schemas import DiagnosisReport, IncidentReport, Severity


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
