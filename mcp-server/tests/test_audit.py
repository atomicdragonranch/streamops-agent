"""Tests for audit trail logging."""

import json

from streamops_mcp.agent.audit import AuditLogger
from streamops_mcp.agent.schemas import (
    DiagnosisReport,
    IncidentReport,
    Severity,
    SourceRecord,
)
from streamops_mcp.agent.schemas.diagnosis import RootCause
from streamops_mcp.agent.schemas.incident import RecommendedAction


def _make_report(
    severity: Severity = Severity.HIGH,
    anomaly_type: str = "latency_spike",
) -> IncidentReport:
    return IncidentReport(
        incident_id="inc-001",
        title="Test incident",
        severity=severity,
        summary="Test summary",
        anomaly_type=anomaly_type,
        root_cause="Test root cause",
        affected_components=["flink-operator"],
        timeline=["15:00 - Detected"],
        recommended_actions=[
            RecommendedAction(
                action="Restart TaskManager",
                rationale="Clear stuck state",
                risk="low",
                requires_downtime=True,
            )
        ],
        low_confidence_claims=["[LOW] Possible GC pressure"],
        monitoring_notes="Watch heap usage",
    )


def _make_diagnosis() -> DiagnosisReport:
    return DiagnosisReport(
        anomaly_type="latency_spike",
        detected_at="2026-06-18T15:00:00Z",
        sources=[
            SourceRecord(
                source_id="src-001",
                tool_name="query_flink_jobs",
                retrieved_at="2026-06-18T15:00:01Z",
                raw_output='{"state": "RUNNING"}',
            ),
            SourceRecord(
                source_id="src-002",
                tool_name="get_consumer_lag",
                retrieved_at="2026-06-18T15:00:02Z",
                raw_output="lag=45000",
            ),
        ],
        claims=[],
        conflicts=[],
        affected_components=[],
        root_cause=RootCause(
            summary="GC pressure",
            confidence="high",
            reasoning="Heap at 92%",
        ),
        tools_used=["query_flink_jobs", "get_consumer_lag"],
    )


class TestAuditLogger:

    def test_log_incident_creates_file(self, tmp_path):
        # Arrange
        log_path = tmp_path / "audit" / "incidents.jsonl"
        audit = AuditLogger(path=log_path)

        # Act
        audit.log_incident(_make_report())

        # Assert
        assert log_path.exists()
        lines = log_path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1

    def test_log_incident_entry_fields(self, tmp_path):
        # Arrange
        log_path = tmp_path / "incidents.jsonl"
        audit = AuditLogger(path=log_path)
        report = _make_report()

        # Act
        entry = audit.log_incident(report, human_approved=True)

        # Assert
        assert entry["incident_id"] == "inc-001"
        assert entry["severity"] == "HIGH"
        assert entry["anomaly_type"] == "latency_spike"
        assert entry["human_approved"] is True
        assert "timestamp" in entry
        assert len(entry["recommended_actions"]) == 1
        assert entry["low_confidence_claims"] == ["[LOW] Possible GC pressure"]

    def test_log_incident_with_diagnosis(self, tmp_path):
        # Arrange
        log_path = tmp_path / "incidents.jsonl"
        audit = AuditLogger(path=log_path)

        # Act
        entry = audit.log_incident(_make_report(), diagnosis=_make_diagnosis())

        # Assert
        assert entry["diagnosis"]["sources_consulted"] == ["query_flink_jobs", "get_consumer_lag"]
        assert entry["diagnosis"]["claim_count"] == 0
        assert entry["diagnosis"]["conflict_count"] == 0
        assert entry["diagnosis"]["tools_used"] == ["query_flink_jobs", "get_consumer_lag"]

    def test_log_incident_without_diagnosis(self, tmp_path):
        # Arrange
        log_path = tmp_path / "incidents.jsonl"
        audit = AuditLogger(path=log_path)

        # Act
        entry = audit.log_incident(_make_report())

        # Assert
        assert "diagnosis" not in entry

    def test_appends_multiple_entries(self, tmp_path):
        # Arrange
        log_path = tmp_path / "incidents.jsonl"
        audit = AuditLogger(path=log_path)

        # Act
        audit.log_incident(_make_report(severity=Severity.LOW))
        audit.log_incident(_make_report(severity=Severity.HIGH))
        audit.log_incident(_make_report(severity=Severity.CRITICAL))

        # Assert
        lines = log_path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 3
        severities = [json.loads(line)["severity"] for line in lines]
        assert severities == ["LOW", "HIGH", "CRITICAL"]

    def test_human_approved_none_when_not_critical(self, tmp_path):
        # Arrange
        log_path = tmp_path / "incidents.jsonl"
        audit = AuditLogger(path=log_path)

        # Act
        entry = audit.log_incident(_make_report(severity=Severity.MEDIUM))

        # Assert
        assert entry["human_approved"] is None


class TestAuditQuery:

    def test_query_all(self, tmp_path):
        # Arrange
        log_path = tmp_path / "incidents.jsonl"
        audit = AuditLogger(path=log_path)
        audit.log_incident(_make_report(severity=Severity.LOW))
        audit.log_incident(_make_report(severity=Severity.HIGH))

        # Act
        results = audit.query()

        # Assert
        assert len(results) == 2

    def test_query_by_severity(self, tmp_path):
        # Arrange
        log_path = tmp_path / "incidents.jsonl"
        audit = AuditLogger(path=log_path)
        audit.log_incident(_make_report(severity=Severity.LOW))
        audit.log_incident(_make_report(severity=Severity.HIGH))
        audit.log_incident(_make_report(severity=Severity.HIGH))

        # Act
        results = audit.query(severity="HIGH")

        # Assert
        assert len(results) == 2
        assert all(r["severity"] == "HIGH" for r in results)

    def test_query_by_anomaly_type(self, tmp_path):
        # Arrange
        log_path = tmp_path / "incidents.jsonl"
        audit = AuditLogger(path=log_path)
        audit.log_incident(_make_report(anomaly_type="latency_spike"))
        audit.log_incident(_make_report(anomaly_type="backpressure"))
        audit.log_incident(_make_report(anomaly_type="latency_spike"))

        # Act
        results = audit.query(anomaly_type="backpressure")

        # Assert
        assert len(results) == 1
        assert results[0]["anomaly_type"] == "backpressure"

    def test_query_with_limit(self, tmp_path):
        # Arrange
        log_path = tmp_path / "incidents.jsonl"
        audit = AuditLogger(path=log_path)
        for _ in range(10):
            audit.log_incident(_make_report())

        # Act
        results = audit.query(limit=3)

        # Assert
        assert len(results) == 3

    def test_query_empty_log(self, tmp_path):
        # Arrange
        log_path = tmp_path / "incidents.jsonl"
        audit = AuditLogger(path=log_path)

        # Act
        results = audit.query()

        # Assert
        assert results == []

    def test_query_nonexistent_file(self, tmp_path):
        # Arrange
        audit = AuditLogger(path=tmp_path / "does_not_exist.jsonl")

        # Act
        results = audit.query()

        # Assert
        assert results == []
