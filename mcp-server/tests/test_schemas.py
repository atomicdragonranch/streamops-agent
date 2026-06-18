"""Tests for structured output schemas."""

import json

import pytest

from streamops_mcp.agent.schemas import DiagnosisReport, IncidentReport, Severity
from streamops_mcp.agent.schemas.diagnosis import AffectedComponent, RootCause
from streamops_mcp.agent.schemas.incident import RecommendedAction


class TestDiagnosisReport:

    def test_valid_diagnosis(self):
        # Arrange
        data = {
            "anomaly_type": "latency_spike",
            "detected_at": "2026-06-18T15:00:00Z",
            "affected_components": [
                {
                    "name": "kafka-consumer",
                    "role": "Ingests events from stream-events topic",
                    "status": "degraded",
                    "evidence": "latency_ms=2340 (threshold=200)",
                }
            ],
            "root_cause": {
                "summary": "GC pressure on TaskManager causing processing delays",
                "confidence": "high",
                "reasoning": "Heap at 92%, GC pause times correlate with latency spikes",
                "supporting_metrics": ["heap_usage_percent=92", "gc_pause_ms=450"],
            },
            "tools_used": ["query_flink_jobs", "query_metrics", "search_logs"],
            "raw_evidence": ["Flink job abc123 RUNNING but degraded"],
        }

        # Act
        report = DiagnosisReport.model_validate(data)

        # Assert
        assert report.anomaly_type == "latency_spike"
        assert len(report.affected_components) == 1
        assert report.root_cause.confidence == "high"
        assert len(report.tools_used) == 3

    def test_minimal_diagnosis(self):
        # Arrange
        data = {
            "anomaly_type": "unknown",
            "detected_at": "2026-06-18T15:00:00Z",
            "affected_components": [],
            "root_cause": {
                "summary": "Under investigation",
                "confidence": "low",
                "reasoning": "Insufficient data",
            },
            "tools_used": [],
        }

        # Act
        report = DiagnosisReport.model_validate(data)

        # Assert
        assert report.anomaly_type == "unknown"
        assert report.raw_evidence == []

    def test_json_schema_generation(self):
        # Act
        schema = DiagnosisReport.model_json_schema()

        # Assert
        assert "anomaly_type" in schema["properties"]
        assert "root_cause" in schema["properties"]

    def test_round_trip_serialization(self):
        # Arrange
        report = DiagnosisReport(
            anomaly_type="backpressure",
            detected_at="2026-06-18T15:00:00Z",
            affected_components=[
                AffectedComponent(
                    name="flink-operator",
                    role="Window aggregation",
                    status="degraded",
                    evidence="backpressure_ratio=0.85",
                )
            ],
            root_cause=RootCause(
                summary="Slow sink causing upstream backpressure",
                confidence="high",
                reasoning="Backpressure ratio 0.85 on sink operator",
            ),
            tools_used=["query_flink_jobs"],
        )

        # Act
        json_str = report.model_dump_json()
        restored = DiagnosisReport.model_validate_json(json_str)

        # Assert
        assert restored.anomaly_type == report.anomaly_type
        assert restored.root_cause.summary == report.root_cause.summary


class TestIncidentReport:

    def test_valid_incident(self):
        # Arrange
        data = {
            "incident_id": "inc-001",
            "title": "Checkpoint timeout on StreamOps Processor",
            "severity": "CRITICAL",
            "summary": "Flink checkpoints failing due to state backend pressure. Risk of data loss if job restarts.",
            "anomaly_type": "checkpoint_failure",
            "root_cause": "RocksDB compaction stalled under write amplification",
            "affected_components": ["state-backend", "checkpoint-coordinator"],
            "timeline": [
                "15:00 - Checkpoint duration exceeds 30s",
                "15:02 - Checkpoint fails with timeout",
                "15:03 - Alert fired by anomaly detector",
            ],
            "recommended_actions": [
                {
                    "action": "Increase state.backend.rocksdb.compaction.level.max-size-level-base to 256MB",
                    "rationale": "Reduces write amplification by allowing larger SST files",
                    "risk": "low",
                    "requires_downtime": True,
                }
            ],
            "monitoring_notes": "Watch checkpoint duration and compaction metrics for 30 minutes after restart",
        }

        # Act
        report = IncidentReport.model_validate(data)

        # Assert
        assert report.severity == Severity.CRITICAL
        assert len(report.recommended_actions) == 1
        assert report.recommended_actions[0].requires_downtime is True

    def test_severity_enum(self):
        # Assert
        assert Severity.LOW.value == "LOW"
        assert Severity.CRITICAL.value == "CRITICAL"
        assert len(Severity) == 4

    def test_json_schema_includes_severity_enum(self):
        # Act
        schema = IncidentReport.model_json_schema()

        # Assert
        assert "Severity" in json.dumps(schema)


class TestRecommendedAction:

    def test_action_with_downtime(self):
        # Arrange
        action = RecommendedAction(
            action="Restart Flink job with larger heap",
            rationale="Current heap insufficient for state size",
            risk="medium",
            requires_downtime=True,
        )

        # Assert
        assert action.requires_downtime is True
        assert action.risk == "medium"

    def test_action_no_downtime(self):
        # Arrange
        action = RecommendedAction(
            action="Scale Kafka consumer parallelism to 8",
            rationale="Current parallelism insufficient for input rate",
            risk="none",
            requires_downtime=False,
        )

        # Assert
        assert action.requires_downtime is False
