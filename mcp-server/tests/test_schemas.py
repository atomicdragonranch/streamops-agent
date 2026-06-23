"""Tests for structured output schemas."""

import json

import pytest

from streamops_mcp.agent.schemas import (
    ClaimRecord,
    Confidence,
    ConflictRecord,
    DiagnosisReport,
    DiagnosticToReportHandoff,
    IncidentReport,
    MonitorToDiagnosticHandoff,
    Severity,
    SourceRecord,
)
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


class TestSourceRecord:

    def test_valid_source(self):
        # Arrange + Act
        source = SourceRecord(
            source_id="src-001",
            tool_name="query_flink_jobs",
            retrieved_at="2026-06-18T15:00:00Z",
            raw_output='{"jobs": [{"id": "abc123", "state": "RUNNING"}]}',
        )

        # Assert
        assert source.source_id == "src-001"
        assert source.tool_name == "query_flink_jobs"


class TestClaimRecord:

    def test_valid_claim(self):
        # Arrange + Act
        claim = ClaimRecord(
            claim_id="C01",
            text="Consumer lag is 45,000 on partition 2",
            source_id="src-002",
            confidence="HIGH",
        )

        # Assert
        assert claim.claim_id == "C01"
        assert claim.source_id == "src-002"
        assert claim.confidence == Confidence.HIGH

    def test_claim_references_source(self):
        # Arrange
        source = SourceRecord(
            source_id="src-003",
            tool_name="get_consumer_lag",
            retrieved_at="2026-06-18T15:01:00Z",
            raw_output="lag=45000",
        )
        claim = ClaimRecord(
            claim_id="C02",
            text="Consumer lag exceeds threshold",
            source_id=source.source_id,
            confidence="MEDIUM",
        )

        # Assert
        assert claim.source_id == source.source_id
        assert claim.confidence == Confidence.MEDIUM


class TestConflictRecord:

    def test_valid_conflict(self):
        # Arrange + Act
        conflict = ConflictRecord(
            conflict_id="conf-001",
            topic="Flink job health status",
            claim_a_id="C01",
            claim_b_id="C02",
        )

        # Assert
        assert conflict.resolution == "unresolved"
        assert conflict.conflict_id == "conf-001"

    def test_conflict_defaults_to_unresolved(self):
        # Arrange + Act
        conflict = ConflictRecord(
            conflict_id="conf-002",
            topic="Consumer lag trend",
            claim_a_id="C03",
            claim_b_id="C04",
        )

        # Assert
        assert conflict.resolution == "unresolved"
        assert conflict.notes == ""

    def test_conflict_with_notes(self):
        # Arrange + Act
        conflict = ConflictRecord(
            conflict_id="conf-003",
            topic="Checkpoint duration",
            claim_a_id="C05",
            claim_b_id="C06",
            resolution="unresolved",
            notes="Flink REST API reports success but Prometheus shows timeout; possible metric lag",
        )

        # Assert
        assert "metric lag" in conflict.notes


class TestDiagnosisWithAttribution:

    def test_full_diagnosis_with_claims_and_sources(self):
        # Arrange
        data = {
            "anomaly_type": "latency_spike",
            "detected_at": "2026-06-18T15:00:00Z",
            "sources": [
                {
                    "source_id": "src-001",
                    "tool_name": "query_flink_jobs",
                    "retrieved_at": "2026-06-18T15:00:01Z",
                    "raw_output": '{"state": "RUNNING"}',
                },
                {
                    "source_id": "src-002",
                    "tool_name": "get_consumer_lag",
                    "retrieved_at": "2026-06-18T15:00:02Z",
                    "raw_output": "lag=45000",
                },
            ],
            "claims": [
                {
                    "claim_id": "C01",
                    "text": "Flink job is RUNNING but degraded",
                    "source_id": "src-001",
                    "confidence": "HIGH",
                },
                {
                    "claim_id": "C02",
                    "text": "Consumer lag is 45,000 on partition 2",
                    "source_id": "src-002",
                    "confidence": "HIGH",
                },
            ],
            "conflicts": [],
            "affected_components": [
                {
                    "name": "kafka-consumer",
                    "role": "Ingests events",
                    "status": "degraded",
                    "evidence": "latency_ms=2340",
                }
            ],
            "root_cause": {
                "summary": "GC pressure causing processing delays",
                "confidence": "high",
                "reasoning": "Heap at 92%, correlated with latency spikes",
            },
            "tools_used": ["query_flink_jobs", "get_consumer_lag"],
        }

        # Act
        report = DiagnosisReport.model_validate(data)

        # Assert
        assert len(report.sources) == 2
        assert len(report.claims) == 2
        assert report.claims[0].source_id == "src-001"
        assert report.claims[1].source_id == "src-002"

    def test_diagnosis_with_conflict(self):
        # Arrange
        data = {
            "anomaly_type": "checkpoint_failure",
            "detected_at": "2026-06-18T15:00:00Z",
            "sources": [
                {
                    "source_id": "src-001",
                    "tool_name": "get_checkpoint_stats",
                    "retrieved_at": "2026-06-18T15:00:01Z",
                    "raw_output": "status=COMPLETED",
                },
                {
                    "source_id": "src-002",
                    "tool_name": "query_metrics",
                    "retrieved_at": "2026-06-18T15:00:02Z",
                    "raw_output": "checkpoint_duration_ms=45000",
                },
            ],
            "claims": [
                {
                    "claim_id": "C01",
                    "text": "Checkpoint completed successfully",
                    "source_id": "src-001",
                    "confidence": "HIGH",
                },
                {
                    "claim_id": "C02",
                    "text": "Checkpoint took 45s, exceeding 30s threshold",
                    "source_id": "src-002",
                    "confidence": "MEDIUM",
                },
            ],
            "conflicts": [
                {
                    "conflict_id": "conf-001",
                    "topic": "Checkpoint health status",
                    "claim_a_id": "C01",
                    "claim_b_id": "C02",
                    "resolution": "unresolved",
                    "notes": "REST API reports success but duration exceeds threshold",
                },
            ],
            "affected_components": [],
            "root_cause": {
                "summary": "Conflicting checkpoint signals require coordinator review",
                "confidence": "low",
                "reasoning": "Cannot determine without resolving conflicting data",
            },
            "tools_used": ["get_checkpoint_stats", "query_metrics"],
        }

        # Act
        report = DiagnosisReport.model_validate(data)

        # Assert
        assert len(report.conflicts) == 1
        assert report.conflicts[0].resolution == "unresolved"
        assert report.conflicts[0].claim_a_id == "C01"
        assert report.conflicts[0].claim_b_id == "C02"

    def test_round_trip_with_attribution(self):
        # Arrange
        report = DiagnosisReport(
            anomaly_type="backpressure",
            detected_at="2026-06-18T15:00:00Z",
            sources=[
                SourceRecord(
                    source_id="src-001",
                    tool_name="query_flink_jobs",
                    retrieved_at="2026-06-18T15:00:01Z",
                    raw_output="backpressure=0.85",
                ),
            ],
            claims=[
                ClaimRecord(
                    claim_id="C01",
                    text="Backpressure ratio is 0.85",
                    source_id="src-001",
                    confidence=Confidence.HIGH,
                ),
            ],
            conflicts=[],
            affected_components=[],
            root_cause=RootCause(
                summary="Slow sink",
                confidence="high",
                reasoning="Backpressure on sink operator",
            ),
            tools_used=["query_flink_jobs"],
        )

        # Act
        json_str = report.model_dump_json()
        restored = DiagnosisReport.model_validate_json(json_str)

        # Assert
        assert len(restored.sources) == 1
        assert len(restored.claims) == 1
        assert restored.claims[0].source_id == restored.sources[0].source_id


class TestConfidence:

    def test_confidence_enum_values(self):
        # Assert
        assert Confidence.HIGH.value == "HIGH"
        assert Confidence.MEDIUM.value == "MEDIUM"
        assert Confidence.LOW.value == "LOW"
        assert Confidence.UNSOURCED.value == "UNSOURCED"
        assert len(Confidence) == 4

    def test_claim_with_high_confidence(self):
        # Arrange + Act
        claim = ClaimRecord(
            claim_id="C01",
            text="Consumer lag is 45,000",
            source_id="src-001",
            confidence=Confidence.HIGH,
        )

        # Assert
        assert claim.confidence == Confidence.HIGH

    def test_claim_with_unsourced_confidence(self):
        # Arrange + Act
        claim = ClaimRecord(
            claim_id="C02",
            text="Network issues may be contributing",
            source_id="src-none",
            confidence=Confidence.UNSOURCED,
        )

        # Assert
        assert claim.confidence == Confidence.UNSOURCED

    def test_confidence_from_string_coercion(self):
        # Arrange
        data = {
            "claim_id": "C03",
            "text": "Flink job is degraded",
            "source_id": "src-001",
            "confidence": "LOW",
        }

        # Act
        claim = ClaimRecord.model_validate(data)

        # Assert
        assert claim.confidence == Confidence.LOW

    def test_confidence_round_trip_serialization(self):
        # Arrange
        claim = ClaimRecord(
            claim_id="C04",
            text="Backpressure ratio is 0.85",
            source_id="src-001",
            confidence=Confidence.MEDIUM,
        )

        # Act
        json_str = claim.model_dump_json()
        restored = ClaimRecord.model_validate_json(json_str)

        # Assert
        assert restored.confidence == Confidence.MEDIUM

    def test_confidence_in_json_schema(self):
        # Act
        schema = DiagnosisReport.model_json_schema()

        # Assert
        schema_str = str(schema)
        assert "Confidence" in schema_str

    def test_mixed_confidence_claims_in_diagnosis(self):
        # Arrange
        report = DiagnosisReport(
            anomaly_type="latency_spike",
            detected_at="2026-06-18T15:00:00Z",
            sources=[
                SourceRecord(
                    source_id="src-001",
                    tool_name="query_flink_jobs",
                    retrieved_at="2026-06-18T15:00:01Z",
                    raw_output='{"state": "RUNNING"}',
                ),
            ],
            claims=[
                ClaimRecord(
                    claim_id="C01",
                    text="Job is running but slow",
                    source_id="src-001",
                    confidence=Confidence.HIGH,
                ),
                ClaimRecord(
                    claim_id="C02",
                    text="Possible GC pressure",
                    source_id="src-001",
                    confidence=Confidence.LOW,
                ),
                ClaimRecord(
                    claim_id="C03",
                    text="Network latency may be involved",
                    source_id="src-001",
                    confidence=Confidence.UNSOURCED,
                ),
            ],
            affected_components=[],
            root_cause=RootCause(
                summary="GC pressure suspected",
                confidence="medium",
                reasoning="Indirect evidence only",
            ),
            tools_used=["query_flink_jobs"],
        )

        # Act
        high_claims = [c for c in report.claims if c.confidence == Confidence.HIGH]
        low_claims = [c for c in report.claims if c.confidence in (Confidence.LOW, Confidence.UNSOURCED)]

        # Assert
        assert len(high_claims) == 1
        assert len(low_claims) == 2


class TestIncidentReportLowConfidence:

    def test_incident_with_low_confidence_claims(self):
        # Arrange
        data = {
            "incident_id": "inc-001",
            "title": "Latency spike on StreamOps Processor",
            "severity": "HIGH",
            "summary": "Processing latency exceeded SLA thresholds.",
            "anomaly_type": "latency_spike",
            "root_cause": "GC pressure on TaskManager",
            "affected_components": ["flink-operator"],
            "timeline": ["15:00 - Latency exceeded threshold"],
            "recommended_actions": [
                {
                    "action": "Increase TaskManager heap",
                    "rationale": "Reduce GC pressure",
                    "risk": "low",
                    "requires_downtime": True,
                }
            ],
            "low_confidence_claims": [
                "[LOW] Possible GC pressure",
                "[UNSOURCED] Network latency may be involved",
            ],
            "monitoring_notes": "Watch heap usage after restart",
        }

        # Act
        report = IncidentReport.model_validate(data)

        # Assert
        assert len(report.low_confidence_claims) == 2
        assert "[UNSOURCED]" in report.low_confidence_claims[1]

    def test_incident_defaults_to_empty_low_confidence(self):
        # Arrange
        data = {
            "incident_id": "inc-002",
            "title": "Healthy check",
            "severity": "LOW",
            "summary": "All systems nominal.",
            "anomaly_type": "none",
            "root_cause": "N/A",
            "affected_components": [],
            "timeline": [],
            "recommended_actions": [],
            "monitoring_notes": "None",
        }

        # Act
        report = IncidentReport.model_validate(data)

        # Assert
        assert report.low_confidence_claims == []


class TestDraftOnlyContract:

    def test_requires_human_approval_defaults_true(self):
        # Arrange
        data = {
            "incident_id": "inc-001",
            "title": "Test incident",
            "severity": "HIGH",
            "summary": "Test",
            "anomaly_type": "test",
            "root_cause": "Test",
            "affected_components": [],
            "timeline": [],
            "recommended_actions": [],
            "monitoring_notes": "None",
        }

        # Act
        report = IncidentReport.model_validate(data)

        # Assert
        assert report.requires_human_approval is True

    def test_requires_human_approval_explicit_false(self):
        # Arrange
        data = {
            "incident_id": "inc-002",
            "title": "Monitoring only",
            "severity": "LOW",
            "summary": "Passive monitoring, no action needed",
            "anomaly_type": "none",
            "root_cause": "N/A",
            "affected_components": [],
            "timeline": [],
            "recommended_actions": [],
            "requires_human_approval": False,
            "monitoring_notes": "None",
        }

        # Act
        report = IncidentReport.model_validate(data)

        # Assert
        assert report.requires_human_approval is False

    def test_requires_human_approval_in_json_schema(self):
        # Act
        schema = IncidentReport.model_json_schema()

        # Assert
        assert "requires_human_approval" in str(schema)


class TestMonitorToDiagnosticHandoff:

    def test_valid_handoff(self):
        # Arrange + Act
        handoff = MonitorToDiagnosticHandoff(
            anomaly_context="Latency spike detected on partition 2",
            schema_hint=DiagnosisReport.model_json_schema(),
        )

        # Assert
        assert handoff.anomaly_context == "Latency spike detected on partition 2"
        assert "anomaly_type" in str(handoff.schema_hint)

    def test_truncates_oversized_context(self, monkeypatch):
        # Arrange
        monkeypatch.setenv("STREAMOPS_AGENT_HANDOFF_MAX_CONTEXT_CHARS", "100")
        from streamops_mcp.config import StreamOpsConfig
        test_config = StreamOpsConfig()
        monkeypatch.setattr("streamops_mcp.agent.schemas.handoff.config", test_config)

        oversized = "x" * 200

        # Act
        handoff = MonitorToDiagnosticHandoff(
            anomaly_context=oversized,
            schema_hint={},
        )

        # Assert
        assert len(handoff.anomaly_context) == 100

    def test_round_trip_serialization(self):
        # Arrange
        handoff = MonitorToDiagnosticHandoff(
            anomaly_context="Backpressure ratio exceeded threshold",
            schema_hint=DiagnosisReport.model_json_schema(),
        )

        # Act
        json_str = handoff.model_dump_json()
        restored = MonitorToDiagnosticHandoff.model_validate_json(json_str)

        # Assert
        assert restored.anomaly_context == handoff.anomaly_context


class TestDiagnosticToReportHandoff:

    def test_valid_handoff(self):
        # Arrange
        diagnosis = DiagnosisReport(
            anomaly_type="latency_spike",
            detected_at="2026-06-18T15:00:00Z",
            sources=[],
            claims=[],
            conflicts=[],
            affected_components=[],
            root_cause=RootCause(
                summary="GC pressure",
                confidence="high",
                reasoning="Heap at 92%",
            ),
            tools_used=["query_flink_jobs"],
        )

        # Act
        handoff = DiagnosticToReportHandoff(
            diagnosis_json=diagnosis.model_dump_json(indent=2),
            schema_hint=IncidentReport.model_json_schema(),
        )

        # Assert
        assert "latency_spike" in handoff.diagnosis_json
        assert "incident_id" in str(handoff.schema_hint)

    def test_rejects_oversized_diagnosis(self, monkeypatch):
        # Arrange
        monkeypatch.setenv("STREAMOPS_AGENT_HANDOFF_MAX_CONTEXT_CHARS", "50")
        from streamops_mcp.config import StreamOpsConfig
        test_config = StreamOpsConfig()
        monkeypatch.setattr("streamops_mcp.agent.schemas.handoff.config", test_config)

        oversized_json = "x" * 100

        # Act + Assert
        with pytest.raises(ValueError, match="exceeds max handoff size"):
            DiagnosticToReportHandoff(
                diagnosis_json=oversized_json,
                schema_hint={},
            )

    def test_round_trip_serialization(self):
        # Arrange
        diagnosis = DiagnosisReport(
            anomaly_type="backpressure",
            detected_at="2026-06-18T15:00:00Z",
            sources=[],
            claims=[],
            conflicts=[],
            affected_components=[],
            root_cause=RootCause(
                summary="Slow sink",
                confidence="high",
                reasoning="Backpressure on sink",
            ),
            tools_used=["query_flink_jobs"],
        )
        handoff = DiagnosticToReportHandoff(
            diagnosis_json=diagnosis.model_dump_json(),
            schema_hint=IncidentReport.model_json_schema(),
        )

        # Act
        json_str = handoff.model_dump_json()
        restored = DiagnosticToReportHandoff.model_validate_json(json_str)

        # Assert
        assert restored.diagnosis_json == handoff.diagnosis_json

    def test_default_max_context_is_50k(self):
        # Arrange
        large_but_under_limit = "a" * 49_000

        # Act
        handoff = DiagnosticToReportHandoff(
            diagnosis_json=large_but_under_limit,
            schema_hint={},
        )

        # Assert
        assert len(handoff.diagnosis_json) == 49_000
