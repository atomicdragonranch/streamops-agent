"""Tests for StreamOps config."""

import pytest
from pydantic import ValidationError

from streamops_mcp.config import StreamOpsConfig


class TestConfig:
    def test_defaults(self):
        cfg = StreamOpsConfig()
        assert cfg.flink_url == "http://localhost:8081"
        assert cfg.prometheus_url == "http://localhost:9090"
        assert cfg.kafka_bootstrap == "localhost:9092"
        assert cfg.kafka_events_topic == "stream-events"
        assert cfg.kafka_alerts_topic == "stream-alerts"
        # Fork defaults preserve single-agent behavior; hypothesis mode defaults to map.
        assert cfg.agent_diagnostic_forks == 1
        assert cfg.agent_hypothesis_mode == "map"
        # Cross-cycle volatility gauge defaults (issue #77).
        assert cfg.agent_incident_ongoing_gap == 1
        assert cfg.agent_incident_worsen_pct == 0.25
        assert cfg.agent_incident_dedup is True

    def test_invalid_worsen_pct_rejected(self, monkeypatch):
        # Arrange: a non-numeric threshold must fail fast at startup
        monkeypatch.setenv("STREAMOPS_AGENT_INCIDENT_WORSEN_PCT", "not-a-number")

        # Act + Assert
        with pytest.raises(ValidationError):
            StreamOpsConfig()

    def test_hypothesis_mode_override(self, monkeypatch):
        monkeypatch.setenv("STREAMOPS_AGENT_HYPOTHESIS_MODE", "llm")
        cfg = StreamOpsConfig()
        assert cfg.agent_hypothesis_mode == "llm"

    def test_invalid_hypothesis_mode_rejected(self, monkeypatch):
        # Arrange: an unknown mode must fail fast at startup, not silently pass
        monkeypatch.setenv("STREAMOPS_AGENT_HYPOTHESIS_MODE", "bogus")

        # Act + Assert
        with pytest.raises(ValidationError):
            StreamOpsConfig()

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("STREAMOPS_FLINK_URL", "http://flink-prod:8081")
        monkeypatch.setenv("STREAMOPS_KAFKA_BOOTSTRAP", "kafka-prod:9092")
        cfg = StreamOpsConfig()
        assert cfg.flink_url == "http://flink-prod:8081"
        assert cfg.kafka_bootstrap == "kafka-prod:9092"

    def test_partial_override(self, monkeypatch):
        monkeypatch.setenv("STREAMOPS_PROMETHEUS_URL", "http://prom:9090")
        cfg = StreamOpsConfig()
        assert cfg.prometheus_url == "http://prom:9090"
        assert cfg.flink_url == "http://localhost:8081"
