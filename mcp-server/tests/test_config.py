"""Tests for StreamOps config."""

import pytest

from streamops_mcp.config import StreamOpsConfig


class TestConfig:

    def test_defaults(self):
        cfg = StreamOpsConfig()
        assert cfg.flink_url == "http://localhost:8081"
        assert cfg.prometheus_url == "http://localhost:9090"
        assert cfg.kafka_bootstrap == "localhost:9092"
        assert cfg.kafka_events_topic == "stream-events"
        assert cfg.kafka_alerts_topic == "stream-alerts"

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
