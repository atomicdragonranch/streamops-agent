"""Tests for runbook loading and injection."""

import pytest

from streamops_mcp.prompts import list_runbooks, load_runbook


class TestLoadRunbook:

    def test_loads_latency_spike(self):
        # Act
        content = load_runbook("latency_spike")

        # Assert
        assert content is not None
        assert "GC pressure" in content
        assert "Diagnostic Steps" in content

    def test_loads_checkpoint_failure(self):
        # Act
        content = load_runbook("checkpoint_failure")

        # Assert
        assert content is not None
        assert "State size" in content

    def test_loads_backpressure(self):
        # Act
        content = load_runbook("backpressure")

        # Assert
        assert content is not None
        assert "bottleneck" in content.lower()

    def test_loads_throughput_drop(self):
        # Act
        content = load_runbook("throughput_drop")

        # Assert
        assert content is not None
        assert "consumer lag" in content.lower()

    def test_loads_error_burst(self):
        # Act
        content = load_runbook("error_burst")

        # Assert
        assert content is not None
        assert "Late events" in content

    def test_returns_none_for_unknown_type(self):
        # Act
        content = load_runbook("nonexistent_anomaly_type")

        # Assert
        assert content is None

    def test_strips_frontmatter(self):
        # Act
        content = load_runbook("latency_spike")

        # Assert
        assert "---" not in content
        assert "anomaly_type:" not in content


class TestListRunbooks:

    def test_lists_all_runbooks(self):
        # Act
        types = list_runbooks()

        # Assert
        assert "latency_spike" in types
        assert "checkpoint_failure" in types
        assert "throughput_drop" in types
        assert "backpressure" in types
        assert "error_burst" in types
        assert len(types) == 5


class TestRunbookInjection:

    def test_latency_context_matches_runbook(self):
        # Arrange
        from streamops_mcp.agent.monitor import MonitorAgent

        # Act
        result = MonitorAgent._resolve_runbooks("Detected latency spike on operator-5")

        # Assert
        assert "Runbook: latency_spike" in result
        assert "GC pressure" in result

    def test_backpressure_context_matches_runbook(self):
        # Arrange
        from streamops_mcp.agent.monitor import MonitorAgent

        # Act
        result = MonitorAgent._resolve_runbooks("Backpressure ratio exceeded 0.8")

        # Assert
        assert "Runbook: backpressure" in result

    def test_multiple_runbooks_matched(self):
        # Arrange
        from streamops_mcp.agent.monitor import MonitorAgent

        # Act
        result = MonitorAgent._resolve_runbooks(
            "Latency spike detected with high backpressure on sink operator"
        )

        # Assert
        assert "Runbook: latency_spike" in result
        assert "Runbook: backpressure" in result

    def test_no_match_returns_empty(self):
        # Arrange
        from streamops_mcp.agent.monitor import MonitorAgent

        # Act
        result = MonitorAgent._resolve_runbooks("All systems nominal, no issues detected")

        # Assert
        assert result == ""

    def test_consumer_lag_matches_throughput_drop(self):
        # Arrange
        from streamops_mcp.agent.monitor import MonitorAgent

        # Act
        result = MonitorAgent._resolve_runbooks("Consumer lag increasing on partition 3")

        # Assert
        assert "Runbook: throughput_drop" in result
