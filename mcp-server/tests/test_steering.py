"""Steering example tests for agent behavioral validation.

These tests validate that the agent infrastructure (prompts, runbooks,
schemas, tool definitions) is consistent with the expected behavior
defined in steering-examples/*.json scenarios. They do NOT call the
Claude API; they verify structural alignment.
"""

import json
from pathlib import Path

import pytest

from streamops_mcp.agent.monitor import MonitorAgent
from streamops_mcp.agent.schemas import DiagnosisReport
from streamops_mcp.agent.tools import ALL_TOOLS
from streamops_mcp.prompts import list_runbooks, load_runbook

EXAMPLES_DIR = Path(__file__).parent / "steering-examples"


def _load_scenarios() -> list[dict]:
    scenarios = []
    for path in sorted(EXAMPLES_DIR.glob("*.json")):
        scenarios.append(json.loads(path.read_text(encoding="utf-8")))
    return scenarios


SCENARIOS = _load_scenarios()
TOOL_NAMES = {t["name"] for t in ALL_TOOLS}


class TestSteeringExampleStructure:

    @pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["scenario"] for s in SCENARIOS])
    def test_scenario_has_required_fields(self, scenario):
        # Assert
        assert "scenario" in scenario
        assert "description" in scenario
        assert "input" in scenario
        assert "expected" in scenario
        assert "anomaly_context" in scenario["input"]

    @pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["scenario"] for s in SCENARIOS])
    def test_expected_tools_exist(self, scenario):
        # Arrange
        expected = scenario["expected"]
        required = set(expected.get("tool_calls_required", []))
        recommended = set(expected.get("tool_calls_recommended", []))

        # Assert
        missing_required = required - TOOL_NAMES
        missing_recommended = recommended - TOOL_NAMES
        assert not missing_required, f"Required tools not defined: {missing_required}"
        assert not missing_recommended, f"Recommended tools not defined: {missing_recommended}"

    @pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["scenario"] for s in SCENARIOS])
    def test_severity_range_valid(self, scenario):
        # Arrange
        valid_severities = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
        severity_range = set(scenario["expected"].get("severity_range", []))

        # Assert
        invalid = severity_range - valid_severities
        assert not invalid, f"Invalid severities: {invalid}"

    @pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["scenario"] for s in SCENARIOS])
    def test_anomaly_type_in_schema(self, scenario):
        # Arrange
        known_types = {
            "latency_spike", "throughput_drop", "backpressure",
            "checkpoint_failure", "memory_pressure", "error_burst",
        }

        # Assert
        assert scenario["expected"]["anomaly_type"] in known_types


class TestSteeringRunbookAlignment:

    @pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["scenario"] for s in SCENARIOS])
    def test_runbook_matches_when_expected(self, scenario):
        # Arrange
        should_match = scenario["expected"].get("runbook_should_match", False)
        anomaly_type = scenario["expected"]["anomaly_type"]

        # Act
        runbook = load_runbook(anomaly_type)

        # Assert
        if should_match:
            assert runbook is not None, f"Expected runbook for {anomaly_type} but none found"
        else:
            pass

    @pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["scenario"] for s in SCENARIOS])
    def test_runbook_injection_matches_context(self, scenario):
        # Arrange
        context = scenario["input"]["anomaly_context"]

        # Act
        result = MonitorAgent._resolve_runbooks(context)

        # Assert
        if scenario["expected"].get("runbook_should_match", False):
            assert len(result) > 0, f"Expected runbook match for context but got empty"


class TestSteeringDiagnosisAlignment:

    @pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["scenario"] for s in SCENARIOS])
    def test_diagnosis_keywords_in_context(self, scenario):
        # Arrange
        context = scenario["input"]["anomaly_context"].lower()
        keywords = scenario["expected"].get("diagnosis_keywords", [])

        # Act + Assert
        matched = [kw for kw in keywords if kw.lower() in context]
        assert len(matched) >= 1, (
            f"Expected at least 1 keyword from {keywords} in anomaly context"
        )

    @pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["scenario"] for s in SCENARIOS])
    def test_simulated_responses_are_valid_json(self, scenario):
        # Arrange
        responses = scenario["input"].get("simulated_tool_responses", {})

        # Act + Assert
        for tool_name, raw in responses.items():
            try:
                json.loads(raw)
            except json.JSONDecodeError:
                pytest.fail(f"Invalid JSON in simulated response for {tool_name}")


class TestSteeringCompleteness:

    def test_all_runbook_types_have_scenarios(self):
        # Arrange
        runbook_types = set(list_runbooks())
        scenario_types = {s["expected"]["anomaly_type"] for s in SCENARIOS}

        # Assert
        missing = runbook_types - scenario_types
        assert not missing, f"Runbook types without steering scenarios: {missing}"

    def test_minimum_scenario_count(self):
        # Assert
        assert len(SCENARIOS) >= 5, f"Expected at least 5 scenarios, got {len(SCENARIOS)}"
