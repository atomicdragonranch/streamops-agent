"""Tests for the prompt loader module."""

import pytest

from streamops_mcp.prompts import load_prompt, load_prompt_metadata


class TestLoadPrompt:

    def test_loads_monitor_prompt(self):
        # Arrange
        name = "monitor"

        # Act
        result = load_prompt(name)

        # Assert
        assert isinstance(result, str)
        assert len(result) > 0
        assert "streaming infrastructure" in result

    def test_loads_diagnostic_prompt(self):
        # Arrange
        name = "diagnostic"

        # Act
        result = load_prompt(name)

        # Assert
        assert isinstance(result, str)
        assert len(result) > 0
        assert "DiagnosisReport" in result

    def test_loads_report_prompt(self):
        # Arrange
        name = "report"

        # Act
        result = load_prompt(name)

        # Assert
        assert isinstance(result, str)
        assert len(result) > 0
        assert "IncidentReport" in result

    def test_strips_yaml_frontmatter(self):
        # Arrange
        name = "monitor"

        # Act
        result = load_prompt(name)

        # Assert
        assert "---" not in result
        assert "name:" not in result
        assert "role:" not in result

    def test_nonexistent_prompt_raises_error(self):
        # Arrange
        name = "nonexistent_prompt_that_does_not_exist"

        # Act / Assert
        with pytest.raises(FileNotFoundError):
            load_prompt(name)

    def test_cached_on_second_load(self):
        # Arrange
        name = "monitor"

        # Act
        first = load_prompt(name)
        second = load_prompt(name)

        # Assert
        assert first is second


class TestLoadPromptMetadata:

    def test_monitor_metadata_has_required_keys(self):
        # Arrange
        name = "monitor"

        # Act
        metadata = load_prompt_metadata(name)

        # Assert
        assert metadata["name"] == "monitor"
        assert "description" in metadata
        assert "role" in metadata
        assert "tools" in metadata

    def test_diagnostic_metadata(self):
        # Arrange
        name = "diagnostic"

        # Act
        metadata = load_prompt_metadata(name)

        # Assert
        assert metadata["name"] == "diagnostic"
        assert metadata["role"] == "streaming-infrastructure-diagnostic"
        assert "diagnostic" in metadata["tools"]

    def test_report_metadata(self):
        # Arrange
        name = "report"

        # Act
        metadata = load_prompt_metadata(name)

        # Assert
        assert metadata["name"] == "report"
        assert metadata["role"] == "streaming-infrastructure-reporter"

    def test_nonexistent_metadata_raises_error(self):
        # Arrange
        name = "nonexistent_prompt_that_does_not_exist"

        # Act / Assert
        with pytest.raises(FileNotFoundError):
            load_prompt_metadata(name)
