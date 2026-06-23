"""Tests for the tool executor structured error responses."""

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from streamops_mcp.agent.executor import (
    _build_error_response,
    _classify_error,
    execute_tool,
)


class TestClassifyError:

    def test_timeout_is_transient(self):
        # Arrange
        exc = TimeoutError("connection timed out")

        # Act
        category, retryable = _classify_error(exc)

        # Assert
        assert category == "transient"
        assert retryable is True

    def test_connection_error_is_transient(self):
        # Arrange
        exc = ConnectionError("refused")

        # Act
        category, retryable = _classify_error(exc)

        # Assert
        assert category == "transient"
        assert retryable is True

    def test_httpx_timeout_is_transient(self):
        # Arrange
        exc = httpx.ReadTimeout("read timed out")

        # Act
        category, retryable = _classify_error(exc)

        # Assert
        assert category == "transient"
        assert retryable is True

    def test_httpx_connect_error_is_transient(self):
        # Arrange
        exc = httpx.ConnectError("connection refused")

        # Act
        category, retryable = _classify_error(exc)

        # Assert
        assert category == "transient"
        assert retryable is True

    def test_value_error_is_validation(self):
        # Arrange
        exc = ValueError("bad parameter")

        # Act
        category, retryable = _classify_error(exc)

        # Assert
        assert category == "validation"
        assert retryable is False

    def test_type_error_is_validation(self):
        # Arrange
        exc = TypeError("expected str")

        # Act
        category, retryable = _classify_error(exc)

        # Assert
        assert category == "validation"
        assert retryable is False

    def test_key_error_is_validation(self):
        # Arrange
        exc = KeyError("missing_field")

        # Act
        category, retryable = _classify_error(exc)

        # Assert
        assert category == "validation"
        assert retryable is False

    def test_permission_error_is_permission(self):
        # Arrange
        exc = PermissionError("access denied")

        # Act
        category, retryable = _classify_error(exc)

        # Assert
        assert category == "permission"
        assert retryable is False

    def test_unknown_error_is_internal(self):
        # Arrange
        exc = RuntimeError("something unexpected")

        # Act
        category, retryable = _classify_error(exc)

        # Assert
        assert category == "internal"
        assert retryable is False


class TestBuildErrorResponse:

    def test_includes_all_fields(self):
        # Arrange
        exc = TimeoutError("timed out")

        # Act
        result = json.loads(_build_error_response("query_flink_jobs", exc, "transient", True))

        # Assert
        assert result["error"] is True
        assert result["errorCategory"] == "transient"
        assert result["isRetryable"] is True
        assert "TimeoutError" in result["message"]
        assert result["tool"] == "query_flink_jobs"

    def test_non_retryable_response(self):
        # Arrange
        exc = ValueError("bad input")

        # Act
        result = json.loads(_build_error_response("get_consumer_lag", exc, "validation", False))

        # Assert
        assert result["error"] is True
        assert result["isRetryable"] is False
        assert result["errorCategory"] == "validation"


class TestExecuteTool:

    @pytest.mark.asyncio
    async def test_unknown_tool_returns_validation_error(self):
        # Arrange
        mock_tools = {}

        # Act
        with patch("streamops_mcp.agent.executor.mcp") as mock_mcp:
            mock_mcp._tool_manager._tools = mock_tools
            result = json.loads(await execute_tool("nonexistent_tool", {}))

        # Assert
        assert result["error"] is True
        assert result["errorCategory"] == "validation"
        assert result["isRetryable"] is False

    @pytest.mark.asyncio
    async def test_successful_tool_returns_result(self):
        # Arrange
        mock_fn = AsyncMock(return_value={"status": "ok"})
        mock_tool = type("Tool", (), {"fn": mock_fn})()

        # Act
        with patch("streamops_mcp.agent.executor.mcp") as mock_mcp:
            mock_mcp._tool_manager._tools = {"test_tool": mock_tool}
            result = json.loads(await execute_tool("test_tool", {"arg": "val"}))

        # Assert
        assert result == {"status": "ok"}
        mock_fn.assert_called_once_with(arg="val")

    @pytest.mark.asyncio
    async def test_timeout_returns_transient_error(self):
        # Arrange
        mock_fn = AsyncMock(side_effect=TimeoutError("connection timed out"))
        mock_tool = type("Tool", (), {"fn": mock_fn})()

        # Act
        with patch("streamops_mcp.agent.executor.mcp") as mock_mcp:
            mock_mcp._tool_manager._tools = {"query_flink_jobs": mock_tool}
            result = json.loads(await execute_tool("query_flink_jobs", {}))

        # Assert
        assert result["error"] is True
        assert result["errorCategory"] == "transient"
        assert result["isRetryable"] is True

    @pytest.mark.asyncio
    async def test_value_error_returns_validation_error(self):
        # Arrange
        mock_fn = AsyncMock(side_effect=ValueError("invalid group_id"))
        mock_tool = type("Tool", (), {"fn": mock_fn})()

        # Act
        with patch("streamops_mcp.agent.executor.mcp") as mock_mcp:
            mock_mcp._tool_manager._tools = {"get_consumer_lag": mock_tool}
            result = json.loads(await execute_tool("get_consumer_lag", {"group_id": ""}))

        # Assert
        assert result["error"] is True
        assert result["errorCategory"] == "validation"
        assert result["isRetryable"] is False
