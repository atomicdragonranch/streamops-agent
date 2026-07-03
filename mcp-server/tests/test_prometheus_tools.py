"""Tests for Prometheus MCP tools."""

import pytest
import respx
from httpx import Response
from mcp.server.fastmcp import FastMCP

from streamops_mcp.tools.prometheus import _step_for_range, register_prometheus_tools


@pytest.fixture
def mcp_server():
    server = FastMCP("test")
    register_prometheus_tools(server)
    return server


@pytest.fixture
def prom_tools(mcp_server):
    return mcp_server._tool_manager._tools


class TestQueryMetrics:

    @respx.mock
    @pytest.mark.asyncio
    async def test_instant_query(self, prom_tools):
        respx.get("http://localhost:9090/api/v1/query").mock(
            return_value=Response(200, json={
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {"__name__": "flink_taskmanager_numRecordsIn", "job": "flink"},
                            "value": [1718000000, "42.5"],
                        }
                    ],
                },
            })
        )
        fn = prom_tools["query_metrics"].fn
        result = await fn(query="flink_taskmanager_numRecordsIn")
        assert result["result_type"] == "vector"
        assert result["results"][0]["value"] == "42.5"

    @respx.mock
    @pytest.mark.asyncio
    async def test_range_query(self, prom_tools):
        respx.get("http://localhost:9090/api/v1/query_range").mock(
            return_value=Response(200, json={
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {"__name__": "latency_ms"},
                            "values": [[1718000000, "10"], [1718000015, "12"]],
                        }
                    ],
                },
            })
        )
        fn = prom_tools["query_metrics"].fn
        result = await fn(query="latency_ms", time_range="5m")
        assert result["result_type"] == "matrix"
        assert len(result["results"][0]["values"]) == 2

    @respx.mock
    @pytest.mark.asyncio
    async def test_prometheus_error_response(self, prom_tools):
        respx.get("http://localhost:9090/api/v1/query").mock(
            return_value=Response(200, json={
                "status": "error",
                "error": "invalid expression",
            })
        )
        fn = prom_tools["query_metrics"].fn
        result = await fn(query="bad{query")
        assert "error" in result

    @respx.mock
    @pytest.mark.asyncio
    async def test_connection_error(self, prom_tools):
        respx.get("http://localhost:9090/api/v1/query").mock(
            side_effect=Exception("Connection refused")
        )
        fn = prom_tools["query_metrics"].fn
        result = await fn(query="test")
        assert "error" in result


class TestStepForRange:

    def test_short_minutes(self):
        assert _step_for_range("5m") == "5s"

    def test_medium_minutes(self):
        assert _step_for_range("15m") == "15s"

    def test_long_minutes(self):
        assert _step_for_range("60m") == "60s"

    def test_hours(self):
        assert _step_for_range("2h") == "60s"

    def test_days(self):
        assert _step_for_range("1d") == "300s"

    def test_unknown(self):
        assert _step_for_range("foo") == "15s"
