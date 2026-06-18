"""Tests for Flink MCP tools.

Uses respx to mock httpx requests to the Flink REST API.
"""

import pytest
import respx
from httpx import Response

from streamops_mcp.tools.flink import register_flink_tools
from mcp.server.fastmcp import FastMCP


@pytest.fixture
def mcp_server():
    server = FastMCP("test")
    register_flink_tools(server)
    return server


@pytest.fixture
def flink_tools(mcp_server):
    return mcp_server._tool_manager._tools


class TestQueryFlinkJobs:

    @respx.mock
    @pytest.mark.asyncio
    async def test_returns_job_list(self, flink_tools):
        respx.get("http://localhost:8081/jobs/overview").mock(
            return_value=Response(200, json={
                "jobs": [
                    {
                        "jid": "abc123",
                        "name": "StreamOps Processor",
                        "state": "RUNNING",
                        "start-time": 1718000000000,
                        "duration": 3600000,
                        "tasks": {"total": 4, "running": 4},
                    }
                ]
            })
        )
        fn = flink_tools["query_flink_jobs"].fn
        result = await fn()
        assert len(result["jobs"]) == 1
        assert result["jobs"][0]["job_id"] == "abc123"
        assert result["jobs"][0]["state"] == "RUNNING"

    @respx.mock
    @pytest.mark.asyncio
    async def test_empty_cluster(self, flink_tools):
        respx.get("http://localhost:8081/jobs/overview").mock(
            return_value=Response(200, json={"jobs": []})
        )
        fn = flink_tools["query_flink_jobs"].fn
        result = await fn()
        assert result["jobs"] == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_connection_error(self, flink_tools):
        respx.get("http://localhost:8081/jobs/overview").mock(side_effect=Exception("Connection refused"))
        fn = flink_tools["query_flink_jobs"].fn
        result = await fn()
        assert "error" in result


class TestGetCheckpointStats:

    @respx.mock
    @pytest.mark.asyncio
    async def test_returns_checkpoint_data(self, flink_tools):
        respx.get("http://localhost:8081/jobs/abc123/checkpoints").mock(
            return_value=Response(200, json={
                "counts": {"completed": 42, "failed": 1, "in_progress": 0, "restored": 0, "total": 43},
                "latest": {
                    "completed": {
                        "id": 42,
                        "duration": 1500,
                        "state_size": 1048576,
                        "trigger_timestamp": 1718000000000,
                    },
                    "failed": None,
                },
                "summary": {
                    "state_size": {"min": 500000, "max": 2000000, "avg": 1000000},
                },
            })
        )
        fn = flink_tools["get_checkpoint_stats"].fn
        result = await fn(job_id="abc123")
        assert result["counts"]["completed"] == 42
        assert result["latest_completed"]["duration_ms"] == 1500
        assert result["size_summary"]["avg"] == 1000000

    @respx.mock
    @pytest.mark.asyncio
    async def test_job_not_found(self, flink_tools):
        respx.get("http://localhost:8081/jobs/missing/checkpoints").mock(
            return_value=Response(404)
        )
        fn = flink_tools["get_checkpoint_stats"].fn
        result = await fn(job_id="missing")
        assert "not found" in result["error"]


class TestGetFlinkExceptions:

    @respx.mock
    @pytest.mark.asyncio
    async def test_returns_exceptions(self, flink_tools):
        respx.get("http://localhost:8081/jobs/abc123/exceptions").mock(
            return_value=Response(200, json={
                "root-exception": "java.lang.OutOfMemoryError: Java heap space",
                "timestamp": 1718000000000,
                "all-exceptions": [
                    {
                        "exception": "java.lang.OutOfMemoryError",
                        "task": "Source: Kafka",
                        "location": "tm-1",
                        "timestamp": 1718000000000,
                    }
                ],
            })
        )
        fn = flink_tools["get_flink_exceptions"].fn
        result = await fn(job_id="abc123")
        assert "OutOfMemoryError" in result["root_exception"]
        assert len(result["exceptions"]) == 1
