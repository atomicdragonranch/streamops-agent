"""Prometheus query tool.

Wraps the Prometheus HTTP API so the agent can run PromQL queries without
knowing the endpoint URL or query syntax details. The agent describes what
it wants to measure ("show me p99 latency over the last 5 minutes") and
the coordinator translates that into PromQL before calling this tool.
"""

import logging
import time
from typing import Optional

import httpx
from mcp.server.fastmcp import FastMCP

from streamops_mcp.config import config

logger = logging.getLogger("streamops-mcp.prometheus")


def register_prometheus_tools(mcp: FastMCP):

    @mcp.tool()
    async def query_metrics(query: str, time_range: Optional[str] = None) -> dict:
        """Execute a PromQL query against Prometheus.

        Args:
            query: PromQL expression (e.g., 'flink_taskmanager_job_task_operator_numRecordsIn')
            time_range: Optional range for range queries (e.g., '5m', '1h'). If provided,
                        uses query_range endpoint; otherwise uses instant query.

        Returns metric name, labels, and values. For range queries, returns a
        time series of (timestamp, value) pairs.
        """
        logger.info("Prometheus query: %s (range=%s)", query, time_range)

        try:
            async with httpx.AsyncClient(base_url=config.prometheus_url, timeout=config.http_timeout) as client:
                if time_range:
                    end = time.time()
                    start = end - _parse_duration(time_range)
                    response = await client.get("/api/v1/query_range", params={
                        "query": query,
                        "start": start,
                        "end": end,
                        "step": _step_for_range(time_range),
                    })
                else:
                    response = await client.get("/api/v1/query", params={"query": query})

                response.raise_for_status()
                data = response.json()

            if data.get("status") != "success":
                error_msg = data.get("error", "Unknown Prometheus error")
                logger.error("Prometheus query failed: %s", error_msg)
                return {"error": error_msg}

            result_type = data.get("data", {}).get("resultType")
            results = data.get("data", {}).get("result", [])

            logger.info("Prometheus returned %d results (type=%s)", len(results), result_type)

            formatted = []
            for r in results[:config.prometheus_max_results]:
                entry = {"metric": r.get("metric", {})}
                if result_type == "matrix":
                    entry["values"] = [
                        {"timestamp": v[0], "value": v[1]} for v in r.get("values", [])
                    ]
                elif result_type == "vector":
                    val = r.get("value", [])
                    entry["timestamp"] = val[0] if len(val) > 0 else None
                    entry["value"] = val[1] if len(val) > 1 else None

                formatted.append(entry)

            return {
                "query": query,
                "result_type": result_type,
                "result_count": len(results),
                "results": formatted,
            }
        except httpx.ConnectError:
            logger.error("Cannot connect to Prometheus at %s", config.prometheus_url)
            return {"error": f"Cannot connect to Prometheus at {config.prometheus_url}"}
        except Exception as e:
            logger.error("Prometheus query failed: %s", e)
            return {"error": str(e)}


def _parse_duration(time_range: str) -> float:
    """Convert a duration string like '5m', '1h', '2d' to seconds."""
    if time_range.endswith("s"):
        return float(time_range[:-1])
    if time_range.endswith("m"):
        return float(time_range[:-1]) * 60
    if time_range.endswith("h"):
        return float(time_range[:-1]) * 3600
    if time_range.endswith("d"):
        return float(time_range[:-1]) * 86400
    return 900.0


def _step_for_range(time_range: str) -> str:
    """Pick a reasonable step size based on the query range."""
    if time_range.endswith("m"):
        minutes = int(time_range[:-1])
        if minutes <= 5:
            return "5s"
        if minutes <= 30:
            return "15s"
        return "60s"
    if time_range.endswith("h"):
        return "60s"
    if time_range.endswith("d"):
        return "300s"
    return "15s"
