"""StreamOps MCP Server.

Exposes streaming infrastructure observability as MCP tools that an AI agent
can call to diagnose pipeline issues. Each tool queries a different backend
(Flink REST API, Kafka, Prometheus) and returns structured data.

Why MCP instead of direct API calls: the agent doesn't need to know endpoint
URLs, auth details, or query syntax. It calls `query_flink_jobs()` and gets
back structured job state.
"""

import logging

from mcp.server.fastmcp import FastMCP

from streamops_mcp.logging_setup import configure_logging
from streamops_mcp.tools.events import register_event_tools
from streamops_mcp.tools.flink import register_flink_tools
from streamops_mcp.tools.kafka import register_kafka_tools
from streamops_mcp.tools.prometheus import register_prometheus_tools

configure_logging()
logger = logging.getLogger("streamops-mcp")

mcp = FastMCP(
    "StreamOps MCP Server",
    instructions="Observability tools for streaming infrastructure: Flink jobs, Kafka topics, Prometheus metrics, event logs",
)

register_flink_tools(mcp)
register_kafka_tools(mcp)
register_prometheus_tools(mcp)
register_event_tools(mcp)

logger.info("StreamOps MCP server initialized with %d tools", len(mcp._tool_manager._tools))


def main():
    mcp.run()


if __name__ == "__main__":
    main()
