"""Tool executor: bridges Claude API tool_use calls to MCP tool implementations.

When Claude returns a tool_use block, the executor looks up the tool name,
calls the corresponding MCP tool function, and returns the result as a
tool_result message for the next API turn.

Errors are returned as structured JSON with category and retryability so
the LLM can reason about whether to retry or try a different approach.
"""

import json
import logging

import httpx

from streamops_mcp.server import mcp

logger = logging.getLogger("streamops-mcp.executor")

_TRANSIENT_EXCEPTION_TYPES = (
    TimeoutError,
    ConnectionError,
    OSError,
    httpx.TimeoutException,
    httpx.ConnectError,
    httpx.RemoteProtocolError,
)


def _classify_error(exc: Exception) -> tuple[str, bool]:
    """Return (error_category, is_retryable) for a tool execution failure."""
    if isinstance(exc, PermissionError):
        return "permission", False
    if isinstance(exc, _TRANSIENT_EXCEPTION_TYPES):
        return "transient", True
    if isinstance(exc, (ValueError, TypeError, KeyError)):
        return "validation", False
    return "internal", False


def _build_error_response(
    tool_name: str, exc: Exception, category: str, retryable: bool,
) -> str:
    return json.dumps({
        "error": True,
        "errorCategory": category,
        "isRetryable": retryable,
        "message": f"{type(exc).__name__}: {exc}",
        "tool": tool_name,
    })


async def execute_tool(tool_name: str, tool_input: dict) -> str:
    """Execute a tool call and return the result as a JSON string.

    On success, returns the tool's result as JSON.
    On failure, returns a structured error with category and retryability.
    """
    logger.info("Executing tool: %s", tool_name)
    logger.debug("Tool input: %s", json.dumps(tool_input, default=str))

    tools = mcp._tool_manager._tools
    if tool_name not in tools:
        logger.error("Unknown tool: %s", tool_name)
        return json.dumps({
            "error": True,
            "errorCategory": "validation",
            "isRetryable": False,
            "message": f"Unknown tool: {tool_name}",
            "tool": tool_name,
        })

    try:
        tool_fn = tools[tool_name].fn
        result = await tool_fn(**tool_input)
        result_json = json.dumps(result, default=str)
        logger.debug("Tool result: %s", result_json[:500])
        return result_json
    except Exception as e:
        category, retryable = _classify_error(e)
        logger.error(
            "Tool execution failed: %s - %s (category=%s, retryable=%s)",
            tool_name, e, category, retryable,
        )
        return _build_error_response(tool_name, e, category, retryable)
