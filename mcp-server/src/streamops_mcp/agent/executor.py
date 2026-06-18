"""Tool executor: bridges Claude API tool_use calls to MCP tool implementations.

When Claude returns a tool_use block, the executor looks up the tool name,
calls the corresponding MCP tool function, and returns the result as a
tool_result message for the next API turn.

Cert ref: Domain 1 (tool execution in the agentic loop; tool_use -> execute
-> tool_result -> next API turn).
"""

import json
import logging

from streamops_mcp.server import mcp

logger = logging.getLogger("streamops-mcp.executor")


async def execute_tool(tool_name: str, tool_input: dict) -> str:
    """Execute a tool call and return the result as a JSON string.

    This is the dispatch layer between Claude's tool_use blocks and the
    actual MCP tool implementations. Returns JSON so Claude can parse
    structured results in its next turn.
    """
    logger.info("Executing tool: %s", tool_name)
    logger.debug("Tool input: %s", json.dumps(tool_input, default=str))

    tools = mcp._tool_manager._tools
    if tool_name not in tools:
        logger.error("Unknown tool: %s", tool_name)
        return json.dumps({"error": f"Unknown tool: {tool_name}"})

    try:
        tool_fn = tools[tool_name].fn
        result = await tool_fn(**tool_input)
        result_json = json.dumps(result, default=str)
        logger.debug("Tool result: %s", result_json[:500])
        return result_json
    except Exception as e:
        logger.error("Tool execution failed: %s - %s", tool_name, e)
        return json.dumps({"error": str(e), "tool": tool_name})
