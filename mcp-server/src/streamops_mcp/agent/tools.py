"""Tool definitions for the Claude API.

These mirror the MCP tools but are formatted as Claude API tool schemas.
The agent calls these via tool_use; the executor dispatches to the MCP
server's actual tool implementations.

Keeping tool definitions separate from execution lets us:
1. Test the agent's tool selection logic without a running MCP server
2. Scope which tools each sub-agent can access (Diagnostic vs Report)
3. Generate the definitions from the MCP tool registry if needed later
"""

FLINK_TOOLS = [
    {
        "name": "query_flink_jobs",
        "description": "List all Flink jobs with their current status (RUNNING/FAILED/CANCELED). Use as the first step when investigating pipeline issues.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_checkpoint_stats",
        "description": "Get checkpoint statistics for a Flink job: duration, size, failure count. Slow or failing checkpoints indicate state backend pressure.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string", "description": "Flink job ID"},
            },
            "required": ["job_id"],
        },
    },
    {
        "name": "get_flink_exceptions",
        "description": "Get recent exceptions for a Flink job including root cause and stack trace. Critical for diagnosing job failures or restarts.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string", "description": "Flink job ID"},
            },
            "required": ["job_id"],
        },
    },
]

KAFKA_TOOLS = [
    {
        "name": "get_consumer_lag",
        "description": "Get consumer group lag across all partitions. Growing lag means the consumer is falling behind the producer.",
        "input_schema": {
            "type": "object",
            "properties": {
                "group_id": {"type": "string", "description": "Consumer group ID (defaults to streamops-processor)"},
            },
            "required": [],
        },
    },
    {
        "name": "get_topic_throughput",
        "description": "Estimate messages/second for a Kafka topic by sampling watermark offsets.",
        "input_schema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "Topic name (defaults to stream-events)"},
                "seconds": {"type": "integer", "description": "Time window for rate estimate (default 60)"},
            },
            "required": [],
        },
    },
]

PROMETHEUS_TOOLS = [
    {
        "name": "query_metrics",
        "description": "Execute a PromQL query against Prometheus. Use for custom metric queries beyond the built-in tools.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "PromQL expression"},
                "time_range": {"type": "string", "description": "Range for range queries (e.g., '5m', '1h')"},
            },
            "required": ["query"],
        },
    },
]

EVENT_TOOLS = [
    {
        "name": "get_recent_events",
        "description": "Retrieve the N most recent events from a Kafka topic. Use to inspect actual payloads, not just aggregated stats.",
        "input_schema": {
            "type": "object",
            "properties": {
                "count": {"type": "integer", "description": "Number of events (max 100, default 20)"},
                "topic": {"type": "string", "description": "Topic name"},
                "event_type": {"type": "string", "description": "Filter: metric, log, alert, heartbeat"},
            },
            "required": [],
        },
    },
    {
        "name": "search_logs",
        "description": "Search log events by message content. Use to find specific errors or trace event sequences around an incident.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Substring to search (case-insensitive)"},
                "count": {"type": "integer", "description": "Max results (max 100, default 20)"},
                "severity": {"type": "string", "description": "Filter: DEBUG, INFO, WARN, ERROR, FATAL"},
                "component": {"type": "string", "description": "Filter by component name"},
            },
            "required": ["pattern"],
        },
    },
]

ALL_TOOLS = FLINK_TOOLS + KAFKA_TOOLS + PROMETHEUS_TOOLS + EVENT_TOOLS

# Diagnostic agent gets all query tools (it needs to investigate)
DIAGNOSTIC_TOOLS = ALL_TOOLS

# Report agent gets no tools (it synthesizes from the diagnosis, doesn't query)
REPORT_TOOLS = []
