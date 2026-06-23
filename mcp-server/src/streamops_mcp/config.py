from pydantic_settings import BaseSettings


class StreamOpsConfig(BaseSettings):
    """Configuration loaded from environment variables.

    Each service endpoint defaults to the Docker Compose local dev stack.
    Override via env vars with STREAMOPS_ prefix for remote or production deployments.
    """

    # Flink REST API (JobManager)
    flink_url: str = "http://localhost:8081"

    # Prometheus query endpoint
    prometheus_url: str = "http://localhost:9090"

    # Kafka bootstrap servers
    kafka_bootstrap: str = "localhost:9092"

    # Kafka topics
    kafka_events_topic: str = "stream-events"
    kafka_alerts_topic: str = "stream-alerts"
    kafka_metrics_topic: str = "stream-metrics"

    # Consumer group for MCP tools (separate from the Flink processor's group)
    kafka_mcp_group: str = "streamops-mcp"

    # Default consumer group to query for lag (the Flink processor's group)
    kafka_processor_group: str = "streamops-processor"

    # HTTP client timeout for Flink and Prometheus queries (seconds)
    http_timeout: float = 10.0

    # Kafka client timeout for consumer operations (seconds)
    kafka_timeout: float = 5.0

    # Maximum results to return from Prometheus queries
    prometheus_max_results: int = 50

    # Maximum exceptions to return from Flink API
    flink_max_exceptions: int = 10

    # Event tool limits
    events_max_count: int = 100
    events_default_count: int = 20
    events_empty_poll_threshold: int = 3
    events_poll_timeout: float = 1.0
    events_log_scan_depth: int = 500

    # Agent configuration
    agent_prompt_dir: str = ""
    agent_model: str = "claude-sonnet-4-6"
    agent_max_tokens: int = 8192
    agent_max_tool_rounds: int = 15
    agent_monitor_interval: int = 60

    model_config = {"env_prefix": "STREAMOPS_"}


config = StreamOpsConfig()
