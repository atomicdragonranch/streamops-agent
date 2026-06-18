from pydantic_settings import BaseSettings


class StreamOpsConfig(BaseSettings):
    """Configuration loaded from environment variables.

    Each service endpoint defaults to the Docker Compose local dev stack.
    Override via env vars for remote or production deployments.
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

    model_config = {"env_prefix": "STREAMOPS_"}


config = StreamOpsConfig()
