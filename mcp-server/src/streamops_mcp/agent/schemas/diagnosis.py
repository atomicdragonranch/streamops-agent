"""Structured output schema for the Diagnostic Agent.

This Pydantic model constrains Claude's output when investigating an anomaly.
The agent must produce a structured diagnosis, not free-form text. This is
the "structured output" pattern from the Claude API: pass the JSON schema
in the tool definition, and the model is forced to conform to it.
"""

from pydantic import BaseModel, Field


class AffectedComponent(BaseModel):
    """A streaming component involved in the anomaly."""

    name: str = Field(description="Component identifier (e.g., 'kafka-consumer', 'flink-operator')")
    role: str = Field(description="What this component does in the pipeline")
    status: str = Field(description="Current state: healthy, degraded, failing, unknown")
    evidence: str = Field(description="Specific metric or log entry supporting the status assessment")


class RootCause(BaseModel):
    """Identified or suspected root cause of the anomaly."""

    summary: str = Field(description="One-sentence root cause description")
    confidence: str = Field(description="Confidence level: high, medium, low")
    reasoning: str = Field(description="Chain of evidence leading to this conclusion")
    supporting_metrics: list[str] = Field(
        default_factory=list,
        description="Metric names and values that support this conclusion",
    )


class DiagnosisReport(BaseModel):
    """Complete diagnosis of a streaming pipeline anomaly.

    The Diagnostic Agent produces this after investigating with MCP tools.
    It becomes the input context for the Report Agent.
    """

    anomaly_type: str = Field(description="Category: latency_spike, throughput_drop, backpressure, checkpoint_failure, memory_pressure, error_burst")
    detected_at: str = Field(description="ISO-8601 timestamp when the anomaly was first detected")
    affected_components: list[AffectedComponent] = Field(
        description="Components involved in or affected by the anomaly"
    )
    root_cause: RootCause = Field(description="Identified or suspected root cause")
    tools_used: list[str] = Field(
        description="MCP tools called during investigation (audit trail)"
    )
    raw_evidence: list[str] = Field(
        default_factory=list,
        description="Key data points collected during investigation",
    )
