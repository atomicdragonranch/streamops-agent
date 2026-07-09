"""Handoff payload schemas for sub-agent boundaries.

Each handoff between agents is validated through a typed Pydantic model.
This catches malformed or oversized payloads before they reach the LLM,
preventing silent failures or token overflow.

Context between agents is passed as structured data, never prose summaries a
downstream agent would have to re-parse. The Monitor hands the Diagnostic agent
a typed DetectedAnomaly (what breached, by how much, where); the Diagnostic
hands the Report agent the full serialized DiagnosisReport.
"""

from pydantic import BaseModel, Field, model_validator

from streamops_mcp.config import config


class DetectedAnomaly(BaseModel):
    """Typed description of an anomaly the Monitor detected.

    Replaces the old free-form ``anomaly_context`` string so the Diagnostic
    sub-agent receives unambiguous, typed context (metric, observed vs baseline,
    breach direction, component) instead of prose it has to re-parse. ``summary``
    keeps a one-line human-readable form for logging and runbook matching; the
    optional fields are best-effort, since not every signal exposes all of them.
    """

    anomaly_type: str = Field(
        description=(
            "Category: latency_spike, throughput_drop, backpressure, "
            "checkpoint_failure, memory_pressure, error_burst, or unknown"
        )
    )
    summary: str = Field(
        description="One-line human-readable description citing the metric and value"
    )
    detected_at: str = Field(description="ISO-8601 timestamp when the anomaly was detected")
    metric: str | None = Field(
        default=None,
        description="Name of the breached metric (e.g. 'consumer_lag', 'processing_latency_ms')",
    )
    observed_value: str | None = Field(
        default=None,
        description="Observed value, as a string to preserve units (e.g. '2,340ms', '85000')",
    )
    baseline: str | None = Field(
        default=None, description="Normal/expected value for context (e.g. '200ms')"
    )
    threshold: str | None = Field(
        default=None, description="The threshold that was breached, if applicable"
    )
    breach_direction: str | None = Field(
        default=None, description="Direction of the breach: 'above' or 'below'"
    )
    affected_component: str | None = Field(
        default=None,
        description="Component the anomaly centers on (e.g. 'sink-kafka', 'streamops-processor')",
    )
    source_signal_ids: list[str] = Field(
        default_factory=list,
        description="Identifiers of the tool signals/metrics that triggered detection (audit trail)",
    )


class MonitorToDiagnosticHandoff(BaseModel):
    """Payload passed from the Monitor agent to the Diagnostic sub-agent."""

    anomaly: DetectedAnomaly = Field(
        description="Structured description of the detected anomaly",
    )
    schema_hint: dict = Field(
        description="JSON schema the diagnostic agent must conform to",
    )

    @model_validator(mode="after")
    def validate_context_size(self):
        # summary is the only unbounded field; truncate it (not raise) so an
        # over-long detection narrative can never overflow the diagnostic prompt.
        max_chars = config.agent_handoff_max_context_chars
        if len(self.anomaly.summary) > max_chars:
            self.anomaly.summary = self.anomaly.summary[:max_chars]
        return self


class DiagnosticToReportHandoff(BaseModel):
    """Payload passed from the Diagnostic agent to the Report sub-agent."""

    diagnosis_json: str = Field(
        description="Serialized DiagnosisReport JSON",
    )
    schema_hint: dict = Field(
        description="JSON schema the report agent must conform to",
    )

    @model_validator(mode="after")
    def validate_context_size(self):
        max_chars = config.agent_handoff_max_context_chars
        if len(self.diagnosis_json) > max_chars:
            raise ValueError(
                f"DiagnosisReport JSON exceeds max handoff size "
                f"({len(self.diagnosis_json)} > {max_chars} chars)"
            )
        return self
