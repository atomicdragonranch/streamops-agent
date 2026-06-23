"""Handoff payload schemas for sub-agent boundaries.

Each handoff between agents is validated through a typed Pydantic model.
This catches malformed or oversized payloads before they reach the LLM,
preventing silent failures or token overflow.
"""

from pydantic import BaseModel, Field, model_validator

from streamops_mcp.config import config


class MonitorToDiagnosticHandoff(BaseModel):
    """Payload passed from the Monitor agent to the Diagnostic sub-agent."""

    anomaly_context: str = Field(
        description="Raw anomaly description from the monitoring loop",
    )
    schema_hint: dict = Field(
        description="JSON schema the diagnostic agent must conform to",
    )

    @model_validator(mode="after")
    def validate_context_size(self):
        max_chars = config.agent_handoff_max_context_chars
        if len(self.anomaly_context) > max_chars:
            self.anomaly_context = self.anomaly_context[:max_chars]
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
