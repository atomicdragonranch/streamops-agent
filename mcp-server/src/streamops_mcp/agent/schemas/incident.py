"""Structured output schema for the Report Agent.

The Report Agent receives a DiagnosisReport and produces an IncidentReport
with severity classification and recommended actions. This is what gets
routed through the escalation logic.
"""

from enum import Enum

from pydantic import BaseModel, Field


class Severity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class RecommendedAction(BaseModel):
    """A specific action to remediate or mitigate the incident."""

    action: str = Field(description="What to do (e.g., 'Increase TaskManager heap to 4GB')")
    rationale: str = Field(description="Why this action addresses the root cause")
    risk: str = Field(description="Risk of taking this action: none, low, medium, high")
    requires_downtime: bool = Field(description="Whether this action requires pipeline restart")


class IncidentReport(BaseModel):
    """Complete incident report produced by the Report Agent.

    This is the final output of the agent pipeline. It includes everything
    a human operator needs: what happened, why, how bad, and what to do.
    Severity determines escalation routing.
    """

    incident_id: str = Field(description="Unique identifier for this incident")
    title: str = Field(description="Short, descriptive title (e.g., 'Checkpoint timeout on StreamOps Processor')")
    severity: Severity = Field(description="Severity classification driving escalation routing")
    summary: str = Field(description="2-3 sentence executive summary for on-call")
    anomaly_type: str = Field(description="Category from the diagnosis")
    root_cause: str = Field(description="One-sentence root cause from the diagnosis")
    affected_components: list[str] = Field(description="Component names affected")
    timeline: list[str] = Field(
        description="Chronological sequence of events leading to the incident"
    )
    recommended_actions: list[RecommendedAction] = Field(
        description="Ordered list of recommended remediation steps"
    )
    monitoring_notes: str = Field(
        description="What to watch after remediation to confirm resolution"
    )
