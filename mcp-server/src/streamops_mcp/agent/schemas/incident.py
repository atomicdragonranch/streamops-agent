"""Structured output schema for the Report Agent.

The Report Agent receives a DiagnosisReport and produces an IncidentReport
with severity classification and recommended actions. This is what gets
routed through the escalation logic.

The report carries the diagnosis's sources and supporting claims forward so
the incident a human reads stays traceable to the tool signals behind it:
synthesis must not drop attribution. See issue #88.
"""

from enum import StrEnum

from pydantic import BaseModel, Field, model_validator

from streamops_mcp.agent.schemas.diagnosis import ClaimRecord, Confidence, SourceRecord


class Severity(StrEnum):
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
    title: str = Field(
        description="Short, descriptive title (e.g., 'Checkpoint timeout on StreamOps Processor')"
    )
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
    low_confidence_claims: list[str] = Field(
        default_factory=list,
        description="Claims with LOW or UNSOURCED confidence, surfaced for operator awareness",
    )
    requires_human_approval: bool = Field(
        default=True,
        description="Whether recommended actions require human approval before execution. True for all actions beyond passive monitoring.",
    )
    monitoring_notes: str = Field(
        description="What to watch after remediation to confirm resolution"
    )
    sources: list[SourceRecord] = Field(
        default_factory=list,
        description="Data sources carried from the diagnosis, so the incident is traceable",
    )
    supporting_claims: list[ClaimRecord] = Field(
        default_factory=list,
        description="Attributed claims from the diagnosis that back this incident",
    )

    @model_validator(mode="after")
    def _check_supporting_attribution(self):
        """Keep the incident self-attributing: every supporting claim must trace
        to a real source carried in this report. Mirrors the diagnosis-level
        integrity check so attribution survives synthesis rather than dangling.
        UNSOURCED claims are exempt, since they explicitly have no backing source.
        """
        source_ids = [s.source_id for s in self.sources]
        dup_sources = sorted({sid for sid in source_ids if source_ids.count(sid) > 1})
        if dup_sources:
            raise ValueError(
                f"Duplicate source_id(s) in incident report, references ambiguous: {dup_sources}"
            )
        source_id_set = set(source_ids)

        claim_ids = [c.claim_id for c in self.supporting_claims]
        dup_claims = sorted({cid for cid in claim_ids if claim_ids.count(cid) > 1})
        if dup_claims:
            raise ValueError(f"Duplicate claim_id(s) in incident report: {dup_claims}")

        for claim in self.supporting_claims:
            if claim.confidence == Confidence.UNSOURCED:
                continue
            if claim.source_id not in source_id_set:
                raise ValueError(
                    f"Supporting claim '{claim.claim_id}' references unknown source_id "
                    f"'{claim.source_id}'; incident attribution would be untraceable"
                )
        return self
