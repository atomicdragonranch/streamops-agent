"""Structured output schema for the Diagnostic Agent.

Pydantic model constraining Claude's output when investigating an anomaly.
The agent must produce a structured diagnosis with claim-source attribution,
not free-form text.
"""

from enum import StrEnum

from pydantic import BaseModel, Field, model_validator


class Confidence(StrEnum):
    """Confidence level for a diagnostic claim based on corroborating evidence."""

    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    UNSOURCED = "UNSOURCED"


class SourceRecord(BaseModel):
    """A data source referenced by one or more claims.

    Every tool call or data point that contributes to the diagnosis gets a
    unique source_id. Claims reference these IDs so any finding can be traced
    back to the tool and data that produced it.

    """

    source_id: str = Field(description="Unique identifier for this source (e.g., 'src-001')")
    tool_name: str = Field(
        description="MCP tool that produced this data (e.g., 'query_flink_jobs')"
    )
    retrieved_at: str = Field(description="ISO-8601 timestamp when the data was retrieved")
    raw_output: str = Field(description="Verbatim or summarized tool output")


class ClaimRecord(BaseModel):
    """A single factual claim paired with its source.

    Every diagnostic finding is recorded as a claim explicitly linked to
    the source_id that produced it. Downstream agents (and humans) can
    verify any claim back to its origin without re-running tools.

    """

    claim_id: str = Field(description="Unique identifier for this claim (e.g., 'C01')")
    text: str = Field(
        description="The factual claim (e.g., 'Consumer lag is 45,000 on partition 2')"
    )
    source_id: str = Field(description="References SourceRecord.source_id that produced this claim")
    confidence: Confidence = Field(
        description=(
            "Confidence level based on corroborating evidence. "
            "HIGH: 2+ independent sources corroborate. "
            "MEDIUM: 1 source supports, no contradictions. "
            "LOW: inferred from indirect evidence or single weak signal. "
            "UNSOURCED: no data source backs the claim."
        ),
    )


class ConflictRecord(BaseModel):
    """Records contradictory data from different sources.

    When two sources report conflicting information, the sub-agent must
    annotate both claims, mark the conflict as unresolved, and escalate
    to the coordinator. Sub-agents must never silently resolve conflicts.

    """

    conflict_id: str = Field(description="Unique identifier (e.g., 'conf-001')")
    topic: str = Field(description="What the conflicting sources disagree about")
    claim_a_id: str = Field(description="First claim ID involved in the conflict")
    claim_b_id: str = Field(description="Second claim ID involved in the conflict")
    resolution: str = Field(
        default="unresolved",
        description="Resolution state: unresolved (escalate to coordinator), resolved_a, resolved_b",
    )
    notes: str = Field(
        default="",
        description="Additional context about the conflict for the coordinator",
    )


class AffectedComponent(BaseModel):
    """A streaming component involved in the anomaly."""

    name: str = Field(description="Component identifier (e.g., 'kafka-consumer', 'flink-operator')")
    role: str = Field(description="What this component does in the pipeline")
    status: str = Field(description="Current state: healthy, degraded, failing, unknown")
    evidence: str = Field(
        description="Specific metric or log entry supporting the status assessment"
    )


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
    It becomes the input context for the Report Agent. All claims are paired
    with sources for full attribution traceability, and any conflicting data
    is annotated for coordinator review.

    """

    anomaly_type: str = Field(
        description="Category: latency_spike, throughput_drop, backpressure, checkpoint_failure, memory_pressure, error_burst"
    )
    detected_at: str = Field(description="ISO-8601 timestamp when the anomaly was first detected")
    sources: list[SourceRecord] = Field(
        default_factory=list,
        description="All data sources consulted during investigation",
    )
    claims: list[ClaimRecord] = Field(
        default_factory=list,
        description="Factual claims, each linked to a source_id for attribution",
    )
    conflicts: list[ConflictRecord] = Field(
        default_factory=list,
        description="Contradictory findings that require coordinator review",
    )
    affected_components: list[AffectedComponent] = Field(
        description="Components involved in or affected by the anomaly"
    )
    root_cause: RootCause = Field(description="Identified or suspected root cause")
    tools_used: list[str] = Field(description="MCP tools called during investigation (audit trail)")
    raw_evidence: list[str] = Field(
        default_factory=list,
        description="Key data points collected during investigation",
    )

    @model_validator(mode="after")
    def _check_attribution_integrity(self):
        """Enforce claim-source attribution integrity so nothing is untraceable.

        Every sourced claim must reference a real source, and every conflict must
        reference real claims. This prevents the "attribution lost" failure the
        claim-source pattern exists to stop: a dangling source_id would leave a
        finding with no traceable origin. UNSOURCED claims are exempt, since they
        explicitly have no backing data source.
        """
        source_ids = [s.source_id for s in self.sources]
        dup_sources = sorted({sid for sid in source_ids if source_ids.count(sid) > 1})
        if dup_sources:
            raise ValueError(
                f"Duplicate source_id(s), references would be ambiguous: {dup_sources}"
            )
        source_id_set = set(source_ids)

        claim_ids = [c.claim_id for c in self.claims]
        dup_claims = sorted({cid for cid in claim_ids if claim_ids.count(cid) > 1})
        if dup_claims:
            raise ValueError(
                f"Duplicate claim_id(s), conflict references would be ambiguous: {dup_claims}"
            )
        claim_id_set = set(claim_ids)

        for claim in self.claims:
            if claim.confidence == Confidence.UNSOURCED:
                continue
            if claim.source_id not in source_id_set:
                raise ValueError(
                    f"Claim '{claim.claim_id}' references unknown source_id "
                    f"'{claim.source_id}'; attribution would be untraceable"
                )

        for conflict in self.conflicts:
            for cid in (conflict.claim_a_id, conflict.claim_b_id):
                if cid not in claim_id_set:
                    raise ValueError(
                        f"Conflict '{conflict.conflict_id}' references unknown claim_id '{cid}'"
                    )

        return self
