"""Audit trail for incident reports.

Persists every IncidentReport and its DiagnosisReport to a JSON Lines file
for post-incident review, trend analysis, and agent accuracy tracking.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from streamops_mcp.agent.schemas import DiagnosisReport, IncidentReport
from streamops_mcp.config import config

logger = logging.getLogger("streamops-mcp.audit")


class AuditLogger:
    """Appends structured audit entries to a JSON Lines file."""

    def __init__(self, path: str | Path | None = None):
        self._path = Path(path or config.audit_log_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return self._path

    def log_incident(
        self,
        report: IncidentReport,
        diagnosis: DiagnosisReport | None = None,
        human_approved: bool | None = None,
    ) -> dict:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "incident_id": report.incident_id,
            "title": report.title,
            "severity": report.severity.value,
            "anomaly_type": report.anomaly_type,
            "root_cause": report.root_cause,
            "summary": report.summary,
            "affected_components": report.affected_components,
            "recommended_actions": [a.model_dump() for a in report.recommended_actions],
            "low_confidence_claims": report.low_confidence_claims,
            "human_approved": human_approved,
        }

        if diagnosis is not None:
            entry["diagnosis"] = {
                "sources_consulted": [s.tool_name for s in diagnosis.sources],
                "claim_count": len(diagnosis.claims),
                "conflict_count": len(diagnosis.conflicts),
                "tools_used": diagnosis.tools_used,
            }

        self._append(entry)
        logger.info("Audit entry written for incident %s", report.incident_id)
        return entry

    def query(
        self,
        severity: str | None = None,
        anomaly_type: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        if not self._path.exists():
            return []

        results = []
        for line in self._path.read_text(encoding="utf-8").strip().splitlines():
            if not line:
                continue
            entry = json.loads(line)
            if severity and entry.get("severity") != severity:
                continue
            if anomaly_type and entry.get("anomaly_type") != anomaly_type:
                continue
            results.append(entry)
            if len(results) >= limit:
                break
        return results

    def _append(self, entry: dict) -> None:
        with self._path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
