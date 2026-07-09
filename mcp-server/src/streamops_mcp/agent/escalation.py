"""Escalation logic: routes incidents by severity.

Severity routing:
  - LOW:      Log only, store for historical analysis
  - MEDIUM:   Log + CLI notification
  - HIGH:     Log + prominent CLI alert
  - CRITICAL: Log + human-in-the-loop (pause for confirmation)

CRITICAL incidents require a human to acknowledge before the system takes
any remediation action, preventing autonomous systems from making things
worse during a severe outage.

Independently of severity, a diagnosis carrying UNRESOLVED conflicts is never
allowed to pass silently. The ConflictRecord contract says sub-agents must
never resolve a conflict themselves; they mark it unresolved and hand it to
the coordinator. Acting autonomously on contradictory evidence is exactly the
failure mode to prevent, so unresolved conflicts are always surfaced and,
unless severity already forced a human decision, gated on human acknowledgment.
"""

import logging

from streamops_mcp.agent.audit import AuditLogger
from streamops_mcp.agent.schemas import (
    ConflictRecord,
    DiagnosisReport,
    IncidentReport,
    Severity,
)

logger = logging.getLogger("streamops-mcp.escalation")

_audit = AuditLogger()

UNRESOLVED = "unresolved"


def _unresolved_conflicts(diagnosis: DiagnosisReport | None) -> list[ConflictRecord]:
    """Return the conflicts the diagnostic sub-agent left for the coordinator.

    Per the ConflictRecord contract these are marked ``resolution="unresolved"``;
    they are the ones the escalation path must act on rather than drop.
    """
    if diagnosis is None:
        return []
    return [c for c in diagnosis.conflicts if c.resolution == UNRESOLVED]


async def escalate(
    report: IncidentReport,
    diagnosis: DiagnosisReport | None = None,
) -> None:
    """Route an incident report through the escalation chain."""
    logger.info("Escalating incident '%s' (severity=%s)", report.title, report.severity.value)

    unresolved = _unresolved_conflicts(diagnosis)
    if unresolved:
        # Always surface first, so the human sees the contradictions before any
        # severity prompt and the audit trail never shows a silent pass.
        _surface_unresolved_conflicts(report, unresolved)

    human_approved = None

    if report.severity == Severity.LOW:
        await _handle_low(report)
    elif report.severity == Severity.MEDIUM:
        await _handle_medium(report)
    elif report.severity == Severity.HIGH:
        await _handle_high(report)
    elif report.severity == Severity.CRITICAL:
        human_approved = await _handle_critical(report)

    # CRITICAL already forced a human decision (with the conflicts surfaced above);
    # for anything lower, unresolved conflicts force their own acknowledgment gate.
    conflicts_acknowledged = None
    if unresolved and report.severity != Severity.CRITICAL:
        conflicts_acknowledged = await _confirm_unresolved_conflicts(report, unresolved)

    _audit.log_incident(
        report,
        diagnosis=diagnosis,
        human_approved=human_approved,
        unresolved_conflict_count=len(unresolved),
        conflicts_acknowledged=conflicts_acknowledged,
    )


async def _handle_low(report: IncidentReport) -> None:
    logger.info("[LOW] %s: %s", report.title, report.summary)


async def _handle_medium(report: IncidentReport) -> None:
    logger.warning("[MEDIUM] %s: %s", report.title, report.summary)
    _print_report_summary(report)
    if report.requires_human_approval:
        print("NOTE: Recommended actions require human approval before execution.")


async def _handle_high(report: IncidentReport) -> None:
    logger.error("[HIGH] %s: %s", report.title, report.summary)
    _print_report_summary(report)
    _print_recommended_actions(report)
    if report.requires_human_approval:
        print("NOTE: Recommended actions require human approval before execution.")


async def _handle_critical(report: IncidentReport) -> bool | None:
    """CRITICAL: human-in-the-loop. Pause for confirmation.

    Returns True if approved, False if rejected, None if no input available.
    """
    logger.critical("[CRITICAL] %s: %s", report.title, report.summary)
    _print_report_summary(report)
    _print_recommended_actions(report)

    print("\n" + "=" * 60)
    print("CRITICAL INCIDENT: Human confirmation required")
    print("=" * 60)
    print(f"\nIncident: {report.title}")
    print(f"Root cause: {report.root_cause}")
    print("\nRecommended actions:")
    for i, action in enumerate(report.recommended_actions, 1):
        risk_marker = f" [RISK: {action.risk}]" if action.risk != "none" else ""
        downtime = " [REQUIRES DOWNTIME]" if action.requires_downtime else ""
        print(f"  {i}. {action.action}{risk_marker}{downtime}")

    print(f"\nMonitoring: {report.monitoring_notes}")
    print("\nApprove recommended actions? (y/n): ", end="", flush=True)

    try:
        response = input()
        if response.strip().lower() in ("y", "yes"):
            logger.info("Human approved actions for incident %s", report.incident_id)
            print("Actions approved. Proceeding with remediation recommendations.")
            return True
        else:
            logger.info("Human rejected actions for incident %s", report.incident_id)
            print("Actions rejected. Incident logged for manual review.")
            return False
    except EOFError:
        logger.warning("No human input available, logging for manual review")
        return None


def _print_report_summary(report: IncidentReport) -> None:
    print(f"\n--- Incident Report: {report.title} ---")
    print(f"Severity: {report.severity.value}")
    print(f"Summary: {report.summary}")
    print(f"Root cause: {report.root_cause}")
    print(f"Affected: {', '.join(report.affected_components)}")


def _print_recommended_actions(report: IncidentReport) -> None:
    if report.recommended_actions:
        print("Recommended actions:")
        for i, action in enumerate(report.recommended_actions, 1):
            print(f"  {i}. {action.action} (risk: {action.risk})")


def _surface_unresolved_conflicts(report: IncidentReport, conflicts: list[ConflictRecord]) -> None:
    """Log and print unresolved conflicts prominently so none passes silently."""
    logger.error(
        "Incident '%s' carries %d UNRESOLVED diagnostic conflict(s); forcing review",
        report.title,
        len(conflicts),
    )
    print("\n" + "!" * 60)
    print(f"UNRESOLVED DIAGNOSTIC CONFLICTS: {len(conflicts)}")
    print("!" * 60)
    for c in conflicts:
        print(f"  - [{c.conflict_id}] {c.topic}: claim {c.claim_a_id} vs {c.claim_b_id}")
        if c.notes:
            print(f"      notes: {c.notes}")


async def _confirm_unresolved_conflicts(
    report: IncidentReport, conflicts: list[ConflictRecord]
) -> bool | None:
    """Force a human decision on a diagnosis with unresolved conflicts.

    Returns True if a human accepted the diagnosis despite the conflicts, False
    if rejected, None if no input is available (automated run). In every case
    the conflicts have already been surfaced and are recorded in the audit, so
    the diagnosis never passes silently.
    """
    print(
        f"\nDiagnosis for '{report.title}' has {len(conflicts)} unresolved "
        "conflict(s). Accept for escalation anyway? (y/n): ",
        end="",
        flush=True,
    )
    try:
        response = input()
    except EOFError:
        logger.warning(
            "No human input available for unresolved conflicts on incident %s; "
            "logged for manual review",
            report.incident_id,
        )
        return None

    if response.strip().lower() in ("y", "yes"):
        logger.info(
            "Human accepted diagnosis for incident %s despite %d unresolved conflict(s)",
            report.incident_id,
            len(conflicts),
        )
        return True
    logger.warning(
        "Human rejected diagnosis for incident %s due to unresolved conflicts",
        report.incident_id,
    )
    return False
