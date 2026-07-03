"""Escalation logic: routes incidents by severity.

Severity routing:
  - LOW:      Log only, store for historical analysis
  - MEDIUM:   Log + CLI notification
  - HIGH:     Log + prominent CLI alert
  - CRITICAL: Log + human-in-the-loop (pause for confirmation)

CRITICAL incidents require a human to acknowledge before the system takes
any remediation action, preventing autonomous systems from making things
worse during a severe outage.
"""

import logging

from streamops_mcp.agent.audit import AuditLogger
from streamops_mcp.agent.schemas import DiagnosisReport, IncidentReport, Severity

logger = logging.getLogger("streamops-mcp.escalation")

_audit = AuditLogger()


async def escalate(
    report: IncidentReport,
    diagnosis: DiagnosisReport | None = None,
) -> None:
    """Route an incident report through the escalation chain."""
    logger.info("Escalating incident '%s' (severity=%s)", report.title, report.severity.value)

    human_approved = None

    if report.severity == Severity.LOW:
        await _handle_low(report)
    elif report.severity == Severity.MEDIUM:
        await _handle_medium(report)
    elif report.severity == Severity.HIGH:
        await _handle_high(report)
    elif report.severity == Severity.CRITICAL:
        human_approved = await _handle_critical(report)

    _audit.log_incident(report, diagnosis=diagnosis, human_approved=human_approved)


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
