"""Cross-cycle volatility gauge (issue #77).

The coordinator is stateless between monitoring cycles: each cycle rebuilds
context and, without help, treats every sighting of a persistent anomaly as the
first. This gauge is a lightweight in-memory cross-cycle memory: it fingerprints
each detected anomaly, tracks it across cycles, classifies it as new / ongoing /
worsening / resolved, and decides whether it warrants re-escalation.

Two jobs:
  1. Idempotency: a persistent, unchanged anomaly is diagnosed and reported once,
     then suppressed until it materially changes or resolves (no alert spam).
  2. Change awareness: a human-readable "since last cycle" summary is fed into the
     next detection so the agent knows an anomaly is ongoing/worsening, not new.

State lives on the coordinator instance (which persists across the run loop), so
it resets on process restart. That is an accepted tradeoff for the gauge; durable
history already lives in the audit log.
"""

import re
from dataclasses import dataclass, field
from enum import StrEnum

from streamops_mcp.agent.schemas import DetectedAnomaly

_LEADING_NUMBER = re.compile(r"[-+]?\d*\.?\d+")


class IncidentStatus(StrEnum):
    """How a detected anomaly relates to what the gauge has seen before."""

    NEW = "new"
    ONGOING = "ongoing"
    WORSENING = "worsening"


def _parse_value(value: str | None) -> float | None:
    """Extract a leading number from an observed_value string ('2,340ms' -> 2340)."""
    if value is None:
        return None
    match = _LEADING_NUMBER.search(value.replace(",", ""))
    return float(match.group()) if match else None


@dataclass
class _IncidentMemory:
    fingerprint: str
    anomaly_type: str
    component: str
    first_cycle: int
    last_cycle: int
    occurrences: int
    last_value: float | None
    reported: bool = False


@dataclass
class VolatilityDelta:
    """The cross-cycle verdict for one cycle's detected anomaly."""

    status: IncidentStatus
    fingerprint: str
    occurrences: int
    should_report: bool
    resolved: list[str] = field(default_factory=list)


class VolatilityGauge:
    """Tracks anomalies across cycles for dedup and change-awareness."""

    def __init__(
        self,
        *,
        ongoing_gap: int = 1,
        worsen_pct: float = 0.25,
        dedup: bool = True,
    ):
        self._ongoing_gap = ongoing_gap
        self._worsen_pct = worsen_pct
        self._dedup = dedup
        self._memory: dict[str, _IncidentMemory] = {}

    @staticmethod
    def fingerprint(anomaly: DetectedAnomaly) -> str:
        """Stable dedup key for an anomaly: its type on its component."""
        component = anomaly.affected_component or "unknown"
        return f"{anomaly.anomaly_type}:{component}"

    def _is_active(self, mem: _IncidentMemory, cycle: int) -> bool:
        """Was this incident seen recently enough to still be the same occurrence?"""
        return cycle - mem.last_cycle <= self._ongoing_gap

    def prior_context(self, cycle: int) -> str:
        """A 'since last cycle' summary of still-active incidents, for detection.

        Empty string when there is nothing carried over (e.g. the first cycle).
        """
        active = [
            m
            for m in self._memory.values()
            if cycle - m.last_cycle <= self._ongoing_gap and cycle > m.last_cycle
        ]
        if not active:
            return ""
        lines = [
            f"- {m.anomaly_type} on {m.component}: active {m.occurrences} cycle(s)"
            + (f", last observed value {m.last_value:g}" if m.last_value is not None else "")
            for m in sorted(active, key=lambda m: m.fingerprint)
        ]
        return (
            "Context from prior cycles (for continuity, confirm whether these persist):\n"
            + "\n".join(lines)
        )

    def observe(self, anomaly: DetectedAnomaly, cycle: int) -> VolatilityDelta:
        """Classify this cycle's anomaly against history, update memory, decide reporting.

        Also marks any other previously-active incident as resolved (a different
        anomaly this cycle means the prior one is no longer the active symptom).
        """
        fp = self.fingerprint(anomaly)
        value = _parse_value(anomaly.observed_value)

        resolved = [
            m.fingerprint
            for m in self._memory.values()
            if m.fingerprint != fp and self._is_active(m, cycle - 1)
        ]

        mem = self._memory.get(fp)
        if mem is None or not self._is_active(mem, cycle):
            # New incident, or the same fingerprint returning after a gap.
            status = IncidentStatus.NEW
            self._memory[fp] = _IncidentMemory(
                fingerprint=fp,
                anomaly_type=anomaly.anomaly_type,
                component=anomaly.affected_component or "unknown",
                first_cycle=cycle,
                last_cycle=cycle,
                occurrences=1,
                last_value=value,
            )
            mem = self._memory[fp]
        else:
            worsened = self._worsened(mem.last_value, value)
            status = IncidentStatus.WORSENING if worsened else IncidentStatus.ONGOING
            mem.occurrences += 1
            mem.last_cycle = cycle
            mem.last_value = value

        should_report = (
            status in (IncidentStatus.NEW, IncidentStatus.WORSENING)
            or not self._dedup
            or not mem.reported
        )
        return VolatilityDelta(
            status=status,
            fingerprint=fp,
            occurrences=mem.occurrences,
            should_report=should_report,
            resolved=sorted(resolved),
        )

    def _worsened(self, prior: float | None, current: float | None) -> bool:
        """True if the observed value rose by at least the worsening threshold."""
        if prior is None or current is None or prior <= 0:
            return False
        return (current - prior) / prior >= self._worsen_pct

    def mark_reported(self, fingerprint: str) -> None:
        """Record that an incident was actually reported, so dedup can suppress repeats."""
        mem = self._memory.get(fingerprint)
        if mem is not None:
            mem.reported = True

    def note_all_clear(self, cycle: int) -> list[str]:
        """A healthy cycle resolves every previously-active incident; return them."""
        return sorted(m.fingerprint for m in self._memory.values() if self._is_active(m, cycle - 1))
