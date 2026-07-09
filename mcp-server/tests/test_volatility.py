"""Tests for the cross-cycle volatility gauge (issue #77)."""

from streamops_mcp.agent.schemas import DetectedAnomaly
from streamops_mcp.agent.volatility import IncidentStatus, VolatilityGauge


def _anom(anomaly_type="latency_spike", component="processor", value=None) -> DetectedAnomaly:
    return DetectedAnomaly(
        anomaly_type=anomaly_type,
        summary="an anomaly",
        detected_at="2026-07-09T12:00:00Z",
        affected_component=component,
        observed_value=value,
    )


class TestVolatilityGauge:
    def test_fingerprint_is_type_and_component(self):
        # Arrange
        gauge = VolatilityGauge()

        # Act + Assert
        assert gauge.fingerprint(_anom("latency_spike", "processor")) == "latency_spike:processor"
        assert gauge.fingerprint(_anom("latency_spike", None)) == "latency_spike:unknown"

    def test_first_sighting_is_new_and_reports(self):
        # Arrange
        gauge = VolatilityGauge()

        # Act
        delta = gauge.observe(_anom(), 1)

        # Assert
        assert delta.status == IncidentStatus.NEW
        assert delta.occurrences == 1
        assert delta.should_report is True

    def test_reported_ongoing_is_suppressed(self):
        # Arrange: reported on cycle 1
        gauge = VolatilityGauge()
        first = gauge.observe(_anom(), 1)
        gauge.mark_reported(first.fingerprint)

        # Act: same incident next cycle
        second = gauge.observe(_anom(), 2)

        # Assert: recognized as ongoing and suppressed (idempotency)
        assert second.status == IncidentStatus.ONGOING
        assert second.occurrences == 2
        assert second.should_report is False

    def test_ongoing_not_yet_reported_still_reports(self):
        # Arrange: cycle 1 was never marked reported (e.g. the pipeline aborted)
        gauge = VolatilityGauge()
        gauge.observe(_anom(), 1)

        # Act
        second = gauge.observe(_anom(), 2)

        # Assert: still owed a report
        assert second.status == IncidentStatus.ONGOING
        assert second.should_report is True

    def test_worsening_reescalates_even_if_reported(self):
        # Arrange: reported at value 2000
        gauge = VolatilityGauge(worsen_pct=0.25)
        first = gauge.observe(_anom(value="2000"), 1)
        gauge.mark_reported(first.fingerprint)

        # Act: value jumps 50%
        second = gauge.observe(_anom(value="3000"), 2)

        # Assert
        assert second.status == IncidentStatus.WORSENING
        assert second.should_report is True

    def test_small_change_is_not_worsening_and_stays_suppressed(self):
        # Arrange
        gauge = VolatilityGauge(worsen_pct=0.25)
        first = gauge.observe(_anom(value="2000"), 1)
        gauge.mark_reported(first.fingerprint)

        # Act: value rises only 5%
        second = gauge.observe(_anom(value="2100"), 2)

        # Assert
        assert second.status == IncidentStatus.ONGOING
        assert second.should_report is False

    def test_returns_as_new_after_gap(self):
        # Arrange: seen on cycle 1, then absent
        gauge = VolatilityGauge(ongoing_gap=1)
        first = gauge.observe(_anom(), 1)
        gauge.mark_reported(first.fingerprint)

        # Act: reappears on cycle 5 (gap of 4 > ongoing_gap)
        later = gauge.observe(_anom(), 5)

        # Assert: treated as a fresh incident
        assert later.status == IncidentStatus.NEW
        assert later.occurrences == 1
        assert later.should_report is True

    def test_other_active_incident_marked_resolved(self):
        # Arrange
        gauge = VolatilityGauge()
        first = gauge.observe(_anom("latency_spike", "processor"), 1)
        gauge.mark_reported(first.fingerprint)

        # Act: a different anomaly this cycle
        second = gauge.observe(_anom("backpressure", "sink"), 2)

        # Assert: the prior active incident is reported resolved
        assert "latency_spike:processor" in second.resolved

    def test_prior_context_empty_then_summarizes_active(self):
        # Arrange
        gauge = VolatilityGauge()

        # Assert: nothing carried over on the first cycle
        assert gauge.prior_context(1) == ""

        # Act
        gauge.observe(_anom("latency_spike", "processor"), 1)
        context = gauge.prior_context(2)

        # Assert
        assert "latency_spike" in context
        assert "processor" in context

    def test_note_all_clear_lists_active_incidents(self):
        # Arrange
        gauge = VolatilityGauge()
        gauge.observe(_anom("latency_spike", "processor"), 1)

        # Act
        resolved = gauge.note_all_clear(2)

        # Assert
        assert resolved == ["latency_spike:processor"]

    def test_dedup_disabled_always_reports(self):
        # Arrange
        gauge = VolatilityGauge(dedup=False)
        first = gauge.observe(_anom(), 1)
        gauge.mark_reported(first.fingerprint)

        # Act
        second = gauge.observe(_anom(), 2)

        # Assert: no suppression when dedup is off
        assert second.status == IncidentStatus.ONGOING
        assert second.should_report is True
