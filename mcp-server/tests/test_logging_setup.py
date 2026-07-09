"""Tests for correlated logging (issue #84).

Verifies that a per-cycle correlation id is bound to the context, propagates
to every log record emitted while it is set (including from other modules via
the shared filter), is unique across cycles, and falls back to a sentinel
outside any cycle.
"""

import logging

import pytest

from streamops_mcp.logging_setup import (
    CorrelationIdFilter,
    configure_logging,
    get_correlation_id,
    new_correlation_id,
    reset_correlation_id,
    set_correlation_id,
)


@pytest.fixture
def captured_records():
    """Attach a capturing handler (with the correlation filter) to the root logger.

    Mirrors the production handler: the filter injects correlation_id onto every
    record so the format string never hits a missing attribute.
    """
    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _Capture()
    handler.addFilter(CorrelationIdFilter())
    root = logging.getLogger()
    root.addHandler(handler)
    prev_level = root.level
    root.setLevel(logging.INFO)
    try:
        yield records
    finally:
        root.removeHandler(handler)
        root.setLevel(prev_level)


class TestCorrelationId:
    def test_new_correlation_id_is_unique_across_calls(self):
        # Arrange / Act
        a = new_correlation_id()
        b = new_correlation_id()

        # Assert: two cold cycles never collide (the restart-collision bug the
        # old int counter had).
        assert a != b
        assert a.startswith("cyc-")

    def test_records_carry_the_bound_correlation_id(self, captured_records):
        # Arrange
        cid = new_correlation_id()
        token = set_correlation_id(cid)

        # Act: log from two different module loggers within the same cycle.
        try:
            logging.getLogger("streamops-mcp.monitor").info("cycle start")
            logging.getLogger("streamops-mcp.executor").info("tool call")
        finally:
            reset_correlation_id(token)

        # Assert: every line from the cycle shares the one id, regardless of
        # which module emitted it.
        assert len(captured_records) == 2
        assert {r.correlation_id for r in captured_records} == {cid}

    def test_two_cycles_get_distinct_ids(self, captured_records):
        # Arrange / Act
        cid1 = new_correlation_id()
        token = set_correlation_id(cid1)
        logging.getLogger("streamops-mcp.monitor").info("cycle 1")
        reset_correlation_id(token)

        cid2 = new_correlation_id()
        token = set_correlation_id(cid2)
        logging.getLogger("streamops-mcp.monitor").info("cycle 2")
        reset_correlation_id(token)

        # Assert
        assert captured_records[0].correlation_id == cid1
        assert captured_records[1].correlation_id == cid2
        assert cid1 != cid2

    def test_record_outside_any_cycle_uses_sentinel(self, captured_records):
        # Arrange / Act: no correlation id has been set in this context.
        logging.getLogger("streamops-mcp").info("startup line")

        # Assert: sentinel, not a crash on a missing attribute.
        assert captured_records[0].correlation_id == "-"

    def test_reset_restores_previous_value(self):
        # Arrange
        assert get_correlation_id() == "-"

        # Act
        token = set_correlation_id("cyc-abc")
        assert get_correlation_id() == "cyc-abc"
        reset_correlation_id(token)

        # Assert
        assert get_correlation_id() == "-"


class TestConfigureLogging:
    def test_configure_logging_is_idempotent(self):
        # Arrange / Act: calling twice must not stack duplicate handlers.
        configure_logging()
        count_after_first = len(logging.getLogger().handlers)
        configure_logging()
        count_after_second = len(logging.getLogger().handlers)

        # Assert
        assert count_after_first == 1
        assert count_after_second == 1

    def test_format_renders_correlation_id_without_error(self):
        # Arrange
        configure_logging()
        handler = logging.getLogger().handlers[0]
        cid = new_correlation_id()
        token = set_correlation_id(cid)
        record = logging.LogRecord(
            name="streamops-mcp.monitor",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )

        # Act: the filter must populate the attribute before the formatter reads it.
        try:
            handler.filters[0].filter(record)
            rendered = handler.formatter.format(record)
        finally:
            reset_correlation_id(token)

        # Assert
        assert f"cid={cid}" in rendered
