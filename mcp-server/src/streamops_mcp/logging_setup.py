"""Correlated logging setup.

Binds a per-cycle correlation id to a ContextVar so every log line emitted
anywhere in a monitoring cycle's async call tree (monitor, diagnostic and
report sub-agents, tool executor, escalation) carries the same id, without
threading it through every function signature. ContextVars propagate across
``await`` within the same asyncio task, so setting the id once at the top of
``run_cycle`` covers the whole cycle.

A log line with no correlation id is orphaned the moment two cycles overlap
or two incidents run concurrently: the lines interleave with no way to
reconstruct one incident's story. See issue #84.
"""

import contextvars
import logging
import uuid

# Default sentinel for lines emitted outside any cycle (startup, config, the
# MCP server's own request handling). Never left unset, so the formatter's
# %(correlation_id)s always resolves.
_UNSET = "-"

_correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default=_UNSET
)

# Includes the correlation id so a single cycle's lines are greppable by cid=.
LOG_FORMAT = "%(asctime)s [%(name)s] [cid=%(correlation_id)s] %(levelname)s %(message)s"


def new_correlation_id(prefix: str = "cyc") -> str:
    """Generate a correlation id that is unique across process restarts.

    A monotonic per-process counter would collide across restarts (two cold
    starts both produce cycle 1); the uuid suffix makes each cycle's id
    globally distinct.
    """
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def set_correlation_id(cid: str) -> contextvars.Token:
    """Bind a correlation id to the current context; returns a reset token."""
    return _correlation_id.set(cid)


def reset_correlation_id(token: contextvars.Token) -> None:
    """Restore the correlation id to its previous value using the token."""
    _correlation_id.reset(token)


def get_correlation_id() -> str:
    """Return the correlation id bound to the current context (or the sentinel)."""
    return _correlation_id.get()


class CorrelationIdFilter(logging.Filter):
    """Injects the current correlation id onto every LogRecord.

    Attached to the handler so the attribute is always present before the
    formatter runs, including for third-party loggers that never heard of the
    correlation id.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = _correlation_id.get()
        return True


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logging with the correlation-id format and filter.

    Idempotent: replaces the root handlers so calling it from multiple entry
    points (agent main, MCP server) does not double-log or drop the filter.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    handler.addFilter(CorrelationIdFilter())

    root = logging.getLogger()
    root.setLevel(level)
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
