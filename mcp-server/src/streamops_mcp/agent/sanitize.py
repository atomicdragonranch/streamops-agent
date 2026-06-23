"""Input sanitization for untrusted data sources.

Kafka event content and log payloads may contain user input, stack traces,
or malformed data. Sanitize before injecting into agent prompts to prevent
prompt injection and keep context clean.
"""

import re

from streamops_mcp.config import config

_INJECTION_PATTERNS = [
    re.compile(r"<\s*/?\s*(?:system|human|assistant|tool_use|tool_result)\b", re.IGNORECASE),
    re.compile(r"\bignore\s+(?:(?:all|previous|above)\s+)*instructions?\b", re.IGNORECASE),
    re.compile(r"\byou\s+are\s+now\b", re.IGNORECASE),
    re.compile(r"\bnew\s+instructions?\s*:", re.IGNORECASE),
]


def sanitize_tool_output(raw: str, source_trust: str = "untrusted") -> str:
    """Sanitize tool output before injecting into agent context.

    Trusted sources pass through with only length truncation.
    Semi-trusted sources get length truncation.
    Untrusted sources get injection pattern stripping and length truncation.
    """
    max_len = config.agent_sanitize_max_output_chars

    if source_trust == "trusted":
        return raw[:max_len]

    if source_trust == "untrusted":
        sanitized = raw
        for pattern in _INJECTION_PATTERNS:
            sanitized = pattern.sub("[REDACTED]", sanitized)
        return sanitized[:max_len]

    return raw[:max_len]
