# Trust Boundaries

StreamOps Agent consumes data from multiple sources with different trust levels. This document defines the trust classification for each source and the safeguards applied.

## Trust Levels

| Source | Trust Level | Rationale | Safeguards |
|--------|------------|-----------|------------|
| Flink REST API | **Trusted** | Internal admin API, not user-facing | Length truncation only |
| Prometheus metrics | **Trusted** | Internal metrics store, numeric data | Length truncation only |
| Kafka consumer lag | **Trusted** | Broker-reported metadata | Length truncation only |
| Kafka event content | **Semi-trusted** | Application-produced, schema-validated | Length truncation |
| Log content in events | **Untrusted** | May contain user input, stack traces | Injection pattern stripping + length truncation |
| Exception messages | **Untrusted** | May contain arbitrary strings from user data | Injection pattern stripping + length truncation |

## Injection Risks

Untrusted content injected into agent prompts could attempt:

- Prompt injection via fake XML tags (`<system>`, `<human>`, `<assistant>`)
- Instruction override ("ignore previous instructions")
- Role hijacking ("you are now...")

The `sanitize_tool_output()` function in `agent/sanitize.py` strips known injection patterns from untrusted sources before they reach the agent context.

## Agent Awareness

All agent system prompts include trust boundary awareness. The diagnostic agent is instructed to treat log content and exception messages as untrusted data that should not influence diagnostic conclusions without corroboration from trusted sources.

## Adding New Sources

When adding a new MCP tool or data source:

1. Classify its trust level in this document
2. Set the appropriate `source_trust` parameter when calling `sanitize_tool_output()`
3. If untrusted, verify that injection patterns are stripped in tests
