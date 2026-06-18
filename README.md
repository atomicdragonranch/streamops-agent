# StreamOps Agent

AI-powered operations agent for streaming infrastructure monitoring. Built with Apache Flink 2.0, Kafka, and Claude to detect anomalies, diagnose root causes, and escalate incidents in real-time pipelines.

## Architecture Overview

```mermaid
graph TB
    subgraph "Java Streaming Layer"
        ES[Event Simulator<br/>Kafka Producer]
        K[Apache Kafka<br/>KRaft Mode]
        FP[Flink Stream Processor<br/>Flink 2.0.2]
        MA[MetricAggregator<br/>30s Tumbling Windows]
        AD[AnomalyDetector<br/>Keyed State + EMA]
    end

    subgraph "Python MCP Server"
        MCP[FastMCP Server<br/>8 Observability Tools]
        FT[Flink Tools]
        KT[Kafka Tools]
        PT[Prometheus Tools]
        ET[Event Tools]
    end

    subgraph "Python Agent Layer"
        MON[Monitor Agent<br/>Coordinator]
        DIAG[Diagnostic Agent<br/>Specialist]
        REP[Report Agent<br/>Specialist]
        ESC[Escalation Engine]
    end

    subgraph "Infrastructure"
        PROM[Prometheus]
        GRAF[Grafana]
    end

    ES -->|Protobuf| K
    K --> FP
    FP --> MA
    FP --> AD
    AD -->|Alerts| K
    FP -.->|Metrics| PROM
    PROM --> GRAF

    MCP --> FT & KT & PT & ET
    FT -->|REST API| FP
    KT -->|Consumer API| K
    PT -->|PromQL| PROM
    ET -->|Consumer API| K

    MON -->|Spawns| DIAG
    MON -->|Spawns| REP
    DIAG -->|Uses| MCP
    REP -->|Reads| DIAG
    MON --> ESC
```

## Multi-Agent Topology

Hub-and-spoke pattern: the Monitor Agent is the coordinator. Sub-agents start with zero context; all information is injected via structured prompts.

```mermaid
graph LR
    subgraph "Coordinator"
        MON[Monitor Agent<br/>Owns the loop]
    end

    subgraph "Sub-Agents"
        DIAG[Diagnostic Agent<br/>Tools + Investigation]
        REP[Report Agent<br/>Synthesis Only]
    end

    MON -->|"Anomaly context<br/>(full text + schema)"| DIAG
    DIAG -->|"DiagnosisReport<br/>(claims + sources + conflicts)"| MON
    MON -->|"DiagnosisReport JSON<br/>(structured, attributed)"| REP
    REP -->|"IncidentReport<br/>(severity + actions)"| MON
```

## Agentic Loop

The core loop is driven by Claude's `stop_reason`. The agent keeps calling tools until it decides it has enough information.

```mermaid
flowchart TD
    START([Cycle Start]) --> POLL[Send system prompt<br/>+ tool definitions]
    POLL --> API[Claude API Call]
    API --> CHECK{stop_reason?}

    CHECK -->|tool_use| EXEC[Execute tool calls<br/>via MCP server]
    EXEC --> RESULT[Append tool_result<br/>to messages]
    RESULT --> ROUNDS{Max rounds<br/>reached?}
    ROUNDS -->|No| API
    ROUNDS -->|Yes| FALLBACK[Use accumulated text]

    CHECK -->|end_turn| ANOMALY{Anomaly<br/>detected?}
    ANOMALY -->|No| HEALTHY([Infrastructure Healthy])
    ANOMALY -->|Yes| SPAWN[Spawn Diagnostic Agent]
    SPAWN --> REPORT[Spawn Report Agent]
    REPORT --> ESCALATE[Route by Severity]

    FALLBACK --> ANOMALY
```

## Data Flow

```mermaid
flowchart LR
    subgraph "Event Generation"
        SIM[Event Simulator]
        MG[MetricGenerator]
        LG[LogGenerator]
        HG[HeartbeatGenerator]
        SR[ScenarioRunner<br/>6 Anomaly Scenarios]
    end

    subgraph "Stream Processing"
        KT[Kafka Topic<br/>stream-events]
        DESER[StreamEventDeserializer<br/>Protobuf]
        SPLIT{Event Type?}
        AGG[MetricAggregator<br/>30s Windows]
        DET[AnomalyDetector<br/>Keyed State]
        ALERT[Kafka Topic<br/>stream-alerts]
    end

    SIM --> MG & LG & HG
    SR --> SIM
    MG & LG & HG -->|Protobuf| KT
    KT --> DESER
    DESER --> SPLIT
    SPLIT -->|Metric| AGG
    SPLIT -->|All| DET
    DET -->|Threshold breach| ALERT
```

## Escalation Flow

```mermaid
flowchart TD
    INC[Incident Report] --> SEV{Severity?}

    SEV -->|LOW| LOG1[Log for<br/>historical analysis]
    SEV -->|MEDIUM| LOG2[Log warning +<br/>CLI notification]
    SEV -->|HIGH| LOG3[Log error +<br/>CLI alert +<br/>recommended actions]
    SEV -->|CRITICAL| HITL[Human-in-the-Loop]

    HITL --> PROMPT[Display incident details<br/>+ recommended actions]
    PROMPT --> HUMAN{Human<br/>approves?}
    HUMAN -->|Yes| APPROVE[Proceed with<br/>remediation]
    HUMAN -->|No| REJECT[Log for<br/>manual review]
```

## Claim-Source Attribution

Every diagnostic finding traces back to the tool and data that produced it. Conflicts between sources are annotated and escalated to the coordinator, never silently resolved.

```mermaid
graph TD
    subgraph "Sources"
        S1[src-001<br/>query_flink_jobs]
        S2[src-002<br/>get_consumer_lag]
        S3[src-003<br/>query_metrics]
    end

    subgraph "Claims"
        C1[C01: Flink job RUNNING<br/>but degraded]
        C2[C02: Consumer lag<br/>45,000 on partition 2]
        C3[C03: Checkpoint duration<br/>within threshold]
    end

    subgraph "Conflicts"
        CONF[conf-001: Job status<br/>resolution: unresolved]
    end

    S1 --> C1
    S2 --> C2
    S3 --> C3
    C1 & C3 -.->|Contradictory| CONF
    CONF -->|Escalate| COORD[Coordinator decides]
```

## Configuration Hierarchy

Both Java and Python follow the same principle: defaults in file, override via environment.

```mermaid
flowchart LR
    subgraph "Java"
        JP[application.properties<br/>on classpath]
        JE[Environment Variables<br/>e.g. KAFKA_BOOTSTRAP]
        JC[Properties Object<br/>Constructor Injection]
    end

    subgraph "Python"
        PP[pydantic-settings<br/>StreamOpsConfig]
        PE[Environment Variables<br/>STREAMOPS_ prefix]
        PC[config singleton]
    end

    JP -->|"Defaults"| JC
    JE -->|"Overrides"| JC
    PP -->|"Defaults"| PC
    PE -->|"Overrides"| PC
```

## Project Structure

```
streamops-agent/
  java/
    proto/                    # Protobuf schema (StreamEvent)
    event-simulator/          # Standalone Kafka producer, 6 anomaly scenarios
    stream-processor/         # Flink 2.0 job (MetricAggregator + AnomalyDetector)
  mcp-server/
    src/streamops_mcp/
      tools/                  # 8 MCP observability tools (Flink, Kafka, Prometheus, Events)
      agent/
        monitor.py            # Coordinator agent (agentic loop, sub-agent spawning)
        escalation.py         # Severity routing + HITL
        executor.py           # Tool dispatch bridge
        tools.py              # Claude API tool definitions (scoped per agent role)
        schemas/              # Pydantic models (DiagnosisReport, IncidentReport)
        main.py               # CLI entry point
      config.py               # pydantic-settings config
    tests/                    # 53 Python tests
  docker-compose.yml          # Kafka KRaft, Flink JM+TM, Prometheus, Grafana
```

## Test Coverage

| Module | Tests | Framework |
|--------|-------|-----------|
| Event Simulator | 20 | JUnit 5, AssertJ |
| Stream Processor | 14 | JUnit 5, AssertJ, Mockito |
| MCP Server + Agent | 53 | pytest |
| **Total** | **87** | |

## Quick Start

```bash
# Start infrastructure
docker compose up -d

# Build Java modules
cd java && mvn clean package -DskipTests

# Start event simulator
java -jar event-simulator/target/event-simulator-0.1.0-SNAPSHOT.jar

# Start Flink processor (submit to running Flink cluster)
# See yarn scripts for orchestration

# Start MCP server
cd mcp-server && uv run python -m streamops_mcp.server

# Run the agent
cd mcp-server && uv run python -m streamops_mcp.agent.main
```

## Cert Reference

This project demonstrates patterns from the Claude Certified Architect exam:

| Pattern | Domain | Implementation |
|---------|--------|----------------|
| Agentic loop (stop_reason driven) | 1 | `monitor.py:_detect_anomalies()` |
| Tool use (MCP tools) | 1 | `executor.py`, `tools.py` |
| Structured output (Pydantic) | 1 | `schemas/diagnosis.py`, `schemas/incident.py` |
| Multi-agent coordinator | 1 | `monitor.py:MonitorAgent` |
| Sub-agent context injection | 1.3 | `monitor.py:_spawn_diagnostic_agent()` |
| Claim-source attribution | 1.3 | `schemas/diagnosis.py:ClaimRecord + SourceRecord` |
| Conflict annotation + escalation | 1.3 | `schemas/diagnosis.py:ConflictRecord` |
| Session isolation (blank sub-agents) | 1.7 | `monitor.py:_spawn_*_agent()` |
| Human-in-the-loop | 1 | `escalation.py:_handle_critical()` |
| Config externalization | -- | `config.py`, `application.properties` |
