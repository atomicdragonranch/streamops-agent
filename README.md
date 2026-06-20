# StreamOps Agent

[![CI](https://github.com/atomicdragonranch/streamops-agent/actions/workflows/ci.yml/badge.svg)](https://github.com/atomicdragonranch/streamops-agent/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Java 17+](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://openjdk.org/)
[![Tests](https://img.shields.io/badge/tests-37%20passing-brightgreen.svg)]()

AI-powered operations agent for streaming infrastructure monitoring. Built with Apache Flink 2.0, Kafka, and Claude to detect anomalies, diagnose root causes, and escalate incidents in real-time pipelines.

### Live Observability

Provisioned Grafana dashboard showing Flink job health, checkpoint performance, backpressure, and JVM metrics. All panels are auto-populated from Prometheus scrapes of the running Flink cluster.

![Flink Overview Dashboard](docs/images/grafana-flink-overview.png)

### Anomaly Detection in Action

The monitor agent runs a health check using MCP tools (Flink REST API, Kafka consumer groups, Prometheus metrics), detects an anomaly, and hands off to a diagnostic sub-agent for independent investigation. Here, it caught two consecutive job failures caused by a Kryo serialization issue and spawned the diagnostic agent to verify the root cause.

![Agent Anomaly Detection](docs/images/agent-anomaly-detection.png)

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
    flink-parent/             # Shared Flink dependency management (Maven parent POM)
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
  config/
    prometheus.yml            # Scrape config for Flink JM + TM metrics
    grafana/provisioning/
      datasources/            # Auto-provisions Prometheus datasource
      dashboards/             # Flink Overview dashboard (auto-loaded)
  docs/images/                # Screenshots for README
  scripts/                    # Demo scenario runner
  .github/workflows/          # CI pipeline (test, lint, build)
  docker-compose.yml          # Kafka KRaft, Flink JM+TM, Kafka UI, Prometheus, Grafana
```

## Test Coverage

| Module | Tests | Framework |
|--------|-------|-----------|
| Event Simulator | 23 | JUnit 5, AssertJ |
| Stream Processor | 14 | JUnit 5, AssertJ, Mockito |
| MCP Server + Agent | 53 | pytest |
| **Total** | **90** | |

## Quick Start

### Prerequisites

- **Docker Desktop** (with Docker Compose v2)
- **JDK 17+** for building and running Java modules
- **Python 3.11+** with [uv](https://docs.astral.sh/uv/) for the MCP server and agent
- **Maven 3.9+** for building the Java modules

### 1. Start Infrastructure

```bash
docker compose up -d
```

This starts Kafka (KRaft mode), Flink (JobManager + TaskManager), Prometheus, Grafana, and Kafka UI. Topics are created automatically by the `kafka-init` container.

### 2. Build Java Modules

```bash
cd java && mvn clean package -DskipTests
```

### 3. Submit the Flink Job

```bash
docker exec streamops-flink-jm mkdir -p /opt/flink/jobs
docker cp java/stream-processor/target/stream-processor-0.1.0-SNAPSHOT.jar \
  streamops-flink-jm:/opt/flink/jobs/stream-processor.jar
docker exec streamops-flink-jm flink run -d /opt/flink/jobs/stream-processor.jar
```

### 4. Start the Event Simulator

```bash
java -jar java/event-simulator/target/event-simulator-0.1.0-SNAPSHOT.jar latency-spike
```

Available scenarios: `latency-spike`, `throughput-drop`, `error-burst`, `backpressure`, `checkpoint-timeout`, `memory-pressure`.

### 5. Run the AI Agent

Requires `ANTHROPIC_API_KEY` in your environment.

```bash
cd mcp-server && uv sync

# Single cycle: run one health check, then exit
uv run python -m streamops_mcp.agent.main --single-cycle

# Continuous monitoring: repeat every 60s until stopped
uv run python -m streamops_mcp.agent.main
```

All Python config has sensible defaults in `config.py` and can be overridden with `STREAMOPS_`-prefixed environment variables (e.g., `STREAMOPS_AGENT_MONITOR_INTERVAL=30`, `STREAMOPS_KAFKA_BOOTSTRAP=kafka:29092`). Java config works the same way: defaults in `application.properties`, overridden by env vars like `KAFKA_BOOTSTRAP`.

### Web Dashboards

Once the stack is running, these dashboards are available in your browser:

| Dashboard | URL | Purpose |
|-----------|-----|---------|
| **Flink Dashboard** | http://localhost:8081 | Job status, task managers, checkpoints, backpressure, exceptions |
| **Kafka UI** | http://localhost:8080 | Browse topics, view messages, consumer groups, partition layout |
| **Prometheus** | http://localhost:9090 | Raw metrics, PromQL queries, target health |
| **Grafana** | http://localhost:3333 | Pre-configured dashboards for Flink metrics (admin/streamops) |

### Troubleshooting

**Flink job fails with "Failed to create checkpoint storage"**

The checkpoint directory inside the Flink containers may not have write permissions on first run. Fix with:

```bash
docker exec streamops-flink-jm bash -c "mkdir -p /tmp/flink-checkpoints && chmod 777 /tmp/flink-checkpoints"
docker exec streamops-flink-tm bash -c "mkdir -p /tmp/flink-checkpoints && chmod 777 /tmp/flink-checkpoints"
```

Then resubmit the job (see step 3 above).

**Flink job restarts with "Connection to node localhost:9092 could not be established"**

The stream processor defaults to `localhost:9092` for Kafka, which works on the host but not inside Docker containers. The `docker-compose.yml` sets `KAFKA_BOOTSTRAP=kafka:29092` on both Flink containers to override this. If you see this error, verify the env var is set:

```bash
docker exec streamops-flink-tm printenv KAFKA_BOOTSTRAP
# Should output: kafka:29092
```

If it's missing, recreate the containers: `docker compose down && docker compose up -d`.

**Simulator fails with "UnsupportedClassVersionError"**

The Java modules are compiled with JDK 17 target. If your default `java` on PATH is older than JDK 17, run the simulator with an explicit path:

```bash
# Find your JDK 17+ installation
$JAVA_HOME/bin/java -jar java/event-simulator/target/event-simulator-0.1.0-SNAPSHOT.jar
```

**Port 3000 (or 3333) already in use**

Another application is using the Grafana port. Edit `docker-compose.yml` and change the host port mapping for the `grafana` service (e.g., `"3333:3000"` to `"4000:3000"`).

**Simulator produces events but Flink shows LAG = 0 and no alerts**

This is normal during warm-up. The AnomalyDetector uses exponential moving averages (EMA) that need several data points before triggering. Run the simulator for at least 30 seconds with an anomaly scenario, then check the `stream-alerts` topic in Kafka UI.

## Architectural Patterns

| Pattern | Implementation |
|---------|----------------|
| Agentic loop (stop_reason driven) | `monitor.py:_detect_anomalies()` |
| Tool use (MCP tools) | `executor.py`, `tools.py` |
| Structured output (Pydantic) | `schemas/diagnosis.py`, `schemas/incident.py` |
| Multi-agent coordinator (hub-and-spoke) | `monitor.py:MonitorAgent` |
| Sub-agent context injection | `monitor.py:_spawn_diagnostic_agent()` |
| Claim-source attribution | `schemas/diagnosis.py:ClaimRecord + SourceRecord` |
| Conflict annotation + escalation | `schemas/diagnosis.py:ConflictRecord` |
| Session isolation (blank sub-agents) | `monitor.py:_spawn_*_agent()` |
| Human-in-the-loop | `escalation.py:_handle_critical()` |
| Config externalization | `config.py`, `application.properties` |
