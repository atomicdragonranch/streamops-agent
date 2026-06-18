"""Demo scenario runner: end-to-end anomaly injection, detection, and diagnosis.

Orchestrates the full StreamOps pipeline:
1. Start the event simulator with a chosen anomaly scenario
2. Wait for events to flow through Kafka -> Flink -> alerts
3. Run the agent in single-cycle mode to detect, diagnose, and report
4. Save the structured DiagnosisReport + IncidentReport as JSON

Prerequisites:
    - Docker Compose stack running (yarn infra:up)
    - Flink job submitted (yarn flink:submit)
    - Java simulator JAR built (yarn build:java)

Usage:
    python scripts/demo_scenario.py latency-spike
    python scripts/demo_scenario.py --all
    python scripts/demo_scenario.py --list
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MCP_SERVER_DIR = PROJECT_ROOT / "mcp-server"
OUTPUT_DIR = PROJECT_ROOT / "demo-output"
SIMULATOR_JAR = PROJECT_ROOT / "java" / "event-simulator" / "target" / "event-simulator-0.1.0-SNAPSHOT.jar"

SCENARIOS = [
    "latency-spike",
    "throughput-drop",
    "error-burst",
    "backpressure",
    "checkpoint-timeout",
    "memory-pressure",
]

SCENARIO_WAIT_SECONDS = {
    "latency-spike": 15,
    "throughput-drop": 20,
    "error-burst": 10,
    "backpressure": 20,
    "checkpoint-timeout": 25,
    "memory-pressure": 15,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [demo] %(levelname)s %(message)s",
)
logger = logging.getLogger("demo")


def check_prerequisites():
    if not SIMULATOR_JAR.exists():
        logger.error("Simulator JAR not found at %s", SIMULATOR_JAR)
        logger.error("Run: yarn build:java")
        return False

    result = subprocess.run(
        ["docker", "compose", "ps", "--format", "json"],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT),
    )
    if result.returncode != 0:
        logger.error("Docker Compose not running. Run: yarn infra:up")
        return False

    return True


def start_simulator(scenario: str) -> subprocess.Popen:
    logger.info("Starting simulator with scenario: %s", scenario)
    proc = subprocess.Popen(
        ["java", "-jar", str(SIMULATOR_JAR), scenario],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(PROJECT_ROOT),
    )
    return proc


def stop_simulator(proc: subprocess.Popen):
    if proc.poll() is None:
        logger.info("Stopping simulator (pid=%d)", proc.pid)
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


async def run_agent_cycle(multi_agent: bool = True):
    sys.path.insert(0, str(MCP_SERVER_DIR / "src"))

    from streamops_mcp.agent.monitor import MonitorAgent

    agent = MonitorAgent(multi_agent=multi_agent)
    report = await agent.run_cycle()
    return report


def save_output(scenario: str, report, diagnosis_data: dict | None = None):
    OUTPUT_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base_name = f"{scenario}_{timestamp}"

    if report is not None:
        report_path = OUTPUT_DIR / f"{base_name}_incident.json"
        report_path.write_text(report.model_dump_json(indent=2))
        logger.info("Incident report saved: %s", report_path)
    else:
        summary_path = OUTPUT_DIR / f"{base_name}_healthy.json"
        summary_path.write_text(json.dumps({
            "scenario": scenario,
            "timestamp": timestamp,
            "result": "no_anomaly_detected",
        }, indent=2))
        logger.info("No anomaly detected, summary saved: %s", summary_path)


async def run_scenario(scenario: str, skip_simulator: bool = False):
    logger.info("=" * 60)
    logger.info("SCENARIO: %s", scenario)
    logger.info("=" * 60)

    sim_proc = None
    try:
        if not skip_simulator:
            sim_proc = start_simulator(scenario)
            wait_seconds = SCENARIO_WAIT_SECONDS.get(scenario, 15)
            logger.info(
                "Waiting %ds for events to flow through the pipeline...",
                wait_seconds,
            )
            time.sleep(wait_seconds)

        logger.info("Running agent monitoring cycle...")
        report = await run_agent_cycle()

        save_output(scenario, report)

        if report:
            logger.info("Result: %s severity incident detected", report.severity.value)
            logger.info("Title: %s", report.title)
            logger.info("Root cause: %s", report.root_cause)
        else:
            logger.info("Result: infrastructure appeared healthy to the agent")

        return report

    finally:
        if sim_proc:
            stop_simulator(sim_proc)


async def run_all_scenarios():
    results = {}
    for scenario in SCENARIOS:
        try:
            report = await run_scenario(scenario)
            results[scenario] = {
                "detected": report is not None,
                "severity": report.severity.value if report else None,
                "title": report.title if report else None,
            }
        except Exception as e:
            logger.error("Scenario '%s' failed: %s", scenario, e, exc_info=True)
            results[scenario] = {"detected": False, "error": str(e)}

        logger.info("Cooling down 10s before next scenario...")
        time.sleep(10)

    summary_path = OUTPUT_DIR / "all_scenarios_summary.json"
    summary_path.write_text(json.dumps(results, indent=2))
    logger.info("All scenarios complete. Summary: %s", summary_path)
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Run end-to-end demo scenarios for StreamOps Agent",
    )
    parser.add_argument(
        "scenario", nargs="?",
        help="Scenario to run (e.g., latency-spike)",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Run all 6 scenarios sequentially",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List available scenarios",
    )
    parser.add_argument(
        "--skip-simulator", action="store_true",
        help="Skip starting the simulator (assume it is already running)",
    )
    args = parser.parse_args()

    if args.list:
        print("Available scenarios:")
        for s in SCENARIOS:
            wait = SCENARIO_WAIT_SECONDS.get(s, 15)
            print(f"  {s} (wait: {wait}s)")
        return

    if not check_prerequisites():
        sys.exit(1)

    if args.all:
        asyncio.run(run_all_scenarios())
    elif args.scenario:
        if args.scenario not in SCENARIOS:
            logger.error(
                "Unknown scenario '%s'. Available: %s",
                args.scenario, ", ".join(SCENARIOS),
            )
            sys.exit(1)
        asyncio.run(run_scenario(args.scenario, skip_simulator=args.skip_simulator))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
