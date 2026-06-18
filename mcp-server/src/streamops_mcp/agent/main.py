"""StreamOps Agent entry point.

Runs the monitoring loop on a configurable interval. Each cycle:
1. Polls infrastructure via MCP tools
2. Detects anomalies
3. Spawns diagnostic + report sub-agents (multi-agent mode)
4. Escalates incidents by severity

Usage:
    python -m streamops_mcp.agent.main
    python -m streamops_mcp.agent.main --single-agent --interval 30
"""

import asyncio
import logging
import sys

from streamops_mcp.agent.monitor import MonitorAgent
from streamops_mcp.config import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("streamops-mcp.agent")


async def run_loop(interval_seconds: int | None = None, multi_agent: bool = True, single_cycle: bool = False):
    if interval_seconds is None:
        interval_seconds = config.agent_monitor_interval
    """Main monitoring loop."""
    agent = MonitorAgent(multi_agent=multi_agent)

    logger.info(
        "StreamOps Agent started: mode=%s, interval=%ds, model=%s",
        "multi-agent" if multi_agent else "single-agent",
        interval_seconds,
        agent.model,
    )

    cycle = 0
    while True:
        cycle += 1
        logger.info("--- Monitoring cycle %d ---", cycle)

        try:
            report = await agent.run_cycle()
            if report:
                logger.info(
                    "Cycle %d: incident detected (severity=%s, title='%s')",
                    cycle, report.severity.value, report.title,
                )
            else:
                logger.info("Cycle %d: all clear", cycle)
        except Exception as e:
            logger.error("Cycle %d failed: %s", cycle, e, exc_info=True)

        if single_cycle:
            break

        logger.info("Next cycle in %ds", interval_seconds)
        await asyncio.sleep(interval_seconds)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="StreamOps monitoring agent")
    parser.add_argument("--interval", type=int, default=None, help="Polling interval in seconds (default: from config)")
    parser.add_argument("--single-agent", action="store_true", help="Run in single-agent mode (no sub-agents)")
    parser.add_argument("--single-cycle", action="store_true", help="Run one cycle and exit")
    args = parser.parse_args()

    try:
        asyncio.run(run_loop(
            interval_seconds=args.interval,
            multi_agent=not args.single_agent,
            single_cycle=args.single_cycle,
        ))
    except KeyboardInterrupt:
        logger.info("Agent stopped by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
