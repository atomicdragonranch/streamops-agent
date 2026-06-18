"""Flink REST API tools.

The Flink JobManager exposes a REST API on port 8081 that provides job status,
checkpoint statistics, and exception history. These tools wrap that API so the
agent can inspect pipeline health without knowing the raw endpoints.
"""

import logging

import httpx
from mcp.server.fastmcp import FastMCP

from streamops_mcp.config import config

logger = logging.getLogger("streamops-mcp.flink")


async def _flink_get(path: str) -> dict:
    async with httpx.AsyncClient(base_url=config.flink_url, timeout=10.0) as client:
        response = await client.get(path)
        response.raise_for_status()
        return response.json()


def register_flink_tools(mcp: FastMCP):

    @mcp.tool()
    async def query_flink_jobs() -> dict:
        """List all Flink jobs with their current status.

        Returns job ID, name, state (RUNNING/FAILED/CANCELED/etc), start time,
        and duration for every job the cluster knows about. Use this as the
        first diagnostic step when investigating pipeline issues.
        """
        logger.info("Querying Flink jobs")
        try:
            data = await _flink_get("/jobs/overview")
            jobs = data.get("jobs", [])
            logger.info("Found %d Flink jobs", len(jobs))
            return {
                "jobs": [
                    {
                        "job_id": j.get("jid"),
                        "name": j.get("name"),
                        "state": j.get("state"),
                        "start_time": j.get("start-time"),
                        "duration_ms": j.get("duration"),
                        "tasks": j.get("tasks", {}),
                    }
                    for j in jobs
                ]
            }
        except httpx.ConnectError:
            logger.error("Cannot connect to Flink at %s", config.flink_url)
            return {"error": f"Cannot connect to Flink at {config.flink_url}"}
        except Exception as e:
            logger.error("Flink jobs query failed: %s", e)
            return {"error": str(e)}

    @mcp.tool()
    async def get_checkpoint_stats(job_id: str) -> dict:
        """Get checkpoint statistics for a specific Flink job.

        Returns the latest checkpoint details: duration, size, alignment
        duration, and counts of completed/failed/in-progress checkpoints.
        Slow or failing checkpoints often indicate state backend pressure
        or network issues between TaskManagers.
        """
        logger.info("Querying checkpoint stats for job %s", job_id)
        try:
            data = await _flink_get(f"/jobs/{job_id}/checkpoints")
            counts = data.get("counts", {})
            latest = data.get("latest", {})
            summary = data.get("summary", {})

            result = {
                "job_id": job_id,
                "counts": {
                    "completed": counts.get("completed", 0),
                    "failed": counts.get("failed", 0),
                    "in_progress": counts.get("in_progress", 0),
                    "restored": counts.get("restored", 0),
                    "total": counts.get("total", 0),
                },
            }

            if latest.get("completed"):
                cp = latest["completed"]
                result["latest_completed"] = {
                    "id": cp.get("id"),
                    "duration_ms": cp.get("duration"),
                    "size_bytes": cp.get("state_size"),
                    "trigger_timestamp": cp.get("trigger_timestamp"),
                }

            if latest.get("failed"):
                fail = latest["failed"]
                result["latest_failed"] = {
                    "id": fail.get("id"),
                    "failure_timestamp": fail.get("failure_timestamp"),
                    "failure_message": fail.get("failure_message"),
                }

            if summary.get("state_size"):
                result["size_summary"] = {
                    "min": summary["state_size"].get("min"),
                    "max": summary["state_size"].get("max"),
                    "avg": summary["state_size"].get("avg"),
                }

            logger.info("Checkpoint stats: %d completed, %d failed",
                        counts.get("completed", 0), counts.get("failed", 0))
            return result
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning("Job %s not found", job_id)
                return {"error": f"Job {job_id} not found"}
            raise
        except httpx.ConnectError:
            logger.error("Cannot connect to Flink at %s", config.flink_url)
            return {"error": f"Cannot connect to Flink at {config.flink_url}"}
        except Exception as e:
            logger.error("Checkpoint query failed for job %s: %s", job_id, e)
            return {"error": str(e)}

    @mcp.tool()
    async def get_flink_exceptions(job_id: str) -> dict:
        """Get recent exceptions for a Flink job.

        Returns the exception history including root cause, timestamp, and
        the TaskManager that threw it. Critical for diagnosing why a job
        failed or is restarting.
        """
        logger.info("Querying exceptions for job %s", job_id)
        try:
            data = await _flink_get(f"/jobs/{job_id}/exceptions")
            exceptions = data.get("all-exceptions", [])
            result = {
                "job_id": job_id,
                "root_exception": data.get("root-exception"),
                "timestamp": data.get("timestamp"),
                "exceptions": [
                    {
                        "exception": e.get("exception"),
                        "task": e.get("task"),
                        "location": e.get("location"),
                        "timestamp": e.get("timestamp"),
                    }
                    for e in exceptions[:10]
                ],
            }
            logger.info("Found %d exceptions for job %s", len(exceptions), job_id)
            return result
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return {"error": f"Job {job_id} not found"}
            raise
        except httpx.ConnectError:
            return {"error": f"Cannot connect to Flink at {config.flink_url}"}
        except Exception as e:
            logger.error("Exception query failed for job %s: %s", job_id, e)
            return {"error": str(e)}
