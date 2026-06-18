"""Event retrieval and log search tools.

These tools consume directly from Kafka to give the agent access to recent
raw events and the ability to search log messages by content. This is the
"read the actual data" capability vs. the metrics tools which show aggregates.
"""

import json
import logging
from typing import Optional

from confluent_kafka import Consumer, TopicPartition
from mcp.server.fastmcp import FastMCP

from streamops_mcp.config import config

logger = logging.getLogger("streamops-mcp.events")


def _create_consumer(group_suffix: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": config.kafka_bootstrap,
        "group.id": f"{config.kafka_mcp_group}-{group_suffix}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })


def _deserialize_event(raw: bytes) -> Optional[dict]:
    """Deserialize a Protobuf StreamEvent from raw Kafka bytes."""
    try:
        from streamops_mcp._proto_helper import deserialize_stream_event
        return deserialize_stream_event(raw)
    except ImportError:
        logger.debug("Protobuf helper not available, attempting JSON fallback")
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None


def register_event_tools(mcp: FastMCP):

    @mcp.tool()
    async def get_recent_events(
        count: int = config.events_default_count,
        topic: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> dict:
        """Retrieve the N most recent events from a Kafka topic.

        Args:
            count: Number of events to retrieve (max configurable)
            topic: Topic to read from (defaults to stream-events)
            event_type: Filter by payload type: 'metric', 'log', 'alert', 'heartbeat'

        Returns raw event data so the agent can inspect actual payloads,
        not just aggregated statistics.
        """
        target_topic = topic or config.kafka_events_topic
        count = min(count, config.events_max_count)
        logger.info("Fetching %d recent events from '%s' (type=%s)", count, target_topic, event_type)

        try:
            consumer = _create_consumer("recent")
            metadata = consumer.list_topics(target_topic, timeout=config.kafka_timeout)
            topic_meta = metadata.topics.get(target_topic)

            if topic_meta is None:
                consumer.close()
                return {"error": f"Topic '{target_topic}' not found"}

            partition_count = len(topic_meta.partitions)

            assignments = []
            for pid in range(partition_count):
                tp = TopicPartition(target_topic, pid)
                _, high = consumer.get_watermark_offsets(tp, timeout=config.kafka_timeout)
                per_partition = max(0, count // partition_count + 1)
                start = max(0, high - per_partition)
                assignments.append(TopicPartition(target_topic, pid, start))

            consumer.assign(assignments)

            events = []
            empty_polls = 0
            while len(events) < count and empty_polls < config.events_empty_poll_threshold:
                msg = consumer.poll(timeout=config.events_poll_timeout)
                if msg is None:
                    empty_polls += 1
                    continue
                if msg.error():
                    logger.warning("Kafka poll error: %s", msg.error())
                    continue

                event = _deserialize_event(msg.value())
                if event is None:
                    continue

                if event_type and not _matches_type(event, event_type):
                    continue

                events.append({
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp": msg.timestamp()[1],
                    "event": event,
                })

            consumer.close()

            logger.info("Retrieved %d events from '%s'", len(events), target_topic)
            return {
                "topic": target_topic,
                "count": len(events),
                "events": events[-count:],
            }
        except Exception as e:
            logger.error("Event retrieval failed: %s", e)
            return {"error": str(e)}

    @mcp.tool()
    async def search_logs(
        pattern: str,
        count: int = 20,
        severity: Optional[str] = None,
        component: Optional[str] = None,
    ) -> dict:
        """Search log events by message content.

        Args:
            pattern: Substring to search for in log messages (case-insensitive)
            count: Max results to return (max 100)
            severity: Filter by severity: 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'
            component: Filter by component name

        Scans recent events on the stream-events topic for LogEvent payloads
        matching the criteria. Useful for finding specific error messages or
        tracing a sequence of events around an incident.
        """
        count = min(count, config.events_max_count)
        pattern_lower = pattern.lower()
        logger.info("Searching logs: pattern='%s', severity=%s, component=%s",
                     pattern, severity, component)

        try:
            consumer = _create_consumer("search")
            metadata = consumer.list_topics(config.kafka_events_topic, timeout=config.kafka_timeout)
            topic_meta = metadata.topics.get(config.kafka_events_topic)

            if topic_meta is None:
                consumer.close()
                return {"error": f"Topic '{config.kafka_events_topic}' not found"}

            scan_depth = config.events_log_scan_depth
            partition_count = len(topic_meta.partitions)

            assignments = []
            for pid in range(partition_count):
                tp = TopicPartition(config.kafka_events_topic, pid)
                _, high = consumer.get_watermark_offsets(tp, timeout=config.kafka_timeout)
                start = max(0, high - (scan_depth // partition_count))
                assignments.append(TopicPartition(config.kafka_events_topic, pid, start))

            consumer.assign(assignments)

            matches = []
            messages_scanned = 0
            empty_polls = 0

            while len(matches) < count and empty_polls < config.events_empty_poll_threshold and messages_scanned < scan_depth:
                msg = consumer.poll(timeout=config.events_poll_timeout)
                if msg is None:
                    empty_polls += 1
                    continue
                if msg.error():
                    continue

                messages_scanned += 1
                event = _deserialize_event(msg.value())
                if event is None:
                    continue

                log_data = _extract_log(event)
                if log_data is None:
                    continue

                if pattern_lower not in log_data.get("message", "").lower():
                    continue

                if severity and log_data.get("severity", "").upper() != severity.upper():
                    continue

                if component and log_data.get("component", "").lower() != component.lower():
                    continue

                matches.append({
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp": msg.timestamp()[1],
                    "log": log_data,
                })

            consumer.close()

            logger.info("Log search: scanned=%d, matched=%d", messages_scanned, len(matches))
            return {
                "pattern": pattern,
                "messages_scanned": messages_scanned,
                "match_count": len(matches),
                "matches": matches,
            }
        except Exception as e:
            logger.error("Log search failed: %s", e)
            return {"error": str(e)}


def _matches_type(event: dict, event_type: str) -> bool:
    return event_type.lower() in event


def _extract_log(event: dict) -> Optional[dict]:
    return event.get("log")
