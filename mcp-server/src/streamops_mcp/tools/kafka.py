"""Kafka observability tools.

These tools query Kafka's admin API for consumer group lag and topic metadata.
Consumer lag is the single most important metric for streaming health: if lag
is growing, the pipeline is falling behind and will eventually hit SLA violations.
"""

import logging

from confluent_kafka import Consumer, TopicPartition
from mcp.server.fastmcp import FastMCP

from streamops_mcp.config import config

logger = logging.getLogger("streamops-mcp.kafka")


def register_kafka_tools(mcp: FastMCP):

    @mcp.tool()
    async def get_consumer_lag(group_id: str | None = None) -> dict:
        """Get consumer group lag across all partitions.

        Lag = latest offset - committed offset. Growing lag means the consumer
        (Flink job) is falling behind the producer (event simulator). The agent
        uses this to determine if a throughput issue is on the consumer side.

        If group_id is not specified, defaults to the Flink processor's group.
        """
        target_group = group_id or config.kafka_processor_group
        logger.info("Querying consumer lag for group '%s'", target_group)

        try:
            consumer = Consumer({
                "bootstrap.servers": config.kafka_bootstrap,
                "group.id": target_group,
                "enable.auto.commit": False,
            })

            metadata = consumer.list_topics(config.kafka_events_topic, timeout=config.kafka_timeout)
            topic_meta = metadata.topics.get(config.kafka_events_topic)
            partition_count = len(topic_meta.partitions) if topic_meta else 0

            committed = consumer.committed(
                [TopicPartition(config.kafka_events_topic, p) for p in range(partition_count)],
                timeout=config.kafka_timeout,
            )

            total_lag = 0
            partitions = []

            for tp in committed:
                low, high = consumer.get_watermark_offsets(tp, timeout=config.kafka_timeout)
                committed_offset = tp.offset if tp.offset >= 0 else 0
                lag = max(0, high - committed_offset)
                total_lag += lag
                partitions.append({
                    "partition": tp.partition,
                    "committed_offset": committed_offset,
                    "latest_offset": high,
                    "lag": lag,
                })

            consumer.close()

            logger.info("Consumer lag for '%s': total=%d across %d partitions",
                        target_group, total_lag, len(partitions))
            return {
                "group_id": target_group,
                "topic": config.kafka_events_topic,
                "total_lag": total_lag,
                "partitions": partitions,
            }
        except Exception as e:
            logger.error("Consumer lag query failed for group '%s': %s", target_group, e)
            return {"error": str(e)}

    @mcp.tool()
    async def get_topic_throughput(topic: str | None = None, seconds: int = 60) -> dict:
        """Estimate topic throughput by sampling latest offsets.

        Reads high watermark offsets for all partitions and estimates
        messages/second over the given time window. Useful for confirming
        whether the event simulator is producing at expected rates.
        """
        target_topic = topic or config.kafka_events_topic
        logger.info("Estimating throughput for topic '%s' over %ds", target_topic, seconds)

        try:
            consumer = Consumer({
                "bootstrap.servers": config.kafka_bootstrap,
                "group.id": f"{config.kafka_mcp_group}-throughput",
                "enable.auto.commit": False,
            })

            metadata = consumer.list_topics(target_topic, timeout=config.kafka_timeout)
            topic_meta = metadata.topics.get(target_topic)

            if topic_meta is None or topic_meta.error is not None:
                consumer.close()
                return {"error": f"Topic '{target_topic}' not found or inaccessible"}

            partition_count = len(topic_meta.partitions)
            total_messages = 0
            partitions = []

            for pid in range(partition_count):
                tp = TopicPartition(target_topic, pid)
                low, high = consumer.get_watermark_offsets(tp, timeout=config.kafka_timeout)
                count = high - low
                total_messages += count
                partitions.append({
                    "partition": pid,
                    "low_watermark": low,
                    "high_watermark": high,
                    "message_count": count,
                })

            consumer.close()

            estimated_rate = total_messages / max(1, seconds) if total_messages > 0 else 0

            logger.info("Topic '%s': %d total messages, ~%.1f msgs/s estimate",
                        target_topic, total_messages, estimated_rate)
            return {
                "topic": target_topic,
                "partition_count": partition_count,
                "total_messages": total_messages,
                "estimated_rate_per_second": round(estimated_rate, 2),
                "sample_window_seconds": seconds,
                "partitions": partitions,
            }
        except Exception as e:
            logger.error("Throughput query failed for topic '%s': %s", target_topic, e)
            return {"error": str(e)}
