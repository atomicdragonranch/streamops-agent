"""Protobuf deserialization helper.

Attempts to import the generated Protobuf classes from the Java proto module.
Falls back gracefully if not available (e.g., when protobuf Python package
is installed but the generated classes haven't been compiled for Python yet).

In Phase 1, the .proto file generates Java classes via Maven. For the MCP
server, we use protobuf's Python runtime to parse the same wire format
directly, without needing the generated Python classes.
"""

import logging

logger = logging.getLogger("streamops-mcp.proto")


def deserialize_stream_event(raw: bytes) -> dict | None:
    """Deserialize a StreamEvent from Protobuf wire format.

    Uses the generic protobuf decoder since we don't have generated Python
    classes. The schema is simple enough that descriptor-less decoding works.
    """
    try:
        result = _decode_raw(raw)
        return result if result else None
    except Exception as e:
        logger.debug("Protobuf deserialization failed: %s", e)
        return None


def _decode_raw(data: bytes) -> dict:
    """Bare-bones protobuf wire format decoder.

    Returns a dict with field numbers as keys. Good enough for inspection;
    the MCP tools know which field numbers map to which payload types
    from the .proto definition.
    """
    from google.protobuf.internal.decoder import _DecodeVarint
    from google.protobuf.internal.wire_format import WIRETYPE_VARINT, WIRETYPE_LENGTH_DELIMITED, WIRETYPE_FIXED64

    result = {}
    pos = 0

    # Field number -> name mapping from stream_events.proto
    field_names = {
        1: "event_id", 2: "timestamp_ms", 3: "source",
        10: "metric", 11: "log", 12: "alert", 13: "heartbeat",
    }

    while pos < len(data):
        try:
            tag, new_pos = _DecodeVarint(data, pos)
            wire_type = tag & 0x7
            field_number = tag >> 3
            pos = new_pos

            field_name = field_names.get(field_number, f"field_{field_number}")

            if wire_type == WIRETYPE_VARINT:
                value, pos = _DecodeVarint(data, pos)
                result[field_name] = value
            elif wire_type == WIRETYPE_LENGTH_DELIMITED:
                length, pos = _DecodeVarint(data, pos)
                raw_value = data[pos:pos + length]
                pos += length
                try:
                    result[field_name] = raw_value.decode("utf-8")
                except UnicodeDecodeError:
                    if field_number >= 10:
                        result[field_name] = _decode_raw(raw_value)
                    else:
                        result[field_name] = raw_value.hex()
            elif wire_type == WIRETYPE_FIXED64:
                import struct
                result[field_name] = struct.unpack("<d", data[pos:pos + 8])[0]
                pos += 8
            else:
                break
        except Exception:
            break

    return result
