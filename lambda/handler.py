"""
AWS Lambda handler for Kinesis Data Firehose processor.

This Lambda is invoked by Firehose as a data transformation function.
It receives CloudWatch Logs data (base64-encoded, gzip-compressed),
filters spans based on configurable attribute values, converts matching
spans to OTLP protobuf format, and exports them to the configured
OTLP endpoint.

Records that pass the filter are marked as "Ok" (for Firehose delivery to S3 backup).
Records that are filtered out are marked as "Dropped".
Records that fail processing are marked as "ProcessingFailed".

Conversion uses google.protobuf.json_format.ParseDict for idiomatic
protobuf serialization from the normalized OTLP JSON structure.
"""

import base64
import gzip
import json
import logging
import os
from typing import Any

import requests
from google.protobuf.json_format import ParseDict
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration from environment variables
OTLP_ENDPOINTS_JSON = os.environ.get("OTLP_ENDPOINTS", "")
OTLP_ENDPOINT = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "")
OTLP_HEADERS = os.environ.get("OTEL_EXPORTER_OTLP_HEADERS", "")
FILTER_ATTRIBUTES = os.environ.get("FILTER_SPAN_ATTRIBUTES", "")
# Dry run mode: logs input/output instead of sending to OTLP. Default is "true".
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() in ("true", "1", "yes")

# Firehose record result types
RESULT_OK = "Ok"
RESULT_DROPPED = "Dropped"
RESULT_FAILED = "ProcessingFailed"

# Span kind string-to-int mapping (OTLP proto uses integers)
_SPAN_KIND_MAP = {
    "INTERNAL": 1,
    "SERVER": 2,
    "CLIENT": 3,
    "PRODUCER": 4,
    "CONSUMER": 5,
    "SPAN_KIND_INTERNAL": 1,
    "SPAN_KIND_SERVER": 2,
    "SPAN_KIND_CLIENT": 3,
    "SPAN_KIND_PRODUCER": 4,
    "SPAN_KIND_CONSUMER": 5,
}

# Status code string-to-int mapping
_STATUS_CODE_MAP = {
    "UNSET": 0,
    "OK": 1,
    "ERROR": 2,
    "STATUS_CODE_UNSET": 0,
    "STATUS_CODE_OK": 1,
    "STATUS_CODE_ERROR": 2,
}


# ---------------------------------------------------------------------------
# Configuration parsing
# ---------------------------------------------------------------------------


def parse_otlp_headers(headers_str: str) -> dict[str, str]:
    """Parse OTEL_EXPORTER_OTLP_HEADERS format: key=value,key2=value2."""
    headers = {}
    if not headers_str:
        return headers
    for pair in headers_str.split(","):
        pair = pair.strip()
        if "=" in pair:
            key, value = pair.split("=", 1)
            headers[key.strip()] = value.strip()
    return headers


def parse_otlp_endpoints(
    endpoints_json: str, fallback_endpoint: str, fallback_headers: str
) -> list[dict[str, str]]:
    """
    Parse the OTLP_ENDPOINTS JSON config into a list of endpoint configs.

    Format: [{"endpoint": "https://...", "headers": "key=val,key2=val2"}, ...]

    Falls back to the single OTEL_EXPORTER_OTLP_ENDPOINT/HEADERS if the JSON
    is empty or not set.
    """
    if endpoints_json:
        try:
            endpoints = json.loads(endpoints_json)
            if isinstance(endpoints, list) and endpoints:
                return [
                    {
                        "endpoint": ep.get("endpoint", ""),
                        "headers": ep.get("headers", ""),
                    }
                    for ep in endpoints
                    if ep.get("endpoint")
                ]
        except (json.JSONDecodeError, TypeError):
            logger.warning("Failed to parse OTLP_ENDPOINTS JSON, falling back to single endpoint")

    if fallback_endpoint:
        return [{"endpoint": fallback_endpoint, "headers": fallback_headers}]
    return []


def parse_filter_attributes(filter_str: str) -> dict[str, list[str]]:
    """
    Parse FILTER_SPAN_ATTRIBUTES environment variable.

    Format: key1=value1|value2,key2=value3
    Values ending with '*' are treated as prefix matches.
    Special value "*" (alone) disables filtering — all spans pass through.
    If empty or "*", no filtering is applied (all spans pass through).
    """
    if not filter_str or filter_str.strip() == "*":
        return {}
    filters = {}
    for pair in filter_str.split(","):
        pair = pair.strip()
        if "=" in pair:
            key, values_str = pair.split("=", 1)
            filters[key.strip()] = [v.strip() for v in values_str.split("|")]
    return filters


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


def span_matches_filter(
    span_data: dict[str, Any], filters: dict[str, list[str]]
) -> bool:
    """
    Check if a span matches the configured filter criteria.

    A span matches if ALL filter keys are present in the span attributes
    and at least one of the allowed values matches per key.
    Values ending with '*' are prefix matches.
    """
    if not filters:
        return True

    attributes = _extract_flat_attributes(span_data.get("attributes", {}))

    for filter_key, allowed_values in filters.items():
        attr_value = attributes.get(filter_key)
        if attr_value is None:
            return False
        if not _value_matches(attr_value, allowed_values):
            return False

    return True


def _value_matches(attr_value: str, allowed_values: list[str]) -> bool:
    """Check if an attribute value matches any allowed value (prefix or exact)."""
    for allowed in allowed_values:
        if allowed.endswith("*"):
            if attr_value.startswith(allowed[:-1]):
                return True
        elif attr_value == allowed:
            return True
    return False


def _extract_flat_attributes(
    attrs: dict[str, Any] | list[dict[str, Any]],
) -> dict[str, str]:
    """Extract a flat {key: string_value} dict from either format for filtering."""
    result: dict[str, str] = {}
    if isinstance(attrs, dict):
        for key, value in attrs.items():
            result[key] = str(value)
    elif isinstance(attrs, list):
        for attr in attrs:
            key = attr.get("key", "")
            value = attr.get("value", {})
            if isinstance(value, dict):
                for vtype in ("stringValue", "intValue", "boolValue", "doubleValue"):
                    if vtype in value:
                        result[key] = str(value[vtype])
                        break
            else:
                result[key] = str(value)
    return result


# ---------------------------------------------------------------------------
# CW Logs JSON → OTLP JSON normalization
# ---------------------------------------------------------------------------


def _normalize_any_value(value: Any) -> dict[str, Any]:
    """Convert a Python value to OTLP AnyValue JSON structure."""
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, list):
        return {"arrayValue": {"values": [_normalize_any_value(v) for v in value]}}
    if isinstance(value, dict):
        # Already in OTLP format (has stringValue/intValue etc.)
        if any(k in value for k in ("stringValue", "intValue", "doubleValue",
                                     "boolValue", "arrayValue", "kvlistValue",
                                     "bytesValue")):
            return value
        # Nested dict → kvlistValue
        return {
            "kvlistValue": {
                "values": [
                    {"key": k, "value": _normalize_any_value(v)}
                    for k, v in value.items()
                ]
            }
        }
    return {"stringValue": str(value)}


def _normalize_attributes(attrs: Any) -> list[dict[str, Any]]:
    """
    Convert attributes from flat dict or OTLP array to OTLP KeyValue array.

    Flat dict: {"key": value} → [{"key": "key", "value": {"stringValue": ...}}]
    OTLP array: already in correct format, pass through with value normalization.
    """
    if not attrs:
        return []

    if isinstance(attrs, dict):
        return [
            {"key": k, "value": _normalize_any_value(v)}
            for k, v in attrs.items()
        ]

    if isinstance(attrs, list):
        # Could be OTLP array format or raw list — check structure
        normalized = []
        for item in attrs:
            if isinstance(item, dict) and "key" in item:
                # Already OTLP KeyValue format
                entry = {"key": item["key"]}
                if "value" in item:
                    val = item["value"]
                    entry["value"] = (
                        val if isinstance(val, dict) and any(
                            k in val for k in ("stringValue", "intValue",
                                               "doubleValue", "boolValue",
                                               "arrayValue", "kvlistValue")
                        ) else _normalize_any_value(val)
                    )
                else:
                    entry["value"] = {"stringValue": ""}
                normalized.append(entry)
            else:
                # Raw list item, shouldn't happen for attributes
                normalized.append({"key": str(item), "value": {"stringValue": str(item)}})
        return normalized

    return []


def _normalize_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize span events to OTLP Event JSON structure."""
    normalized = []
    for event in events:
        otlp_event: dict[str, Any] = {}

        if event.get("timeUnixNano") is not None:
            otlp_event["timeUnixNano"] = str(int(event["timeUnixNano"]))

        if "name" in event:
            otlp_event["name"] = event["name"]

        if "attributes" in event:
            otlp_event["attributes"] = _normalize_attributes(event["attributes"])

        if "droppedAttributesCount" in event:
            otlp_event["droppedAttributesCount"] = event["droppedAttributesCount"]

        normalized.append(otlp_event)
    return normalized


def _normalize_links(links: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize span links to OTLP Link JSON structure."""
    normalized = []
    for link in links:
        otlp_link: dict[str, Any] = {}

        if "traceId" in link:
            otlp_link["traceId"] = link["traceId"]
        if "spanId" in link:
            otlp_link["spanId"] = link["spanId"]
        if "traceState" in link:
            otlp_link["traceState"] = link["traceState"]
        if "attributes" in link:
            otlp_link["attributes"] = _normalize_attributes(link["attributes"])

        normalized.append(otlp_link)
    return normalized


def _hex_to_base64(hex_str: str) -> str:
    """Convert a hex-encoded ID to base64 (required by protobuf JSON encoding)."""
    return base64.b64encode(bytes.fromhex(hex_str)).decode()


def normalize_span_to_otlp_json(span_data: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize a CW Logs span dict into OTLP JSON Span structure.

    The result is compatible with ParseDict(span_dict, Span()) using the
    OTLP protobuf JSON encoding conventions.
    """
    otlp_span: dict[str, Any] = {}

    # IDs: CW Logs uses hex, protobuf JSON encoding requires base64
    if "traceId" in span_data:
        otlp_span["traceId"] = _hex_to_base64(span_data["traceId"])
    if "spanId" in span_data:
        otlp_span["spanId"] = _hex_to_base64(span_data["spanId"])
    if "parentSpanId" in span_data:
        otlp_span["parentSpanId"] = _hex_to_base64(span_data["parentSpanId"])
    if "traceState" in span_data:
        otlp_span["traceState"] = span_data["traceState"]

    if "name" in span_data:
        otlp_span["name"] = span_data["name"]

    # Span kind: normalize string to int
    if "kind" in span_data:
        kind = span_data["kind"]
        if isinstance(kind, str):
            otlp_span["kind"] = _SPAN_KIND_MAP.get(kind, 0)
        else:
            otlp_span["kind"] = kind

    # Timestamps as strings (OTLP JSON uses fixed64 as string)
    if span_data.get("startTimeUnixNano") is not None:
        otlp_span["startTimeUnixNano"] = str(int(span_data["startTimeUnixNano"]))
    if span_data.get("endTimeUnixNano") is not None:
        otlp_span["endTimeUnixNano"] = str(int(span_data["endTimeUnixNano"]))

    # Attributes
    if "attributes" in span_data:
        otlp_span["attributes"] = _normalize_attributes(span_data["attributes"])

    # Events
    if "events" in span_data and span_data["events"]:
        otlp_span["events"] = _normalize_events(span_data["events"])

    # Links
    if "links" in span_data and span_data["links"]:
        otlp_span["links"] = _normalize_links(span_data["links"])

    # Status
    if "status" in span_data:
        status = span_data["status"]
        otlp_status: dict[str, Any] = {}
        if "code" in status:
            code = status["code"]
            if isinstance(code, str):
                otlp_status["code"] = _STATUS_CODE_MAP.get(code, 0)
            else:
                otlp_status["code"] = code
        if "message" in status:
            otlp_status["message"] = status["message"]
        otlp_span["status"] = otlp_status

    # Flags
    if "flags" in span_data:
        otlp_span["flags"] = span_data["flags"]

    return otlp_span


# ---------------------------------------------------------------------------
# Build ExportTraceServiceRequest using ParseDict
# ---------------------------------------------------------------------------


def build_export_request(
    spans_data: list[dict[str, Any]],
) -> ExportTraceServiceRequest:
    """
    Build an ExportTraceServiceRequest from a list of span JSON objects.

    Groups spans by resource and scope, normalizes to OTLP JSON structure,
    then uses ParseDict for idiomatic protobuf conversion.
    """
    # Group spans by resource → scope
    resource_spans_map: dict[str, dict[str, Any]] = {}

    for span_data in spans_data:
        resource_data = span_data.get("resource", {})
        resource_key = json.dumps(
            resource_data.get("attributes", {}), sort_keys=True
        )

        if resource_key not in resource_spans_map:
            resource_spans_map[resource_key] = {
                "resource": resource_data,
                "scope_spans": {},
            }

        scope_data = span_data.get("instrumentationScope", {})
        scope_key = json.dumps(scope_data, sort_keys=True)

        if scope_key not in resource_spans_map[resource_key]["scope_spans"]:
            resource_spans_map[resource_key]["scope_spans"][scope_key] = {
                "scope": scope_data,
                "spans": [],
            }

        resource_spans_map[resource_key]["scope_spans"][scope_key][
            "spans"
        ].append(span_data)

    # Build the OTLP JSON structure
    resource_spans_list = []
    for entry in resource_spans_map.values():
        resource_attrs = _normalize_attributes(
            entry["resource"].get("attributes", {})
        )
        scope_spans_list = []
        for scope_entry in entry["scope_spans"].values():
            scope_data = scope_entry["scope"]
            spans_json = [
                normalize_span_to_otlp_json(s) for s in scope_entry["spans"]
            ]
            scope_span = {"spans": spans_json}
            if scope_data:
                scope_span["scope"] = {}
                if "name" in scope_data:
                    scope_span["scope"]["name"] = scope_data["name"]
                if "version" in scope_data:
                    scope_span["scope"]["version"] = scope_data["version"]
            scope_spans_list.append(scope_span)

        resource_spans_list.append({
            "resource": {"attributes": resource_attrs},
            "scopeSpans": scope_spans_list,
        })

    # Use ParseDict for idiomatic protobuf conversion
    request_dict = {"resourceSpans": resource_spans_list}
    return ParseDict(request_dict, ExportTraceServiceRequest())


# ---------------------------------------------------------------------------
# OTLP Export
# ---------------------------------------------------------------------------


def _send_to_endpoint(
    endpoint_config: dict[str, str],
    payload: bytes,
    resource_span_count: int,
) -> bool:
    """Send serialized OTLP payload to a single endpoint. Returns True on success."""
    endpoint = endpoint_config["endpoint"].rstrip("/")
    if not endpoint.endswith("/v1/traces"):
        endpoint = f"{endpoint}/v1/traces"

    headers = {"Content-Type": "application/x-protobuf"}
    headers.update(parse_otlp_headers(endpoint_config.get("headers", "")))

    try:
        response = requests.post(endpoint, data=payload, headers=headers, timeout=30)
        if response.status_code in (200, 202):
            logger.info(
                "Exported %d resource spans (%d bytes) to %s",
                resource_span_count,
                len(payload),
                endpoint,
            )
            return True
        logger.error(
            "OTLP export failed for %s: status=%d body=%s",
            endpoint,
            response.status_code,
            response.text[:500],
        )
        return False
    except requests.exceptions.RequestException as e:
        logger.error("OTLP export request failed for %s: %s", endpoint, str(e))
        return False


def export_to_otlp(
    request: ExportTraceServiceRequest,
    raw_spans: list[dict[str, Any]],
) -> bool:
    """
    Export traces to all configured OTLP endpoints using HTTP/protobuf.

    In dry-run mode, logs the incoming spans and the serialized request
    instead of making the HTTP calls.

    Returns True if at least one endpoint succeeds (or in dry-run mode).
    """
    endpoints = parse_otlp_endpoints(OTLP_ENDPOINTS_JSON, OTLP_ENDPOINT, OTLP_HEADERS)

    if DRY_RUN:
        logger.info("[DRY RUN] Incoming spans (%d):", len(raw_spans))
        for i, span in enumerate(raw_spans):
            logger.info("[DRY RUN] Span %d: %s", i, json.dumps(span))
        logger.info(
            "[DRY RUN] OTLP request (%d resource_spans, %d bytes)",
            len(request.resource_spans),
            len(request.SerializeToString()),
        )
        endpoint_list = [ep["endpoint"] for ep in endpoints] if endpoints else ["(not configured)"]
        logger.info("[DRY RUN] Would send to: %s", ", ".join(endpoint_list))
        return True

    if not endpoints:
        logger.error("No OTLP endpoints configured (set OTLP_ENDPOINTS or OTEL_EXPORTER_OTLP_ENDPOINT)")
        return False

    payload = request.SerializeToString()
    resource_span_count = len(request.resource_spans)

    any_success = False
    for ep in endpoints:
        if _send_to_endpoint(ep, payload, resource_span_count):
            any_success = True

    return any_success


# ---------------------------------------------------------------------------
# Firehose record decoding
# ---------------------------------------------------------------------------


def decode_firehose_record(data: str) -> dict[str, Any]:
    """Decode base64 + gzip compressed CW Logs subscription filter data."""
    compressed = base64.b64decode(data)
    decompressed = gzip.decompress(compressed)
    return json.loads(decompressed)


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    AWS Lambda handler for Kinesis Data Firehose transformation.

    1. Decodes and decompresses CW Logs data
    2. Filters spans based on configured attribute values
    3. Converts matching spans to OTLP protobuf (via ParseDict)
    4. Exports to the configured OTLP endpoint (or logs in dry-run mode)
    5. Returns Firehose-compatible response with record statuses
    """
    filters = parse_filter_attributes(FILTER_ATTRIBUTES)
    output_records = []
    spans_to_export: list[dict[str, Any]] = []

    for record in event.get("records", []):
        record_id = record["recordId"]
        try:
            cw_data = decode_firehose_record(record["data"])

            if DRY_RUN:
                logger.info(
                    "[DRY RUN] Incoming record %s: %s",
                    record_id,
                    json.dumps(cw_data),
                )

            message_type = cw_data.get("messageType")
            if message_type == "CONTROL_MESSAGE":
                output_records.append({
                    "recordId": record_id,
                    "result": RESULT_DROPPED,
                    "data": record["data"],
                })
                continue

            record_has_matching_spans = False

            for log_event in cw_data.get("logEvents", []):
                try:
                    span_data = json.loads(log_event.get("message", ""))
                except (json.JSONDecodeError, TypeError):
                    logger.warning("Skipping non-JSON log event")
                    continue

                spans = _extract_spans(span_data)

                for span in spans:
                    if span_matches_filter(span, filters):
                        spans_to_export.append(span)
                        record_has_matching_spans = True

            result = RESULT_OK if record_has_matching_spans else RESULT_DROPPED
            output_records.append({
                "recordId": record_id,
                "result": result,
                "data": record["data"],
            })

        except Exception as e:
            logger.error("Error processing record %s: %s", record_id, str(e))
            output_records.append({
                "recordId": record_id,
                "result": RESULT_FAILED,
                "data": record["data"],
            })

    # Export collected spans
    if spans_to_export:
        label = "[DRY RUN] " if DRY_RUN else ""
        logger.info("%sExporting %d spans", label, len(spans_to_export))
        request = build_export_request(spans_to_export)
        if not export_to_otlp(request, raw_spans=spans_to_export):
            logger.error("Failed to export spans — marking records as failed")
            for rec in output_records:
                if rec["result"] == RESULT_OK:
                    rec["result"] = RESULT_FAILED
    else:
        logger.info("No spans matched the filter criteria")

    return {"records": output_records}


def _extract_spans(span_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract individual spans from either batch or single-span format."""
    spans: list[dict[str, Any]] = []

    if "resourceSpans" in span_data:
        # OTLP batch format
        for rs in span_data["resourceSpans"]:
            resource = rs.get("resource", {})
            for ss in rs.get("scopeSpans", []):
                scope = ss.get("scope", {})
                for span in ss.get("spans", []):
                    span["resource"] = resource
                    span["instrumentationScope"] = scope
                    spans.append(span)
    elif "traceId" in span_data:
        # Single span (flat CW Logs format)
        if "scope" in span_data and "instrumentationScope" not in span_data:
            span_data["instrumentationScope"] = span_data["scope"]
        spans.append(span_data)

    return spans
