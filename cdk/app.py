#!/usr/bin/env python3
"""CDK application for the AC Trace Exporter stack.

Context values are sourced from environment variables, typically loaded
via: uv run --env-file .env cdk deploy
"""

import os
import sys

import aws_cdk as cdk

from stacks.trace_exporter_stack import TraceExporterStack

# Validate: either OTLP_ENDPOINTS (multi) or the single-endpoint pair must be set
if not os.environ.get("otlp_endpoints"):
    _missing = [
        var
        for var in ("otlp_endpoint", "otlp_headers")
        if not os.environ.get(var)
    ]
    if _missing:
        sys.exit(
            f"ERROR: Required environment variable(s) not set: {', '.join(_missing)}\n"
            "Set 'otlp_endpoints' (JSON array) for multiple endpoints, or set both\n"
            "'otlp_endpoint' and 'otlp_headers' for a single endpoint.\n"
            "Ensure your .env file contains them or pass via: "
            "uv run --env-file .env cdk deploy"
        )

app = cdk.App(
    context={
        "otlp_endpoints": os.environ.get("otlp_endpoints", ""),
        "otlp_endpoint": os.environ.get("otlp_endpoint", ""),
        "otlp_headers": os.environ.get("otlp_headers", ""),
        "filter_span_attributes": os.environ.get(
            "filter_span_attributes",
            "aws.xray.origin=AWS::BedrockAgentCore::*",
        ),
        "log_group_name": os.environ.get("log_group_name", "aws/spans"),
        # Needed when subscribing to an AgentCore runtime's own log group (the
        # unified span destination), because that group also carries runtime stdout
        # and otel-rt-logs records whose JSON has traceId "". Those get mistaken for
        # spans by the processor and exported as garbage with empty IDs.
        "subscription_filter_pattern": os.environ.get(
            "subscription_filter_pattern", ""
        ),
        "dry_run": os.environ.get("dry_run", "true"),
    }
)

TraceExporterStack(
    app,
    "AcTraceExporterStack",
    description="Exports AgentCore trace spans from CloudWatch to an OTLP endpoint via Firehose",
)

app.synth()
