#!/usr/bin/env python3
"""CDK application for the AC Trace Exporter stack."""

import aws_cdk as cdk

from stacks.trace_exporter_stack import TraceExporterStack

app = cdk.App()

TraceExporterStack(
    app,
    "AcTraceExporterStack",
    description="Exports AgentCore trace spans from CloudWatch to an OTLP endpoint via Firehose",
)

app.synth()
