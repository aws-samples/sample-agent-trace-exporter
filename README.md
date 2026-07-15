# ac-trace-exporter

Exports AgentCore trace spans from CloudWatch Logs (`aws/spans` log group) to an OTLP-compatible endpoint using HTTP/protobuf protocol.

## Architecture

```
CloudWatch Logs (aws/spans)
        │
        ▼  (Subscription Filter - all events)
Kinesis Data Firehose
        │
        ▼  (Data Transformation Lambda)
Lambda Processor
   ├── Decodes CW Logs records (base64 + gzip)
   ├── Filters spans by attribute values (prefix match supported)
   │   Default: aws.xray.origin starts with "AWS::BedrockAgentCore::"
   ├── Converts matching spans to OTLP protobuf
   └── DRY_RUN=true:  logs input + output (default)
       DRY_RUN=false: sends to OTLP endpoint via HTTP/protobuf
        │
        ▼  (Backup - records with matching spans)
S3 Bucket (30-day retention)
```

## Project Structure

```
ac-trace-exporter/
├── cdk/                    # CDK infrastructure code
│   ├── app.py              # CDK app entry point
│   ├── stacks/
│   │   └── trace_exporter_stack.py
│   ├── cdk.json
│   └── requirements.txt
├── lambda/                 # Lambda function code
│   ├── handler.py          # Firehose processor Lambda
│   └── requirements.txt
├── pyproject.toml
└── README.md
```

## Configuration

### Environment Variables (Lambda)

| Variable | Description | Example |
|----------|-------------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint URL | `https://otel-collector.example.com:4318` |
| `OTEL_EXPORTER_OTLP_HEADERS` | Headers for OTLP requests (comma-separated key=value) | `api-key=secret123,org-id=myorg` |
| `FILTER_SPAN_ATTRIBUTES` | Span attribute filters (comma-separated key=value\|value) | `aws.xray.origin=AWS::BedrockAgentCore::*` |
| `DRY_RUN` | When `true`, logs input/output instead of sending to OTLP (default: `true`) | `false` |

### CDK Context Values

Pass at deploy time via `--context` or in `cdk.json`:

| Key | Description | Default |
|-----|-------------|---------|
| `otlp_endpoint` | OTLP endpoint URL | (empty) |
| `otlp_headers` | OTLP headers string | (empty) |
| `filter_span_attributes` | Span filter expression | `aws.xray.origin=AWS::BedrockAgentCore::*` |
| `log_group_name` | CloudWatch log group to subscribe to | `aws/spans` |
| `dry_run` | Log instead of sending to OTLP | `true` |

## Deployment

### Prerequisites

- Python 3.12+
- AWS CDK CLI (`npm install -g aws-cdk`)
- AWS credentials configured
- Docker (optional — local bundling via pip/uv is attempted first)

### Install Dependencies

```bash
uv sync --all-extras --refresh --reinstall
```

### Deploy

```bash
cd cdk
uv run cdk deploy \
  --context otlp_endpoint="https://your-otel-collector:4318" \
  --context otlp_headers="Authorization=Bearer token123" \
  --context dry_run="true"
```

### Filtering Spans

The `FILTER_SPAN_ATTRIBUTES` / `filter_span_attributes` value uses this format:

```
key1=value1|value2,key2=value3
```

- Multiple attribute filters are comma-separated (AND logic between keys)
- Multiple allowed values per key are pipe-separated (OR logic between values)
- Values ending with `*` are treated as **prefix matches**
- If empty/unset, all spans pass through without filtering

**Default:** `aws.xray.origin=AWS::BedrockAgentCore::*` — passes only spans originating from BedrockAgentCore services.

Set to `*` to disable filtering and allow all traces through.

**Examples:**

```bash
# Default — only BedrockAgentCore spans
--context filter_span_attributes="aws.xray.origin=AWS::BedrockAgentCore::*"

# No filter — allow all traces
--context filter_span_attributes="*"

# Filter by service name
--context filter_span_attributes="service.name=my-agent"

# Filter by service name (multiple values)
--context filter_span_attributes="service.name=agent-a|agent-b"

# Filter by tenant ID (from W3C baggage propagation)
--context filter_span_attributes="tenant.id=tenant-123"
```

Example: `service.name=agent-a|agent-b,deployment.environment=production`

This passes only spans where `service.name` is "agent-a" OR "agent-b" AND `deployment.environment` is "production".

## How It Works

1. **Subscription Filter**: CloudWatch Logs subscription filter forwards all log events from `aws/spans` to Kinesis Data Firehose.

2. **Firehose Processing**: Firehose invokes the Lambda processor for data transformation. The Lambda:
   - Decodes the base64 + gzip compressed CW Logs data
   - Parses span JSON from log event messages (flat attribute format: `{"key": value}`)
   - Filters spans based on `FILTER_SPAN_ATTRIBUTES` (default: `aws.xray.origin=AWS::BedrockAgentCore::*`)
   - Converts matching spans to OTLP protobuf format
   - **Dry-run mode** (default): Logs incoming records and OTLP output to CloudWatch without sending
   - **Live mode** (`DRY_RUN=false`): Sends to the configured OTLP endpoint via HTTP POST (`/v1/traces`)

3. **Record Disposition**:
   - `Ok`: Record had matching spans (delivered to S3 backup)
   - `Dropped`: Record had no matching spans or was a control message (not delivered)
   - `ProcessingFailed`: Error during processing or OTLP export failure (retried by Firehose)

4. **Backup**: All `Ok` records are stored in S3 (gzipped, partitioned by date) with a 30-day retention.
