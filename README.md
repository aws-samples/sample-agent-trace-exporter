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

### Integration Architecture

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              TRACE DATA SOURCES                                        │
│                                                                                        │
│  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐                    │
│  │ Strands / CrewAI │   │  Custom Agent    │   │  AgentCore       │                    │
│  │  LangGraph       │   │  Frameworks      │   │  Services        │                    │
│  └────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘                    │
│           │                      │                      │                              │
│           │                      │                      │                              │
└───────────┼──────────────────────┼──────────────────────┼──────────────────────────────┘
            │                      │                      │
            │  Writes spans        │  Writes spans        │  Writes spans
            │                      │                      │
            ▼                      ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              AWS ACCOUNT (Your Account)                                 │
│                                                                                         │
│   ┌───────────────────────────────────────────┐                                         │
│   │   CloudWatch Logs (aws/spans log group)   │◀──── Both AgentCore services and        │
│   │                                           │      agent frameworks write telemetry   │
│   └─────────────────────┬─────────────────────┘      directly to this log group         │
│                         │  Subscription Filter                                          │
│                         ▼                                                               │
│   ┌───────────────────────────────────────────┐                                         │
│   │   Amazon Data Firehose                    │                                         │
│   │   (Buffering: 60s / 1MB)                  │                                         │
│   └──────────┬─────────────────────┬──────────┘                                         │
│              │                     │                                                    │
│              │ Data Transform      │ Backup (all matched records)                       │
│              ▼                     ▼                                                    │
│   ┌────────────────────┐   ┌────────────────┐                                           │
│   │  Lambda Processor  │   │  S3 Bucket     │                                           │
│   │  ─────────────────-│   │  (30-day TTL)  │                                           │
│   │  • Decode CW Logs  │   └────────────────┘                                           │
│   │  • Filter spans    │                                                                │
│   │  • Convert to OTLP │                                                                │
│   └─────────┬──────────┘                                                                │
│             │  HTTP/protobuf POST /v1/traces                                            │
│             ▼                                                                           │
└─────────────────────────────────────────────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                         THIRD-PARTY OBSERVABILITY PLATFORMS                             │
│                                                                                         │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                │
│   │   Datadog    │  │   Splunk     │  │  New Relic   │  │  Grafana /   │                │
│   │              │  │              │  │              │  │  Tempo       │                │
│   └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘                │
│                                                                                         │
│   Any OTLP-compatible collector or backend                                              │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Why Firehose? (vs Direct Lambda Subscription)

A CW Logs subscription filter can invoke Lambda directly without Firehose. This project uses Firehose for its built-in resilience features:

| | With Firehose (current) | Direct Lambda |
|---|---|---|
| Buffering/batching | Configurable (60s / 1MB) | CW Logs batches only |
| S3 backup | Built-in destination | Must write to S3 yourself |
| Retry on failure | Firehose retries with backoff + dead-letter to S3 | CW Logs retries (24h retention) |
| Cost | Firehose ingestion + Lambda | Lambda only |
| Complexity | More infra, record disposition logic | Simpler stack |
| Max record size | 6 MB per record batch | 256 KB per invocation payload |

If you don't need the S3 backup or Firehose retry semantics, a direct CW Logs → Lambda subscription is a simpler alternative.

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

- Python 3.13+
- AWS CDK CLI (`npm install -g aws-cdk`)
- AWS credentials configured
- Docker or [Finch](https://github.com/runfinch/finch) — required for bundling Lambda dependencies during `cdk deploy`. If using Finch, set `CDK_DOCKER="finch"` in your `cdk/.env` file.

### Install Dependencies

```bash
uv sync --all-extras --refresh --reinstall
```

### Deploy

1. Create your environment file from the sample:

```bash
cd cdk
cp .env.sample .env
```

2. Edit `cdk/.env` with your actual values:

```
otlp_endpoint="https://your-otel-collector:4318"
otlp_headers="Authorization=Bearer <your-token>"
dry_run="true"
```

3. Deploy the stack:

```bash
uv run --env-file .env cdk deploy
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


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

