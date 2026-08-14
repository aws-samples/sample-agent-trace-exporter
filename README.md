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
| `OTLP_ENDPOINTS` | JSON array of endpoint configs (multiple endpoints) | `[{"endpoint": "https://...", "headers": "key=val"}]` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Single OTLP endpoint URL (fallback if `OTLP_ENDPOINTS` is empty) | `https://otel-collector.example.com:4318` |
| `OTEL_EXPORTER_OTLP_HEADERS` | Headers for single endpoint (comma-separated key=value) | `api-key=secret123,org-id=myorg` |
| `FILTER_SPAN_ATTRIBUTES` | Span attribute filters (comma-separated key=value\|value) | `aws.xray.origin=AWS::BedrockAgentCore::*` |
| `DRY_RUN` | When `true`, logs input/output instead of sending to OTLP (default: `true`) | `false` |

### CDK Context Values

Pass at deploy time via `--context` or in `cdk.json`:

| Key | Description | Default |
|-----|-------------|---------|
| `otlp_endpoints` | JSON array of OTLP endpoint configs | (empty) |
| `otlp_endpoint` | Single OTLP endpoint URL (fallback) | (empty) |
| `otlp_headers` | OTLP headers string (fallback) | (empty) |
| `filter_span_attributes` | Span filter expression | `aws.xray.origin=AWS::BedrockAgentCore::*` |
| `log_group_name` | CloudWatch log group to subscribe to | `aws/spans` |
| `subscription_filter_pattern` | CloudWatch Logs subscription filter pattern | (empty — all events) |
| `dry_run` | Log instead of sending to OTLP | `true` |

### AgentCore runtimes that use the unified span destination

AgentCore runtime can deliver an agent's spans to the **agent's own** log group —
`/aws/bedrock-agentcore/runtimes/<agent_id>-<endpoint_name>`, log stream `spans` — instead
of the shared `aws/spans` group. This is the default for newly created agents in supported
regions, and it requires `aws-opentelemetry-distro >= 0.18.0` (earlier versions ignore the
setting and deliver to `aws/spans`). See
[Span destination for agents hosted in Amazon Bedrock AgentCore runtime](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/observability-configure.html#observability-configure-unified-traces).

To export from such an agent, point `log_group_name` at that log group:

```bash
log_group_name="/aws/bedrock-agentcore/runtimes/my_agent-AbC123XyZ-DEFAULT"
subscription_filter_pattern='{ $.traceId != "" }'
```

`subscription_filter_pattern` matters here. Unlike the dedicated `aws/spans` group, an
agent's own log group **mixes spans with log records** — runtime stdout and `otel-rt-logs`.
Those records are JSON and carry a `traceId` field, so without a pattern they are forwarded
to the processor alongside real spans. The processor now rejects them (see below), but
filtering at the subscription is cheaper: it avoids paying Firehose and Lambda for records
that will be discarded.

Leave `subscription_filter_pattern` empty when subscribing to `aws/spans`, which contains
only spans.

### Configuring Multiple OTLP Endpoints

The Lambda can fan out traces to multiple OTLP-compatible backends simultaneously. Set the `otlp_endpoints` environment variable (via `cdk/.env`) to a JSON array where each entry specifies an endpoint URL and its authentication headers:

```bash
otlp_endpoints='[{"endpoint": "https://your-collector:4318", "headers": "Authorization=Bearer <token>"}]'
```

Each entry in the array:

| Field | Description |
|-------|-------------|
| `endpoint` | The OTLP HTTP endpoint URL. `/v1/traces` is appended automatically if not present. |
| `headers` | Comma-separated `key=value` pairs sent as HTTP headers with the request. |

**Behavior:**
- The Lambda serializes the protobuf payload once and sends it to every configured endpoint.
- Export is considered successful if **at least one** endpoint responds with 200/202.
- Failures for individual endpoints are logged but do not block delivery to other endpoints.

**Fallback:** If `otlp_endpoints` is empty or unset, the Lambda falls back to the single-endpoint configuration (`OTEL_EXPORTER_OTLP_ENDPOINT` + `OTEL_EXPORTER_OTLP_HEADERS`).


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

```bash
# Option A: Multiple endpoints (preferred)
otlp_endpoints='[{"endpoint": "https://your-collector:4318", "headers": "Authorization=Bearer <token>"}]'

# Option B: Single endpoint (legacy)
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
   - **Discards records that are not spans** (see below)
   - Filters spans based on `FILTER_SPAN_ATTRIBUTES` (default: `aws.xray.origin=AWS::BedrockAgentCore::*`)
   - Converts matching spans to OTLP protobuf format
   - **Dry-run mode** (default): Logs incoming records and OTLP output to CloudWatch without sending
   - **Live mode** (`DRY_RUN=false`): Sends to the configured OTLP endpoint via HTTP POST (`/v1/traces`)

3. **Record Disposition**:
   - `Ok`: Record had matching spans (delivered to S3 backup)
   - `Dropped`: Record had no matching spans or was a control message (not delivered)
   - `ProcessingFailed`: Error during processing or OTLP export failure (retried by Firehose)

4. **Backup**: All `Ok` records are stored in S3 (gzipped, partitioned by date) with a 30-day retention.

### Distinguishing spans from log records

A log group that holds both spans and log records needs care, because log records are also
JSON with a `traceId` field. Treating one as a span fails in two distinct ways:

- A log record emitted **outside** any span has `traceId: ""`. Serialised as a span it
  produces zero-length ids, and OTLP backends reject the *entire*
  `ExportTraceServiceRequest` — e.g. Tempo answers
  `InvalidArgument: trace ids must be 128 bit, received 0 bits`. Because the rejection is
  per-request, one such record discards every real span batched with it.
- A log record emitted **inside** a span carries that span's real `traceId` *and* `spanId`,
  so validating id lengths alone still admits it. It then becomes a nameless, parentless
  span that collides with the real span of the same id, and the trace renders with
  duplicated subtrees.

`_is_span_record()` therefore requires both well-formed ids (32-hex trace, 16-hex span) and
span shape (a `name` and a `startTimeUnixNano`). Log records have neither. Skipped records
are counted in the Lambda log as `Skipped N non-span record(s)`.

Run the tests with:

```bash
uv sync --all-extras
uv run pytest tests/ -q
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

