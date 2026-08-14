"""Regression tests for distinguishing spans from log records.

These cover the two failure modes that appear when the subscribed log group is an
AgentCore runtime's own log group (the "unified span destination") rather than the
shared ``aws/spans`` group. That group mixes spans with log records, and both bugs
were silent: the trace simply never appeared, or appeared with duplicated subtrees.
"""

import importlib.util
import pathlib

import pytest

# The lambda directory cannot be imported normally: "lambda" is a Python keyword,
# so `import lambda.handler` is a syntax error. Load it by path instead.
_HANDLER_PATH = pathlib.Path(__file__).resolve().parent.parent / "lambda" / "handler.py"

pytest.importorskip("requests", reason="install the 'lambda' extra to run these")
pytest.importorskip("google.protobuf", reason="install the 'lambda' extra to run these")
pytest.importorskip(
    "opentelemetry.proto", reason="install the 'lambda' extra to run these"
)

_spec = importlib.util.spec_from_file_location("ac_handler", _HANDLER_PATH)
handler = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(handler)


TRACE_ID = "6a7f15733647d9e56083989538fae445"  # 32 hex chars
SPAN_ID = "bbfd7c5844629acb"  # 16 hex chars


def _span(**over):
    span = {
        "traceId": TRACE_ID,
        "spanId": SPAN_ID,
        "name": "invoke_agent Strands Agents",
        "kind": "SERVER",
        "startTimeUnixNano": 1786711218000000000,
        "endTimeUnixNano": 1786711221000000000,
        "attributes": {"aws.local.service": "my-agent"},
        "resource": {"attributes": {"service.name": "my-agent"}},
    }
    span.update(over)
    return span


class TestIsSpanRecord:
    def test_accepts_a_real_span(self):
        assert handler._is_span_record(_span()) is True

    def test_rejects_log_record_emitted_outside_a_span(self):
        """traceId "" would serialise to zero-length ids.

        The backend then rejects the whole ExportTraceServiceRequest with
        "trace ids must be 128 bit, received 0 bits", so one such record destroys
        every real span batched alongside it.
        """
        record = {
            "traceId": "",
            "spanId": "",
            "body": "Skipping installation of LoggingHandler",
            "severityText": "WARN",
            "timeUnixNano": 1786711848837293056,
        }
        assert handler._is_span_record(record) is False

    def test_rejects_log_record_emitted_inside_a_span(self):
        """The hard case: it carries the enclosing span's real traceId AND spanId.

        An id-length check alone lets it through, after which it becomes a nameless,
        parentless span that collides with the real span of the same id and renders
        as a duplicated subtree.
        """
        record = {
            "traceId": TRACE_ID,
            "spanId": SPAN_ID,
            "body": "some application log line",
            "severityText": "INFO",
            "timeUnixNano": 1786711848837293056,
        }
        assert handler._is_span_record(record) is False

    @pytest.mark.parametrize(
        "over",
        [
            {"traceId": "abc"},  # too short
            {"spanId": "abc"},  # too short
            {"name": ""},  # nameless
            {"startTimeUnixNano": None},  # no start time
        ],
        ids=["short-trace-id", "short-span-id", "empty-name", "no-start-time"],
    )
    def test_rejects_malformed(self, over):
        assert handler._is_span_record(_span(**over)) is False

    def test_missing_keys_do_not_raise(self):
        assert handler._is_span_record({}) is False


class TestExtractSpans:
    def test_flat_span_survives(self):
        assert len(handler._extract_spans(_span())) == 1

    def test_log_record_is_dropped(self):
        record = {"traceId": TRACE_ID, "spanId": SPAN_ID, "body": "log", "severityText": "INFO"}
        assert handler._extract_spans(record) == []

    def test_otlp_batch_format_still_works(self):
        batch = {
            "resourceSpans": [
                {
                    "resource": {"attributes": {"service.name": "my-agent"}},
                    "scopeSpans": [
                        {
                            "scope": {"name": "strands"},
                            "spans": [
                                {
                                    "traceId": TRACE_ID,
                                    "spanId": SPAN_ID,
                                    "name": "chat",
                                    "startTimeUnixNano": 1,
                                    "endTimeUnixNano": 2,
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        spans = handler._extract_spans(batch)
        assert len(spans) == 1
        assert spans[0]["name"] == "chat"
        # resource/scope are attached so build_export_request can group correctly
        assert spans[0]["resource"]["attributes"]["service.name"] == "my-agent"
        assert spans[0]["instrumentationScope"]["name"] == "strands"

    def test_batch_with_one_bad_span_keeps_the_good_one(self):
        """The whole point: one junk record must not take the batch down with it."""
        batch = {
            "resourceSpans": [
                {
                    "resource": {"attributes": {"service.name": "my-agent"}},
                    "scopeSpans": [
                        {
                            "scope": {},
                            "spans": [
                                {
                                    "traceId": TRACE_ID,
                                    "spanId": SPAN_ID,
                                    "name": "good",
                                    "startTimeUnixNano": 1,
                                },
                                {"traceId": "", "spanId": "", "name": "bad"},
                            ],
                        }
                    ],
                }
            ]
        }
        spans = handler._extract_spans(batch)
        assert [s["name"] for s in spans] == ["good"]

    def test_unrelated_json_is_ignored(self):
        assert handler._extract_spans({"messageType": "CONTROL_MESSAGE"}) == []


def test_valid_spans_still_serialise_to_otlp():
    """End-to-end guard: a filtered span set must still produce a valid request."""
    req = handler.build_export_request(handler._extract_spans(_span()))
    assert len(req.resource_spans) == 1
    span = req.resource_spans[0].scope_spans[0].spans[0]
    assert span.trace_id.hex() == TRACE_ID
    assert span.span_id.hex() == SPAN_ID
