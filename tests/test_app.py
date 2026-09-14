"""pytest suite for the LogProcessor Lambda — all AWS calls mocked."""
import io
import json
import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Import app with boto3.client patched so no clients are created for real.
# ---------------------------------------------------------------------------
SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC))

import boto3  # noqa: E402

# Mirror the environment variables SAM injects in template.yaml.
os.environ.setdefault("SNS_TOPIC_ARN",
                       "arn:aws:sns:us-east-1:123456789012:log-alerts")
os.environ.setdefault("METRIC_NAMESPACE", "LogMonitoring")
os.environ.setdefault("ERROR_SUMMARY_LIMIT", "5")

s3_mock = MagicMock(name="s3")
sns_mock = MagicMock(name="sns")
cw_mock = MagicMock(name="cloudwatch")


def _fake_client(name, *args, **kwargs):
    return {"s3": s3_mock, "sns": sns_mock, "cloudwatch": cw_mock}[name]


boto3.client = _fake_client

import app  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_mocks():
    for mock in (s3_mock, sns_mock, cw_mock):
        mock.reset_mock()
        mock.reset_mock(side_effect=True)
    sns_mock.publish.return_value = {"MessageId": "fake-id"}
    yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _event(bucket="logs-bucket", key="app/2026-09-13.jsonl"):
    return {
        "Records": [
            {
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key},
                },
            }
        ]
    }


def _s3_object(lines):
    """Point s3_mock at a fake object whose Body yields the given lines."""
    payload = "\n".join(
        line if isinstance(line, str) else json.dumps(line) for line in lines
    ).encode()
    s3_mock.get_object.return_value = {"Body": io.BytesIO(payload)}


def _metric(metric_name):
    """Return MetricData entries matching metric_name from the last put call."""
    assert cw_mock.put_metric_data.called, "expected put_metric_data to be called"
    _, kwargs = cw_mock.put_metric_data.call_args
    return [m for m in kwargs["MetricData"] if m["MetricName"] == metric_name]


def _rec(severity, msg="boom", service="payments", ts="2026-09-13T02:11:04Z",
        status_code=None, latency_ms=None):
    rec = {"ts": ts, "service": service, "severity": severity, "message": msg}
    if status_code is not None:
        rec["status_code"] = status_code
    if latency_ms is not None:
        rec["latency_ms"] = latency_ms
    return rec


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
def test_happy_path_errors_detected_sns_sent_once_metrics_emitted():
    _s3_object([
        _rec("INFO", "ok"),
        _rec("ERROR", "card declined"),
        _rec("INFO", "ok"),
        _rec("ERROR", "timeout upstream"),
    ])

    result = app.lambda_handler(_event(), None)

    assert result["statusCode"] == 200
    assert result["filesProcessed"] == 1

    # Exactly one consolidated SNS alert.
    assert sns_mock.publish.call_count == 1
    _, kwargs = sns_mock.publish.call_args
    assert "2 error(s)" in kwargs["Subject"]
    assert "card declined" in kwargs["Message"]
    assert "timeout upstream" in kwargs["Message"]

    # Dimensioned metrics carry Service + LogFile.
    dimmed = [m for m in _metric("ErrorsSeen") if m.get("Dimensions")]
    assert len(dimmed) == 1
    dims = {d["Name"]: d["Value"] for d in dimmed[0]["Dimensions"]}
    assert dims == {"Service": "payments", "LogFile": "2026-09-13.jsonl"}
    assert dimmed[0]["Value"] == 2

    # Undimensioned aggregate exists for the alarm.
    assert any(not m.get("Dimensions") and m["Value"] == 2
               for m in _metric("ErrorsSeen"))

    processed = [m for m in _metric("RecordsProcessed") if m.get("Dimensions")]
    assert processed[0]["Value"] == 4


def test_no_error_file_sends_no_sns_but_still_emits_metrics():
    _s3_object([_rec("INFO", "all good"), _rec("WARN", "slow query")])

    result = app.lambda_handler(_event(), None)

    assert result["filesProcessed"] == 1
    sns_mock.publish.assert_not_called()
    assert _metric("ErrorsSeen")[0]["Value"] in (0,)
    processed = [m for m in _metric("RecordsProcessed") if m.get("Dimensions")]
    assert processed[0]["Value"] == 2


def test_malformed_lines_are_counted_not_fatal():
    _s3_object([
        '{"this is": "not valid json"',
        _rec("ERROR", "disk full", service="storage"),
        "just some plain text",
        _rec("INFO", "fine"),
        '{"severity": "INFO"} trailing junk {{{',
    ])

    result = app.lambda_handler(_event(), None)

    assert result["filesProcessed"] == 1
    # The single ERROR still produced exactly one alert.
    assert sns_mock.publish.call_count == 1
    malformed = [m for m in _metric("MalformedLines") if m.get("Dimensions")]
    assert malformed[0]["Value"] == 3
    processed = [m for m in _metric("RecordsProcessed") if m.get("Dimensions")]
    assert processed[0]["Value"] == 2


def test_many_errors_consolidated_into_one_email_with_overflow_note():
    lines = [_rec("ERROR", f"failure #{i}") for i in range(7)]
    _s3_object(lines)

    app.lambda_handler(_event(), None)

    assert sns_mock.publish.call_count == 1
    _, kwargs = sns_mock.publish.call_args
    body = kwargs["Message"]
    assert "7 error(s)" in kwargs["Subject"]
    # Only the first 5 summaries are listed, with an overflow note.
    assert "failure #4" in body
    assert "failure #5" not in body
    assert "+2 more error(s)" in body


def test_sns_failure_raises_for_retry():
    _s3_object([_rec("ERROR", "kaboom")])
    sns_mock.publish.side_effect = RuntimeError("SNS throttled")

    with pytest.raises(RuntimeError):
        app.lambda_handler(_event(), None)


def test_s3_read_failure_raises_for_retry():
    from botocore.exceptions import ClientError
    s3_mock.get_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}},
        "GetObject",
    )

    with pytest.raises(ClientError):
        app.lambda_handler(_event(), None)

    sns_mock.publish.assert_not_called()


def test_non_jsonl_suffix_is_skipped_quietly():
    result = app.lambda_handler(_event(key="app/report.csv"), None)

    assert result["filesProcessed"] == 0
    s3_mock.get_object.assert_not_called()
    sns_mock.publish.assert_not_called()
    cw_mock.put_metric_data.assert_not_called()


def test_url_encoded_key_is_decoded():
    _s3_object([_rec("INFO", "fine")])

    app.lambda_handler(_event(key="app/my+log%20file.jsonl"), None)

    _, kwargs = s3_mock.get_object.call_args
    assert kwargs["Key"] == "app/my log file.jsonl"


def test_template_yaml_smoke_check():
    import yaml

    # CloudFormation intrinsic tags (!Ref, !Sub, ...) have no YAML
    # constructor; map them to plain scalars/sequences/mappings.
    def _unknown(loader, tag_suffix, node):
        if isinstance(node, yaml.ScalarNode):
            return loader.construct_scalar(node)
        if isinstance(node, yaml.SequenceNode):
            return loader.construct_sequence(node)
        return loader.construct_mapping(node)

    yaml.SafeLoader.add_multi_constructor("", _unknown)

    template_path = Path(__file__).resolve().parent.parent / "template.yaml"
    with open(template_path) as fh:
        template = yaml.safe_load(fh)

    resources = template["Resources"]
    for name in ("LogsBucket", "LogProcessor", "AlertsTopic", "ErrorSpikeAlarm",
                 "LogMonitoringDashboard"):
        assert name in resources, f"missing resource {name}"

    fn_props = resources["LogProcessor"]["Properties"]
    assert fn_props["Handler"] == "app.lambda_handler"
    env = fn_props["Environment"]["Variables"]
    assert "SNS_TOPIC_ARN" in env and "METRIC_NAMESPACE" in env
    # Detector thresholds are configurable via environment variables.
    assert env["FIVEXX_THRESHOLD"] == "5"
    assert env["ERROR_RATE_THRESHOLD"] == "0.10"
    assert env["LATENCY_P99_THRESHOLD_MS"] == "2000"

    # The dashboard graphs the pipeline's metrics.
    dashboard_body = json.dumps(
        resources["LogMonitoringDashboard"]["Properties"]["DashboardBody"])
    for metric in ("ErrorsSeen", "ServerErrors5xx", "ErrorRate", "LatencyP99"):
        assert metric in dashboard_body, f"dashboard missing {metric}"

    policies = fn_props["Policies"][0]["Statement"]
    actions = {a for stmt in policies for a in stmt["Action"]}
    for needed in ("s3:GetObject", "sns:Publish", "cloudwatch:PutMetricData"):
        assert needed in actions, f"missing IAM action {needed}"

    bucket_props = resources["LogsBucket"]["Properties"]
    assert bucket_props["VersioningConfiguration"]["Status"] == "Enabled"
    assert bucket_props["PublicAccessBlockConfiguration"]["BlockPublicAcls"] is True

    alarm = resources["ErrorSpikeAlarm"]["Properties"]
    assert alarm["MetricName"] == "ErrorsSeen"
    assert alarm["Period"] == 300
    assert len(alarm["AlarmActions"]) == 1


# ---------------------------------------------------------------------------
# New detectors: 5xx spike, error-rate threshold, latency p99 threshold
# ---------------------------------------------------------------------------
def test_fivexx_spike_detector_fires_alert_without_error_severity():
    # 6 responses with 5xx status, all marked INFO — the 5xx detector
    # (default threshold 5) must still raise an alert.
    _s3_object([_rec("INFO", f"req {i}", status_code=503)
                for i in range(6)])

    result = app._process_object("logs-bucket", "app/2026-09-13.jsonl")

    assert result["fivexx"] == 6
    assert [f["detector"] for f in result["findings"]] == ["5xx_spike"]

    assert sns_mock.publish.call_count == 1
    _, kwargs = sns_mock.publish.call_args
    assert "[5xx_spike]" in kwargs["Subject"]
    assert "5xx_spike" in kwargs["Message"]
    assert "6 responses with 5xx status" in kwargs["Message"]

    fivexx = [m for m in _metric("ServerErrors5xx") if m.get("Dimensions")]
    assert fivexx[0]["Value"] == 6
    assert any(not m.get("Dimensions") and m["Value"] == 6
               for m in _metric("ServerErrors5xx"))


def test_fivexx_detector_stays_quiet_below_threshold():
    _s3_object([_rec("INFO", f"req {i}", status_code=503) for i in range(4)]
               + [_rec("INFO", "ok", status_code=200)])

    result = app._process_object("logs-bucket", "app/2026-09-13.jsonl")

    assert result["fivexx"] == 4
    assert result["findings"] == []
    sns_mock.publish.assert_not_called()


def test_error_rate_detector_fires_on_high_error_fraction():
    lines = ([_rec("ERROR", f"bad {i}") for i in range(3)]
             + [_rec("INFO", f"ok {i}") for i in range(17)])
    _s3_object(lines)

    result = app._process_object("logs-bucket", "app/2026-09-13.jsonl")

    assert result["error_rate"] == pytest.approx(0.15)
    assert "error_rate" in [f["detector"] for f in result["findings"]]

    _, kwargs = sns_mock.publish.call_args
    assert "[error_rate]" in kwargs["Subject"]
    assert "error rate 15.0%" in kwargs["Message"]

    rate = [m for m in _metric("ErrorRate") if m.get("Dimensions")]
    assert rate[0]["Value"] == pytest.approx(0.15)


def test_error_rate_detector_stays_quiet_below_threshold():
    lines = [_rec("ERROR", "one bad")] + [_rec("INFO", f"ok {i}")
                                          for i in range(19)]
    _s3_object(lines)

    result = app._process_object("logs-bucket", "app/2026-09-13.jsonl")

    assert result["findings"] == []


def test_latency_p99_detector_fires_and_emits_metric():
    lines = ([_rec("INFO", f"ok {i}", latency_ms=100) for i in range(9)]
             + [_rec("ERROR", "slow", latency_ms=5000)])
    _s3_object(lines)

    result = app._process_object("logs-bucket", "app/2026-09-13.jsonl")

    assert result["latency_p99"] == 5000
    assert "latency_p99" in [f["detector"] for f in result["findings"]]

    _, kwargs = sns_mock.publish.call_args
    assert "[latency_p99]" in kwargs["Subject"]
    assert "p99 latency 5,000 ms" in kwargs["Message"]

    p99 = [m for m in _metric("LatencyP99") if m.get("Dimensions")]
    assert p99[0]["Value"] == 5000
    assert p99[0]["Unit"] == "Milliseconds"
    assert any(not m.get("Dimensions") for m in _metric("LatencyP99"))


def test_no_latency_samples_means_no_latency_metric():
    _s3_object([_rec("INFO", "ok"), _rec("INFO", "fine")])

    app.lambda_handler(_event(), None)

    assert cw_mock.put_metric_data.called
    _, kwargs = cw_mock.put_metric_data.call_args
    names = {m["MetricName"] for m in kwargs["MetricData"]}
    assert "LatencyP99" not in names


def test_detector_thresholds_read_from_env_with_bad_value_fallback():
    import importlib
    import os as _os

    old = dict(_os.environ)
    try:
        _os.environ["FIVEXX_THRESHOLD"] = "not-a-number"
        _os.environ["ERROR_RATE_THRESHOLD"] = "12"
        _os.environ["LATENCY_P99_THRESHOLD_MS"] = ""
        importlib.reload(app)
        assert app.FIVEXX_THRESHOLD == 5  # bad value -> default
        assert app.ERROR_RATE_THRESHOLD == 12.0  # valid value honored
        assert app.LATENCY_P99_THRESHOLD_MS == 2000.0  # empty -> default
    finally:
        _os.environ.clear()
        _os.environ.update(old)
        importlib.reload(app)


def test_generate_sample_logs_produces_valid_demo_file(tmp_path):
    import subprocess

    script = (Path(__file__).resolve().parent.parent
              / "scripts" / "generate_sample_logs.py")
    out = tmp_path / "demo.jsonl"
    proc = subprocess.run(
        [sys.executable, str(script), "--records", "100", "--burst-size", "20",
         "--seed", "1", "--output", str(out)],
        capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr

    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(rows) == 100
    for row in rows:
        assert {"ts", "service", "severity", "status_code", "latency_ms",
                "message", "request_id"} <= set(row)

    errors = sum(1 for r in rows if r["severity"] == "ERROR")
    fivexx = sum(1 for r in rows if 500 <= r["status_code"] < 600)
    # The injected burst (20 records, mostly errors) must be visible.
    assert errors >= 10
    assert fivexx >= 10
    # Timestamps are monotonic and ISO-8601.
    assert [r["ts"] for r in rows] == sorted(r["ts"] for r in rows)
