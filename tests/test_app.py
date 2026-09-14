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


def _rec(severity, msg="boom", service="payments", ts="2026-09-13T02:11:04Z"):
    return {"ts": ts, "service": service, "severity": severity, "message": msg}


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
    for name in ("LogsBucket", "LogProcessor", "AlertsTopic", "ErrorSpikeAlarm"):
        assert name in resources, f"missing resource {name}"

    fn_props = resources["LogProcessor"]["Properties"]
    assert fn_props["Handler"] == "app.lambda_handler"
    env = fn_props["Environment"]["Variables"]
    assert "SNS_TOPIC_ARN" in env and "METRIC_NAMESPACE" in env

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
