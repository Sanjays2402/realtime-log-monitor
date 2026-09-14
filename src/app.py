"""
LogProcessor Lambda — real-time log monitoring.

Triggered by S3 ObjectCreated events on the logs bucket. For each new log
object it:
  1. downloads and parses the file as JSON-lines records
     (malformed lines are counted, never fatal),
  2. detects records with ``severity == "ERROR"``,
  3. publishes ONE consolidated SNS alert email per file,
  4. emits CloudWatch metrics (ErrorsSeen / RecordsProcessed / MalformedLines).

A total object-read failure raises so Lambda's async retry kicks in; a
failed SNS publish also raises for the same reason. Metric emission is
best-effort so a CloudWatch hiccup never blocks an alert.
"""

import io
import json
import os
import urllib.parse
from datetime import datetime, timezone

import boto3

# ---------------------------------------------------------------------------
# Configuration (all overridable via Lambda environment variables)
# ---------------------------------------------------------------------------
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
METRIC_NAMESPACE = os.environ.get("METRIC_NAMESPACE", "LogMonitoring")
ERROR_SUMMARY_LIMIT = int(os.environ.get("ERROR_SUMMARY_LIMIT", "5"))
EXPECTED_SUFFIX = os.environ.get("EXPECTED_SUFFIX", ".jsonl")

# ---------------------------------------------------------------------------
# AWS clients (created once per execution environment)
# ---------------------------------------------------------------------------
s3 = boto3.client("s3")
sns = boto3.client("sns")
cloudwatch = boto3.client("cloudwatch")


def _log(level, event, **fields):
    """Structured JSON log line to stdout (picked up by CloudWatch Logs)."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "event": event,
    }
    entry.update(fields)
    print(json.dumps(entry, default=str))


def _summarize(record):
    """Build a one-line human summary of an error record."""
    service = record.get("service", "unknown")
    ts = record.get("ts") or record.get("timestamp", "?")
    message = record.get("message") or record.get("msg") or "(no message)"
    request_id = record.get("request_id") or record.get("req")
    suffix = f" (req={request_id})" if request_id else ""
    text = f"[{ts}] {service}: {message}{suffix}"
    return text[:280]


def _build_alert(bucket, key, error_count, errors, shown, processed, malformed):
    """Render the consolidated alert email body."""
    lines = [
        f"Source:  s3://{bucket}/{key}",
        f"Records: {processed} processed, {error_count} error, {malformed} malformed",
        "",
        "Error details:",
    ]
    for i, summary in enumerate(shown, 1):
        lines.append(f"{i}. {summary}")
    if error_count > len(shown):
        lines.append(f"+{error_count - len(shown)} more error(s) not shown "
                     f"(summary limit {ERROR_SUMMARY_LIMIT})")
    return "\n".join(lines)


def _emit_metrics(service, log_file, errors, processed, malformed):
    """Publish per-file metrics (dimensioned) plus an aggregate ErrorsSeen
    metric with no dimensions, which is what the CloudWatch alarm watches."""
    dims = [
        {"Name": "Service", "Value": service[:255]},
        {"Name": "LogFile", "Value": log_file[:255]},
    ]
    metric_data = [
        {"MetricName": "ErrorsSeen", "Dimensions": dims,
         "Value": errors, "Unit": "Count"},
        {"MetricName": "RecordsProcessed", "Dimensions": dims,
         "Value": processed, "Unit": "Count"},
        {"MetricName": "MalformedLines", "Dimensions": dims,
         "Value": malformed, "Unit": "Count"},
        # Undimensioned aggregate — the alarm's data source.
        {"MetricName": "ErrorsSeen", "Value": errors, "Unit": "Count"},
    ]
    try:
        cloudwatch.put_metric_data(
            Namespace=METRIC_NAMESPACE, MetricData=metric_data)
        _log("info", "metrics_emitted",
             namespace=METRIC_NAMESPACE, errors=errors, processed=processed)
    except Exception as exc:  # best-effort: never block alert delivery
        _log("warning", "metrics_failed", error=str(exc))


def _dominant_service(records):
    """Most common `service` value across parsed records (for the metric
    dimension); falls back to 'unknown'."""
    counts = {}
    for record in records:
        name = str(record.get("service", "unknown"))
        counts[name] = counts.get(name, 0) + 1
    return max(counts, key=counts.get) if counts else "unknown"


def _process_object(bucket, key):
    """Download, parse and analyze one log object. Raises on total read
    failure (so the event is retried) or on SNS publish failure."""
    response = s3.get_object(Bucket=bucket, Key=key)  # raises on failure
    body = response["Body"]
    if hasattr(body, "read"):
        stream = body
    else:  # pragma: no cover - defensive
        stream = io.BytesIO(bytes(body))

    records = []
    malformed = 0
    error_summaries = []

    raw = stream.read()
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            malformed += 1
            continue
        if not isinstance(record, dict):
            malformed += 1
            continue
        records.append(record)
        if str(record.get("severity", "")).upper() == "ERROR":
            error_summaries.append(_summarize(record))

    processed = len(records)
    error_count = len(error_summaries)
    service = _dominant_service(records)
    log_file = key.rsplit("/", 1)[-1]

    _log("info", "file_processed", bucket=bucket, key=key,
         processed=processed, errors=error_count, malformed=malformed)

    _emit_metrics(service, log_file, error_count, processed, malformed)

    if error_count:
        if not SNS_TOPIC_ARN:
            raise RuntimeError("SNS_TOPIC_ARN is not configured")
        shown = error_summaries[:ERROR_SUMMARY_LIMIT]
        subject = f"[LogMonitor] {error_count} error(s) in {log_file}"
        message = _build_alert(bucket, key, error_count, error_summaries,
                               shown, processed, malformed)
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100],
                    Message=message)
        _log("info", "alert_sent", bucket=bucket, key=key,
             errors=error_count, shown=len(shown))
    else:
        _log("info", "no_errors", bucket=bucket, key=key)

    return {"processed": processed, "errors": error_count,
            "malformed": malformed}


def lambda_handler(event, context):
    """S3 event entry point."""
    results = []
    for record in event.get("Records", []):
        s3info = record.get("s3", {})
        bucket = s3info.get("bucket", {}).get("name")
        key = s3info.get("object", {}).get("key")
        if not bucket or not key:
            raise ValueError(f"Malformed S3 event record: {record!r}")
        key = urllib.parse.unquote_plus(key)

        if not key.endswith(EXPECTED_SUFFIX):
            _log("info", "skipped_non_log_file", bucket=bucket, key=key,
                 expected_suffix=EXPECTED_SUFFIX)
            continue

        _log("info", "processing_file", bucket=bucket, key=key)
        # Any exception here (S3 read failure, SNS failure) propagates so
        # Lambda's async retry reprocesses the object.
        results.append(_process_object(bucket, key))

    return {"statusCode": 200, "filesProcessed": len(results)}
