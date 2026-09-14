"""
LogProcessor Lambda — real-time log monitoring.

Triggered by S3 ObjectCreated events on the logs bucket. For each new log
object it:
  1. downloads and parses the file as JSON-lines records
     (malformed lines are counted, never fatal),
  2. runs detectors over the records:
     - severity == "ERROR" records (the classic alert trigger),
     - 5xx HTTP status-code spikes,
     - error-rate threshold breaches,
     - latency p99 threshold breaches,
  3. publishes ONE consolidated SNS alert email per file when errors or any
     detector fires,
  4. emits CloudWatch metrics (ErrorsSeen / RecordsProcessed / MalformedLines
     plus ServerErrors5xx / ErrorRate / LatencyP99).

A total object-read failure raises so Lambda's async retry kicks in; a
failed SNS publish also raises for the same reason. Metric emission is
best-effort so a CloudWatch hiccup never blocks an alert.
"""

import io
import json
import math
import os
import urllib.parse
from datetime import datetime, timezone

import boto3

# ---------------------------------------------------------------------------
# Configuration (all overridable via Lambda environment variables)
# ---------------------------------------------------------------------------
def _int_env(name, default):
    """Read an int env var, falling back to default on missing/invalid."""
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _float_env(name, default):
    """Read a float env var, falling back to default on missing/invalid."""
    try:
        return float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
METRIC_NAMESPACE = os.environ.get("METRIC_NAMESPACE", "LogMonitoring")
ERROR_SUMMARY_LIMIT = _int_env("ERROR_SUMMARY_LIMIT", 5)
EXPECTED_SUFFIX = os.environ.get("EXPECTED_SUFFIX", ".jsonl")

# Detector thresholds (documented in the README).
FIVEXX_THRESHOLD = _int_env("FIVEXX_THRESHOLD", 5)
ERROR_RATE_THRESHOLD = _float_env("ERROR_RATE_THRESHOLD", 0.10)
LATENCY_P99_THRESHOLD_MS = _float_env("LATENCY_P99_THRESHOLD_MS", 2000.0)

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


def _build_alert(bucket, key, error_count, errors, shown, processed,
                 malformed, findings=()):
    """Render the consolidated alert email body."""
    lines = [
        f"Source:  s3://{bucket}/{key}",
        f"Records: {processed} processed, {error_count} error, {malformed} malformed",
    ]
    if findings:
        lines.append("Detectors triggered:")
        for finding in findings:
            lines.append(f"  - {finding['detector']}: {finding['detail']}")
    lines += ["", "Error details:"]
    for i, summary in enumerate(shown, 1):
        lines.append(f"{i}. {summary}")
    if error_count > len(shown):
        lines.append(f"+{error_count - len(shown)} more error(s) not shown "
                     f"(summary limit {ERROR_SUMMARY_LIMIT})")
    return "\n".join(lines)


def _emit_metrics(service, log_file, errors, processed, malformed, stats):
    """Publish per-file metrics (dimensioned) plus undimensioned aggregates.

    Undimensioned datapoints let alarms and the dashboard aggregate across
    files and services; the error-spike alarm watches undimensioned
    ErrorsSeen.
    """
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
        {"MetricName": "ServerErrors5xx", "Dimensions": dims,
         "Value": stats["fivexx"], "Unit": "Count"},
        {"MetricName": "ErrorRate", "Dimensions": dims,
         "Value": stats["error_rate"], "Unit": "None"},
        # Undimensioned aggregates — the alarm's / dashboard's data source.
        {"MetricName": "ErrorsSeen", "Value": errors, "Unit": "Count"},
        {"MetricName": "ServerErrors5xx", "Value": stats["fivexx"],
         "Unit": "Count"},
        {"MetricName": "ErrorRate", "Value": stats["error_rate"],
         "Unit": "None"},
    ]
    if stats["latency_samples"]:
        metric_data += [
            {"MetricName": "LatencyP99", "Dimensions": dims,
             "Value": stats["latency_p99"], "Unit": "Milliseconds"},
            {"MetricName": "LatencyP99", "Value": stats["latency_p99"],
             "Unit": "Milliseconds"},
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


# ---------------------------------------------------------------------------
# Detectors
# ---------------------------------------------------------------------------
def _is_error(record):
    return str(record.get("severity", "")).upper() == "ERROR"


def _status_code(record):
    """HTTP status code from a record, or None if absent/unparseable."""
    raw = record.get("status_code", record.get("status"))
    if raw is None:
        return None
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def _latency_ms(record):
    """Latency in ms from a record, or None if absent/unparseable."""
    for field in ("latency_ms", "duration_ms"):
        if field in record:
            try:
                value = float(record[field])
            except (TypeError, ValueError):
                continue
            if value >= 0:
                return value
    return None


def _percentile(values, pct):
    """Nearest-rank percentile; 0.0 when there are no samples."""
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = math.ceil(pct / 100.0 * len(ordered))
    return ordered[min(len(ordered) - 1, max(0, rank - 1))]


def _detect(records):
    """Run the anomaly detectors over parsed records.

    Returns (findings, stats): a list of finding dicts
    ({"detector": name, "detail": human text}) and a stats dict with the
    raw numbers for metrics. Thresholds come from the FIVEXX_THRESHOLD,
    ERROR_RATE_THRESHOLD and LATENCY_P99_THRESHOLD_MS env vars.
    """
    total = len(records)
    stats = {"fivexx": 0, "error_rate": 0.0, "latency_p99": 0.0,
             "latency_samples": 0}
    if not total:
        return [], stats

    errors = [r for r in records if _is_error(r)]
    fivexx = sum(1 for r in records
                 if (code := _status_code(r)) is not None and 500 <= code < 600)
    error_rate = len(errors) / total
    latencies = [v for v in (_latency_ms(r) for r in records)
                 if v is not None]
    latency_p99 = _percentile(latencies, 99)

    stats.update({"fivexx": fivexx, "error_rate": error_rate,
                  "latency_p99": latency_p99,
                  "latency_samples": len(latencies)})

    findings = []
    if fivexx >= FIVEXX_THRESHOLD:
        findings.append({
            "detector": "5xx_spike",
            "detail": (f"{fivexx} responses with 5xx status "
                       f"(threshold {FIVEXX_THRESHOLD})"),
        })
    if error_rate >= ERROR_RATE_THRESHOLD:
        findings.append({
            "detector": "error_rate",
            "detail": (f"error rate {error_rate:.1%} "
                       f"({len(errors)}/{total} records) "
                       f">= threshold {ERROR_RATE_THRESHOLD:.1%}"),
        })
    if latencies and latency_p99 >= LATENCY_P99_THRESHOLD_MS:
        findings.append({
            "detector": "latency_p99",
            "detail": (f"p99 latency {latency_p99:,.0f} ms "
                       f">= threshold {LATENCY_P99_THRESHOLD_MS:,.0f} ms"),
        })
    return findings, stats


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
        if _is_error(record):
            error_summaries.append(_summarize(record))

    processed = len(records)
    error_count = len(error_summaries)
    findings, stats = _detect(records)
    service = _dominant_service(records)
    log_file = key.rsplit("/", 1)[-1]

    _log("info", "file_processed", bucket=bucket, key=key,
         processed=processed, errors=error_count, malformed=malformed,
         fivexx=stats["fivexx"], error_rate=round(stats["error_rate"], 4),
         latency_p99=round(stats["latency_p99"], 1),
         detectors=[f["detector"] for f in findings])

    _emit_metrics(service, log_file, error_count, processed, malformed, stats)

    if error_count or findings:
        if not SNS_TOPIC_ARN:
            raise RuntimeError("SNS_TOPIC_ARN is not configured")
        shown = error_summaries[:ERROR_SUMMARY_LIMIT]
        subject = f"[LogMonitor] {error_count} error(s) in {log_file}"
        for finding in findings:
            subject += f" [{finding['detector']}]"
        message = _build_alert(bucket, key, error_count, error_summaries,
                               shown, processed, malformed, findings)
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100],
                    Message=message)
        _log("info", "alert_sent", bucket=bucket, key=key,
             errors=error_count, shown=len(shown),
             detectors=[f["detector"] for f in findings])
    else:
        _log("info", "no_errors", bucket=bucket, key=key)

    return {"processed": processed, "errors": error_count,
            "malformed": malformed, "fivexx": stats["fivexx"],
            "error_rate": stats["error_rate"],
            "latency_p99": stats["latency_p99"], "findings": findings}


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
