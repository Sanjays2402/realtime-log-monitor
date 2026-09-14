#!/usr/bin/env python3
"""Generate realistic demo JSONL logs for the real-time log monitor.

Produces a file of log records with a background error rate plus a
concentrated *error burst* (5xx statuses, ERROR severities, spiking
latency) partway through — so every detector in the LogProcessor Lambda
(5xx spike, error-rate threshold, latency p99) fires when the file is
uploaded to the logs bucket.

Usage:
    python3 scripts/generate_sample_logs.py --records 600 --burst-size 40
    aws s3 cp demo-logs.jsonl s3://<logs-bucket>/demo-logs.jsonl

Only the standard library is used; no AWS credentials needed.
"""
import argparse
import json
import random
import sys
from datetime import datetime, timedelta, timezone

SERVICES = ["payments", "auth", "search", "checkout", "notify"]

MESSAGES = {
    "INFO": [
        "request completed", "cache hit", "user session refreshed",
        "webhook delivered", "index updated", "charge ok",
    ],
    "WARN": [
        "slow query (>500ms)", "retrying upstream call", "cache miss storm",
        "deprecated endpoint used", "connection pool near limit",
    ],
    "ERROR": [
        "upstream timeout", "card declined", "database connection refused",
        "null pointer in handler", "queue consumer crashed",
        "payment gateway 5xx", "disk full on worker",
    ],
}


def _record(ts, service, severity, rng):
    status = 200
    latency = max(1.0, rng.lognormvariate(4.6, 0.7))  # ~median 100ms
    if severity == "ERROR":
        status = rng.choice([500, 502, 503, 504])
        latency = max(1.0, rng.lognormvariate(8.0, 0.6))  # seconds-scale
    elif severity == "WARN":
        status = rng.choice([200, 200, 429])
        latency = max(1.0, rng.lognormvariate(6.2, 0.5))
    return {
        "ts": ts.isoformat().replace("+00:00", "Z"),
        "service": service,
        "severity": severity,
        "status_code": status,
        "latency_ms": round(latency, 1),
        "message": rng.choice(MESSAGES[severity]),
        "request_id": "".join(rng.choice("0123456789abcdef") for _ in range(8)),
    }


def generate(records, burst_size, burst_at, error_rate, seed, start):
    rng = random.Random(seed)
    out = []
    burst_start = int(records * burst_at)
    burst_end = min(records, burst_start + burst_size)
    for i in range(records):
        ts = start + timedelta(seconds=i)
        service = rng.choice(SERVICES)
        if burst_start <= i < burst_end:
            severity = "ERROR" if rng.random() < 0.8 else "WARN"
        else:
            roll = rng.random()
            severity = "ERROR" if roll < error_rate else (
                "WARN" if roll < error_rate + 0.10 else "INFO")
        out.append(_record(ts, service, severity, rng))
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate demo JSONL logs with an error burst for the "
                    "real-time log monitor.")
    parser.add_argument("--records", "-n", type=int, default=600,
                        help="total log records to generate (default: 600)")
    parser.add_argument("--burst-size", type=int, default=40,
                        help="records in the injected error burst (default: 40)")
    parser.add_argument("--burst-at", type=float, default=0.6,
                        help="fraction into the file where the burst starts "
                             "(default: 0.6)")
    parser.add_argument("--error-rate", type=float, default=0.03,
                        help="background ERROR fraction outside the burst "
                             "(default: 0.03)")
    parser.add_argument("--seed", type=int, default=42,
                        help="random seed for reproducible output (default: 42)")
    parser.add_argument("--output", "-o", default="demo-logs.jsonl",
                        help="output file (default: demo-logs.jsonl)")
    args = parser.parse_args(argv)

    if args.records < 1:
        parser.error("--records must be >= 1")
    if not 0.0 <= args.burst_at <= 1.0:
        parser.error("--burst-at must be between 0 and 1")

    start = datetime.now(timezone.utc).replace(microsecond=0)
    rows = generate(args.records, args.burst_size, args.burst_at,
                    args.error_rate, args.seed, start)
    with open(args.output, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")

    errors = sum(1 for r in rows if r["severity"] == "ERROR")
    fivexx = sum(1 for r in rows if 500 <= r["status_code"] < 600)
    p99 = sorted(r["latency_ms"] for r in rows)[int(0.99 * len(rows)) - 1]
    print(f"wrote {len(rows)} records to {args.output}: "
          f"{errors} ERROR, {fivexx} 5xx, p99 latency {p99:,.0f} ms",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
