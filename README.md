# Real-Time Log Monitoring on AWS

An event-driven pipeline: an app writes JSON-lines logs to S3, each new file
triggers a Lambda that parses the records, emails **one consolidated alert**
per file containing `ERROR`s, and emits CloudWatch metrics with an alarm on
error spikes.

Built with AWS SAM (Python 3.12). Everything is free-tier friendly.

## Architecture

![Architecture](docs/architecture.svg)

## How it works

1. **Ingest.** Any `.jsonl` object uploaded to the logs bucket fires an
   `s3:ObjectCreated:*` notification (suffix filter keeps CSVs, zips, etc.
   out of the pipeline).
2. **Parse.** The Lambda streams the object and parses it as JSON-lines.
   Malformed lines are *counted* in the `MalformedLines` metric — never fatal.
   A record is an error when `severity == "ERROR"` (case-insensitive).
3. **Alert.** If a file contains errors, the function publishes **one** SNS
   message listing up to 5 error summaries plus an overflow note
   (`+N more error(s)`), so a bad deploy can't spam the inbox.
4. **Observe.** Per-file metrics carry `Service` (most common service in the
   file) and `LogFile` dimensions for dashboards; an additional
   *undimensioned* `ErrorsSeen* datapoint is emitted per file so the alarm
   can aggregate across files and services.
5. **Retry semantics.** A total S3 read failure or SNS publish failure raises,
   so Lambda's async retry reprocesses the object. Metric emission is
   best-effort so a CloudWatch hiccup never blocks an alert.

The IAM role is least-privilege: `s3:GetObject` on the logs bucket only,
`sns:Publish` on the alert topic only, `cloudwatch:PutMetricData` scoped to
the pipeline's namespace via condition, plus its own log group.

## Deploy

Prerequisites: AWS CLI configured, SAM CLI installed.

```bash
sam build
sam deploy --guided
```

`--guided` will ask for the stack name, region, and the `AlertEmail`
parameter. **Confirm the SNS subscription email** AWS sends you, otherwise
alerts stay in `PendingConfirmation`.

To see it work end to end:

```bash
BUCKET=$(aws cloudformation describe-stacks --stack-name <stack> \
  --query "Stacks[0].Outputs[?OutputKey=='LogsBucketName'].OutputValue" \
  --output text)

# A file with errors -> one alert email
cat > demo.jsonl <<'EOF'
{"ts":"2026-09-13T02:11:04Z","service":"payments","severity":"INFO","message":"charge ok"}
{"ts":"2026-09-13T02:11:05Z","service":"payments","severity":"ERROR","message":"card declined","request_id":"abc123"}
{"ts":"2026-09-13T02:11:06Z","service":"payments","severity":"ERROR","message":"upstream timeout"}
EOF
aws s3 cp demo.jsonl "s3://$BUCKET/demo.jsonl"

# A clean file -> no email, metrics still emitted
echo '{"ts":"2026-09-13T02:12:00Z","service":"payments","severity":"INFO","message":"all good"}' \
  | aws s3 cp - "s3://$BUCKET/clean.jsonl"
```

## Testing matrix

| Case | How to reproduce | Expected result |
|---|---|---|
| Happy path | Upload `.jsonl` with ≥1 `ERROR` record | One SNS email listing errors; `ErrorsSeen`, `RecordsProcessed` metrics emitted |
| All-OK file | Upload `.jsonl` with no `ERROR`s | No email; metrics still emitted (`ErrorsSeen = 0`) |
| Malformed lines | Mix garbage lines / bad JSON into the file | File still processed; `MalformedLines` counts them; valid errors still alert |
| Large file | Upload a multi-MB `.jsonl` (within the 2 min timeout) | Processed line-by-line; alert lists first 5 errors + overflow note |
| Permission error | Revoke the role's `s3:GetObject` (or delete the object mid-flight) | Handler raises → Lambda async retry; error visible in CloudWatch Logs |
| Alarm firing | Upload files totaling ≥ `ErrorThreshold` errors in 5 min | `error-spike` alarm → `ALARM` → SNS email; returns to `OK` afterwards |
| Duplicate event | Same object notification delivered twice | Idempotent-ish: second run re-emits metrics and re-sends the email (documented; add a DynamoDB dedupe table if this matters to you) |

Run the unit suite (all AWS clients mocked, no credentials needed):

```bash
python3 -m pytest tests/ -q
```

## Cost (free tier)

| Service | Free tier | This project |
|---|---|---|
| Lambda | 1M requests + 400k GB-s / mo | A few invocations per log file |
| S3 | 5 GB storage, 20k GETs, 2k PUTs / mo | One bucket, tiny log files |
| SNS | 1,000 email publishes / mo | One email per error-containing file |
| CloudWatch | 10 metrics, 10 alarms, 5 GB logs | 3 metrics, 1 alarm, small log volume |

Realistic monthly cost at hobby scale: **$0**.

## Enhancement ideas

- Fan out alerts to Slack via AWS Chatbot in addition to email.
- Add a DynamoDB dedupe table (object ETag) to make duplicate deliveries a no-op.
- Ship a CloudWatch dashboard JSON for per-service error rates.
- Compress/archive processed files to a second bucket with a lifecycle rule.
- Split `LogProcessor` into parse + alert steps with SQS for backpressure on huge files.
- Add per-service alarms (e.g. `payments` errors) using metric math.

## Portfolio deliverables checklist

- [ ] Screenshot: objects in the logs bucket (versioning enabled)
- [ ] Screenshot: Lambda execution in CloudWatch Logs (structured JSON lines)
- [ ] Screenshot: the SNS "File Backed Up"-style alert email (subject `[LogMonitor] N error(s) in …`)
- [ ] Screenshot: CloudWatch metrics graph (`ErrorsSeen`, `RecordsProcessed`)
- [ ] Screenshot: `error-spike` alarm in `ALARM` state with notification history
- [ ] GitHub repo: Lambda code + SAM template + this README
- [ ] Resume bullet, e.g.: *"Built an event-driven log-monitoring pipeline on AWS (S3 → Lambda → SNS/CloudWatch) that parses JSON-lines logs, sends consolidated error alerts, and pages on error spikes — deployed with SAM, fully covered by mocked unit tests."*
