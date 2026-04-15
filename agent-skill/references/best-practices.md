# Best Practices: Cross-Account MWAA with Asset Watchers

Sources are cited inline. Where no source is cited, the recommendation is based on the combined patterns from this project's sample code and general AWS/SQS guidance.

## Avoid Infinite Trigger Loops

> **Source:** [Apache Airflow Event Scheduling docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html)

**CRITICAL:** Only use triggers that detect *transient* events (e.g., a new SQS message). Triggers that check for *permanent* states — like whether a file exists in S3 or whether a database row is present — will fire indefinitely once the condition becomes true. The Airflow docs explicitly call out `S3KeyTrigger` as incompatible with Asset Watchers for this reason.

SQS `MessageQueueTrigger` is safe because each message is consumed and deleted after processing.

## Asset URI Naming

> **Source:** [Apache Airflow Assets docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)

- Asset URIs are **case-sensitive** (including the host portion — this deviates from RFC 3986). `s3://Bucket/data` and `s3://bucket/data` are treated as different assets.
- The `airflow` URI scheme is reserved and cannot be used.
- **Never store credentials or secrets in asset URIs or `extra` fields** — these are stored unencrypted in the Airflow metadata database.
- Keep the URI stable — embed partition dates, record counts, and job status in the SQS **message body**, not the URI.
- Define the Asset, its Watcher, and the consumer DAG `schedule=[...]` in the same file. This makes URI drift impossible.

## Asset Permission Security

> **Source:** [Apache Airflow Assets docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)

Granting `can_create` permission on Assets is **effectively equivalent to granting trigger permissions on ALL downstream DAGs** that depend on those assets. In a multi-tenant MWAA environment, this means a user who can create asset events can trigger any DAG scheduled on that asset. Scope asset permissions carefully in shared environments.

## MWAA Default SQS Policy Risk

> **Source:** [CeleryStrike security research](https://github.com/AI-redteam/CeleryStrike)

MWAA's default execution role includes a wildcard SQS policy: `sqs:*` on `arn:aws:sqs:*:*:airflow-celery-*` across **any account**. This is a design-level concern for cross-account architectures. Review and tighten your execution role's SQS permissions — ensure cross-account SQS access is scoped to your specific queue ARN, not the celery wildcard.

## SQS Tuning

| Parameter | Recommended | Why |
|-----------|-------------|-----|
| `VisibilityTimeout` | 300s (5 min) | Must exceed Asset Watcher processing time, otherwise messages get redelivered and cause duplicate DAG runs |
| `MessageRetentionPeriod` | 86400s (1 day) minimum | Must cover maximum consumer downtime (MWAA upgrades, environment recreation). Use 1209600 (14 days) for critical pipelines |
| `ReceiveMessageWaitTimeSeconds` | 20s | Long polling reduces API calls and costs |
| `waiter_delay` (on `MessageQueueTrigger`) | 10–30s | Controls how frequently the Asset Watcher polls SQS. Lower = faster reaction, higher = fewer API calls. Default varies by provider. |
| DLQ `maxReceiveCount` | 3–5 | Messages that repeatedly fail processing go to DLQ instead of cycling forever |

> **Source for `waiter_delay`:** [Astronomer Event-Driven Scheduling docs](https://www.astronomer.io/docs/learn/airflow-event-driven-scheduling) — documents the `waiter_delay` parameter on `MessageQueueTrigger` and provider version requirements (`apache-airflow-providers-amazon>=9.7.0`, `apache-airflow-providers-common-messaging>=1.0.2`).

**Use one queue per logical asset boundary.** Separate queues allow scoped IAM policies and prevent a noisy producer from delaying unrelated consumers.

## Session Token Handling

> **Source:** [Apache Airflow issue #51213](https://github.com/apache/airflow/issues/51213), fixed in [PR #51699](https://github.com/apache/airflow/pull/51699) (June 2025)

SQS Asset Watchers stopped working after ~30 minutes due to AWS session token expiration. This is fixed in `apache-airflow-providers-amazon>=9.22.0`. If you observe the watcher stopping:
1. Check triggerer logs for `ExpiredTokenException` or `InvalidClientTokenId`
2. Verify provider package version
3. Confirm the MWAA execution role has a session duration long enough for your workloads

## Trigger Stability

> **Source:** [Apache Airflow PR #64659](https://github.com/apache/airflow/pull/64659) (Apr 2026) and [PR #64625](https://github.com/apache/airflow/pull/64625)

Two bugs affected Asset Watcher stability in earlier Airflow 3.x versions:
- **Null trigger crash:** The triggerer's `Trigger.clean_unused()` could delete triggers between parsing loops, causing `AttributeError: 'NoneType'` on the DAG processor. Fixed with a null-check guard.
- **Triggers deleted every parsing loop:** Serialization mismatch caused `BaseEventTrigger.hash()` to produce different hashes for the same trigger, resulting in triggers being recreated on every DAG parse cycle.

Both are fixed in Airflow 3.2.x. If on 3.0.6, pin to the latest patch and monitor triggerer logs for repeated trigger creation/deletion patterns.

## Conditional Asset Scheduling

> **Source:** [Apache Airflow Asset Scheduling docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html)

For multi-producer patterns, Airflow 3.x supports AND/OR logic:

```python
from airflow.sdk import Asset

orders_asset = Asset("orders_complete", watchers=[...])
inventory_asset = Asset("inventory_complete", watchers=[...])

# Trigger only when BOTH assets update (AND)
@dag(schedule=[orders_asset & inventory_asset])
def combined_report(): ...

# Trigger when EITHER updates (OR)
@dag(schedule=[orders_asset | inventory_asset])
def alert_on_any_update(): ...
```

**Note:** With AND scheduling, ALL assets must update at least once since the last DAG run before the downstream DAG fires. A single missing update blocks everything. Failed or skipped tasks do NOT emit asset events.

## Alternative: MwaaTriggerDagRunOperator

> **Source:** [Apache Airflow Amazon Provider docs](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/mwaa.html)

For simpler cases where you don't need SQS durability, Airflow's Amazon provider includes `MwaaTriggerDagRunOperator` which can directly trigger DAGs in a **remote MWAA environment**:

```python
MwaaTriggerDagRunOperator(
    task_id="trigger_remote_dag",
    env_name="consumer-mwaa-environment",
    trigger_dag_id="target_dag",
    wait_for_completion=True,
    airflow_version=3,
)
```

Also available: `MwaaDagRunSensor` and `MwaaTaskSensor` for monitoring remote DAG/task completion. This requires `airflow:InvokeRestApi` cross-account IAM permissions.

**When to use SQS + Asset Watchers instead:** When you need message durability (producer doesn't care if consumer is temporarily down), multiple consumers for the same event, or full decoupling (producer doesn't know consumer's environment name).

## Monitoring and Alerting

**CloudWatch alarms to set up:**

1. **SQS `ApproximateNumberOfMessagesVisible` on main queue** — growing beyond expected rate means the triggerer is likely down or consumer DAG is paused
2. **SQS `ApproximateNumberOfMessagesVisible` on DLQ** — any messages here indicate processing failures
3. **MWAA CloudWatch log group `airflow-{ENV}-Triggerer`** — monitor for `ERROR` level entries

**Common triggerer log patterns:**
- `botocore.exceptions.ClientError` — IAM permissions missing
- `QueueDoesNotExist` — wrong queue URL or queue deleted
- `ImportError` — provider package version conflict
- `ExpiredTokenException` — session token bug (upgrade providers)

## Provider Version Requirements

> **Source:** [Astronomer docs](https://www.astronomer.io/docs/learn/airflow-event-driven-scheduling) + this project's `requirements.txt`

| Package | Minimum Version | Notes |
|---------|----------------|-------|
| `apache-airflow-providers-amazon` | 9.7.0 (recommend 9.22.0) | 9.22.0 includes session token fix |
| `apache-airflow-providers-common-messaging` | 1.0.2 (recommend 2.0.0) | Provides `MessageQueueTrigger` |
| `aiobotocore` | 3.1.2 | Required async dependency for SQS |

Always use a constraints file to prevent dependency conflicts. Test requirements locally with the [aws-mwaa-docker-images](https://github.com/aws/aws-mwaa-local-runner) container before deploying.

> **Source for local testing:** [AWS MWAA Python Dependencies best practices](https://docs.aws.amazon.com/mwaa/latest/userguide/best-practices-dependencies.html)

## Message Payload Design

Include enough metadata for the consumer to make routing decisions without calling back to the producer:

```json
{
  "timestamp": "2025-10-15T00:00:00+00:00",
  "dag_id": "producer_orders_etl",
  "run_id": "scheduled__2025-10-15T00:00:00+00:00",
  "data": {
    "status": "processed",
    "partition_date": "2025-10-15",
    "record_count": 42000,
    "output_path": "s3://producer-bucket/orders/2025/10/15/"
  }
}
```

Validate defensively — check for expected keys before accessing nested values. Log raw `event.extra` on parse failures.
