# MWAA Cross-Account Orchestration — Claude Code Skill

A Claude Code skill that generates and deploys producer/consumer DAG files for cross-account Amazon MWAA orchestration using Airflow 3.x Asset Watchers with SQS.

## Prerequisites

Before using this skill, you need:

1. **Two MWAA environments** (Airflow 3.x) — one producer, one consumer
2. **An SQS queue** — accessible from both accounts (cross-account policy configured)
3. **VPC networking** — private subnets with NAT Gateway routes, security groups with self-referencing inbound rules
4. **AWS CLI** configured with credentials that can access both MWAA environments
5. **Provider packages** in both environments' `requirements.txt`:
   - `apache-airflow-providers-amazon>=9.22.0`
   - `apache-airflow-providers-common-messaging>=2.0.0`

## What Triggers This Skill

The skill activates when you say things like:
- "Write cross-account MWAA DAGs"
- "Create producer consumer DAGs"
- "Generate Asset Watcher DAG for SQS"
- "Write DAGs to trigger across MWAA environments"

It does **not** activate for single-account MWAA, general Airflow questions, or infrastructure provisioning.

## What the Skill Does

### Two Modes

**Mode A — Sample DAGs (default)**
For testing whether your cross-account setup works. The skill generates ready-to-deploy sample DAGs with simulated data processing. You only need to provide the SQS queue URL.

**Mode B — Custom DAGs**
For real business logic. Describe your tasks (e.g., "producer runs a Glue job", "consumer triggers dbt") and the skill customizes the DAGs while preserving the same SQS/Asset Watcher structure.

### Auto-Deploy Flow (Steps 1–9)

After generating the DAGs, the skill can:

1. **Discover** your MWAA environments via `aws mwaa list-environments`
2. **Pre-flight validate** — VPC networking, CloudWatch log streams, environment status, SQS reachability, requirements.txt, constraints.txt
3. **Upload** DAGs to the correct S3 buckets
4. **Fix issues** — update requirements.txt, add constraints.txt / .airflowignore if missing
5. **Verify** triggerer health on the consumer environment
6. **Trigger** a test run of the producer DAG

Every step that modifies state asks for confirmation first.

## What You Get

After applying the skill, you get two deployed DAG files:

**producer_dag_sqs.py** — Processes data and publishes a JSON message to SQS via `SqsHook`. The `publish_to_sqs` task log shows the message was successfully sent with a MessageId:

![Producer — publish_to_sqs task log showing "Successfully published message to SQS" and MessageId](screenshots/producer-publish-task-log.png)

**consumer_dag_sqs.py** — An `Asset` with an `AssetWatcher` using `MessageQueueTrigger` that monitors SQS and auto-triggers downstream tasks. The `process_message` task log shows the received Asset event with `from_trigger: True` and the SQS message batch payload:

![Consumer — process_message task log showing received Asset event with SQS message_batch payload](screenshots/consumer-process-task-log.png)

The consumer DAG is auto-triggered with run type `asset_triggered` — no manual intervention needed:

![Consumer DAG runs — auto-triggered by Asset Watcher with state "success"](screenshots/consumer-dag-runs.png)

The consumer's Asset (`customer_data_sqs`) is visible in the Airflow Assets page, linked to the consuming DAG:

![Assets page — customer_data_sqs asset with 1 consuming DAG](screenshots/consumer-assets.png)

<!-- TODO: Add SQS console screenshot at screenshots/sqs-queue.png -->

### End-to-End Flow

```
Producer DAG triggered
  → process_customer_data (success)
    → publish_to_sqs → JSON message sent to SQS
      → Consumer Asset Watcher detects SQS message (~30s)
        → consumer_dag_sqs auto-triggered (asset_triggered)
          → process_message extracts payload (success)
            → send_notification (success)
```

SQS messages are consumed by the Asset Watcher — the queue stays at 0 after processing.

## Troubleshooting Coverage

The skill includes a troubleshooting guide that covers:

| Symptom | Root Cause |
|---------|-----------|
| Health API green but nothing runs | NAT Gateway missing in private subnets |
| Tasks stuck in `up_for_retry` | Workers can't reach AWS services (networking) |
| Consumer DAG import error | Provider packages not installed (requirements.txt failed) |
| `dags reserialize` succeeds but import error persists | DAG processor has different dependency state than webserver |
| Watcher stops after ~30 min | Session token bug — upgrade providers-amazon to >=9.22.0 |
| DAG triggers infinitely | Wrong trigger type (permanent state instead of consumable event) |
| MWAA CLI returns 307 | Use `-L` flag with curl to follow redirects |

## Best Practices Included

- **VPC networking validation** — 2 private subnets in different AZs + NAT Gateway, cross-check with CloudWatch Log Streams
- **Don't trust Health API alone** — always verify with CloudWatch logs
- **Dependency management** — pin versions, constraints.txt, .airflowignore, force reinstall after network fix
- **Orchestration pattern selection** — decision matrix for Asset Watchers + SQS vs MwaaTriggerDagRunOperator vs Sensors
- **DAG authoring guidelines** — lightweight top-level code, idempotent tasks, no credentials in code

## Skill Structure

```
agent-skill/
  SKILL.md                       # Entry point: mode selection, DAG templates, auto-deploy (Steps 1-9)
  TEST_PLAN.md                   # Test plan
  README.md                      # This file
  references/
    deployment-guide.md          # Deployment steps
    infrastructure-guide.md      # IAM / SQS / VPC configuration
    troubleshooting-guide.md     # Troubleshooting table + VPC network diagnostics
    best-practices.md            # Airflow 3.x best practices
```
