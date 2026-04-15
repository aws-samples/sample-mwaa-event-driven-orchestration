---
name: mwaa-cross-account-orchestration
description: Generates and deploys producer and consumer DAG Python files for cross-account Amazon MWAA orchestration using Airflow 3.x Asset Watchers with SQS. Use when user asks to "write cross-account MWAA DAGs", "create producer consumer DAGs", "generate Asset Watcher DAG for SQS", "write DAGs to trigger across MWAA environments", or mentions writing Airflow DAGs for cross-account event-driven orchestration. Do NOT use for single-account MWAA, general Airflow questions, or infrastructure provisioning (SQS/IAM/VPC).
metadata:
  category: aws-data-engineering
  tags: [mwaa, airflow, cross-account, sqs, asset-watchers, dag-generation, deployment]
---

# MWAA Cross-Account DAG Generator & Deployer

Generates two ready-to-deploy Python DAG files for cross-account MWAA orchestration, then optionally deploys them:
- **Producer DAG** — publishes a message to SQS when work completes
- **Consumer DAG** — uses an Asset Watcher to react to SQS messages and trigger downstream tasks
- **Auto-deploy** — discovers MWAA environments, uploads DAGs, checks requirements, verifies triggerer health

The user already has their MWAA environments and SQS queue. This skill produces the DAG code and can deploy it.

## Background

Airflow 3.x Asset Watchers enable event-driven cross-account orchestration via SQS — replacing the polling/sensor patterns from 2.x. The producer pushes a message to SQS, the consumer's Asset Watcher (running in the triggerer) reacts within seconds. No direct MWAA-to-MWAA connectivity needed.

## Instructions

### Step 1: Determine Mode and Collect Inputs

Before generating DAGs, briefly assess whether Asset Watchers + SQS is the right pattern for the user's scenario. If their description better fits an alternative, mention it before proceeding:

- **Same-account, one-to-one trigger, producer needs to wait for consumer** → suggest [`MwaaTriggerDagRunOperator`](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/mwaa.html) instead — no SQS infrastructure needed, and the producer can block until the remote DAG finishes.
- **Trigger condition is a persistent state (file on S3, partition in a table)** → suggest a deferrable Sensor instead — Asset Watchers with persistent-state triggers cause infinite DAG runs.
- **Cross-account, event-driven, decoupled teams** → this skill is the right fit. Proceed.

If the user explicitly asks for Asset Watchers + SQS, or describes a cross-account scenario, skip the assessment and proceed directly.

This skill operates in two modes. Determine which one based on the user's request:

**Mode A — Sample DAGs (default):** The user wants to test whether their cross-account setup works. They say things like "generate cross-account DAGs", "create sample DAGs", "test my MWAA setup", or don't specify custom logic. In this mode:
- Generate the sample DAGs from Step 2/3 exactly as-is
- Only ask for the **SQS queue URL** (if it cannot be auto-detected)
- Do NOT ask about DAG names, schedule, processing logic, or downstream tasks

**Mode B — Custom DAGs:** The user describes specific business logic. They say things like "producer runs a Glue job", "consumer triggers dbt", or provide custom task descriptions. In this mode:
- Use the sample DAGs as the structural base (same imports, same patterns)
- Customize dag_id, task names, tags, schedule, and task logic based on user's description
- Still use `SqsHook` for SQS, `Asset`/`AssetWatcher`/`MessageQueueTrigger` for the consumer
- Keep user-described tasks as stubs (print/logging) unless user provides actual implementation code

**Auto-detect SQS queue URL (both modes):**
1. Run `aws mwaa list-environments` to find MWAA environments
2. If SQS queue can be inferred from existing DAGs or environment config, use it
3. If not, ask the user for just the **SQS queue URL**

If the user asks about IAM or SQS setup, point them to `references/infrastructure-guide.md`.

### Step 2: Generate the Producer DAG

Write the file to the user's working directory as a `.py` file.

**Mode A (sample):** Use the template below **exactly as-is**, only replacing `<USER_SQS_QUEUE_URL>` with the actual SQS queue URL. Do NOT modify code structure, imports, variable names, or logic.

**Mode B (custom):** Use the template as the structural base. Customize `dag_id`, task names, tags, schedule, and processing logic based on user's description. Keep the SQS publishing pattern (SqsHook, message structure) identical.

**Template:**

```python
# Producer DAG - Sends messages to SQS queue (using TaskFlow API)
from airflow.decorators import dag, task
from airflow.sdk import Asset
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from datetime import datetime
import json


@dag(
    dag_id="producer_dag_sqs",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["producer", "sqs"],
    description="Producer DAG that processes data and sends messages to SQS"
)
def producer_dag_sqs():
    
    @task
    def process_customer_data():
        """Simulate data processing task"""
        print("Processing customer data...")
        # Simulate some data processing
        processed_data = {
            "customer_id": "CUST-12345",
            "total_orders": 42,
            "revenue": 15000.50,
            "status": "processed"
        }
        return processed_data
    
    @task
    def publish_to_sqs(processed_data, **kwargs):
        """Publish message to SQS queue"""
        try:
            sqs_hook = SqsHook(aws_conn_id='aws_default')
            queue_url = '<USER_SQS_QUEUE_URL>'
            
            # Access context variables via kwargs in Airflow 3.x
            logical_date = kwargs.get("logical_date")
            dag = kwargs.get("dag")
            run_id = kwargs.get("run_id")
            
            # Create message payload
            message = {
                "timestamp": logical_date.isoformat() if logical_date else datetime.now().isoformat(),
                "dag_id": dag.dag_id if dag else "producer_dag_sqs",
                "run_id": run_id or "unknown",
                "data": processed_data
            }
            
            # Send message to SQS
            response = sqs_hook.send_message(
                queue_url=queue_url,
                message_body=json.dumps(message),
                message_attributes={
                    'dag_id': {'StringValue': message["dag_id"], 'DataType': 'String'}
                }
            )
            
            print(f"Successfully published message to SQS: {message}")
            print(f"SQS MessageId: {response.get('MessageId')}")
            return response
            
        except Exception as e:
            print(f"Error publishing to SQS: {str(e)}")
            raise
    
    # Define task dependencies
    data = process_customer_data()
    publish_to_sqs(data)

# Instantiate the DAG
producer_dag_sqs()
```

### Step 3: Generate the Consumer DAG

Write the file to the user's working directory as a separate `.py` file.

**Mode A (sample):** Use the template below **exactly as-is**, only replacing `<USER_SQS_QUEUE_URL>` with the actual SQS queue URL. Do NOT modify code structure, imports, variable names, or logic.

**Mode B (custom):** Use the template as the structural base. Customize `dag_id`, task names, tags, and downstream logic based on user's description. Keep the Asset Watcher pattern (trigger, Asset, schedule, message extraction) identical.

**Template:**

```python
# Consumer DAG - Receives messages from SQS queue using MessageQueueTrigger (using TaskFlow API)
from airflow.decorators import dag, task
from airflow.sdk import Asset, AssetWatcher
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from datetime import datetime
import json

# Define the trigger
queue_trigger = MessageQueueTrigger(
    scheme="sqs",
    sqs_queue="<USER_SQS_QUEUE_URL>",
    aws_conn_id="aws_default")

# Define the asset with the watcher inline
queue_asset = Asset(
    "customer_data_sqs",
    watchers=[AssetWatcher(name="sqs_watcher", trigger=queue_trigger)]
)

@dag(
    dag_id="consumer_dag_sqs",
    start_date=datetime(2025, 1, 1),
    schedule=[queue_asset],  # Triggered by the Asset (which has the watcher)
    catchup=False,
    tags=["consumer", "sqs", "messaging", "event-driven", "taskflow"],
    description="Consumer DAG triggered by messages in SQS queue"
)
def consumer_dag_sqs():  
    @task
    def process_message(**kwargs):
        """Process the message received from SQS"""
        triggering_asset_events = kwargs.get("triggering_asset_events")
        if not triggering_asset_events:
            print("No triggering events found")
            return []

        results =[] 
        # airflow/models/taskinstance.py has get_triggering_events() function which has the structure
        for asset, events in triggering_asset_events.items(): 
            print(f"Asset: {asset}")
            for event in events:
                print(f"Timestamp: {event.timestamp}")
                print(f"Extra: {event.extra}")
                    
                messages = []
                for msg in event.extra.get("payload", {}).get("message_batch", []):
                    body = json.loads(msg.get("Body", "{}"))
                    messages.append(body)

                results.append({
                    "processed": True,
                    "asset_uri": asset,
                    "timestamp": event.timestamp,
                    "messages": messages
                })
        
        return results

    @task
    def send_notification(results):
        """Send notification after processing"""
        print(f"Processed {len(results)} event(s) from SQS")
        for result in results:
            print(f"Asset: {result['asset_uri']} at {result['timestamp']}")
            for message in result.get("messages", []):
                print(f"  Message: {message}")
    
    # Define task dependencies
    result = process_message()
    send_notification(result)

# Instantiate the DAG
consumer_dag_sqs()
```

### Steps 4-9: Auto-Deploy to MWAA

After generating both DAGs, follow `references/deployment-guide.md` for the full auto-deploy flow.

#### Step 4: Discover Environments

1. Run `aws sts get-caller-identity` to verify CLI access. If it fails, fall back to manual checklist below.
2. Run `aws mwaa list-environments` to find environments.
3. Ask user which is producer and which is consumer.

#### Step 5: Pre-Flight Validation

Run all checks before uploading anything. Report results as a checklist to the user.

1. **Check VPC networking (CRITICAL)** — per [AWS MWAA networking requirements](https://docs.aws.amazon.com/mwaa/latest/userguide/networking-about.html), validate the following. If any FAIL, **STOP and fix before proceeding**:
   - **Subnets**: 2 private subnets in **different AZs** (required for HA). For public routing mode, also need 2 public subnets.
   - **NAT Gateway (public routing mode)**: Each private subnet's route table must have `0.0.0.0/0 → nat-xxx`. AWS recommends **2 NAT Gateways** (one per public subnet) for HA. Each needs an Elastic IP. If only `local` route exists, nothing will work.
   - **VPC Endpoints (private routing mode)**: If no internet gateway exists, VPC endpoints are required for each AWS service (S3, SQS, CloudWatch Logs, ECR, etc.) with **private DNS enabled** and associated to both private subnets.
   - **Security group**: Must allow **self-referencing inbound** (all traffic from same SG) and **all outbound** (`0.0.0.0/0`). Max 5 security groups.
   - **Internet Gateway**: Must exist and be attached to VPC (public routing mode only).
   - **NACLs**: Inbound and outbound must allow all traffic (public mode). Private mode: inbound allow all, outbound deny all (traffic goes through VPC endpoints).
   See `references/troubleshooting-guide.md` for diagnostic commands.
2. **Check CloudWatch log streams** — verify that Scheduler, Worker, DAGProcessing, and Triggerer log groups have at least 1 log stream each. If all are empty, components are not running (likely networking issue). Do NOT rely on the health API alone.
4. **Get environment details** — `aws mwaa get-environment` for both environments. Extract S3 bucket, DAG path, Airflow version, status. Verify both are `AVAILABLE` and running Airflow 3.x.
5. **Validate SQS queue exists** — run `aws sqs get-queue-attributes --queue-url <URL> --attribute-names QueueArn` to confirm the queue is reachable from the current credentials. If it fails, warn the user (likely cross-account — the consumer account owns the queue).
6. **Check SQS queue policy** — run `aws sqs get-queue-attributes --queue-url <URL> --attribute-names Policy` and verify the policy allows `sqs:SendMessage` from the producer account. If the policy is missing or doesn't grant cross-account access, warn the user and point to `references/infrastructure-guide.md`.
7. **Check requirements.txt** — download from S3 for both environments. Verify:
   - `apache-airflow-providers-amazon>=9.22.0` is present
   - `apache-airflow-providers-common-messaging>=2.0.0` is present
   - If missing, flag it but do NOT auto-update yet.
8. **Check constraints.txt** — verify `constraints.txt` exists in the S3 `dags/` folder for both environments. Also check `.airflowignore` is present (so Airflow doesn't parse constraints.txt as a DAG).

Present all results to the user as a pass/fail checklist before proceeding:
```
Pre-flight validation:
[PASS] VPC networking — 2 private subnets in different AZs, NAT Gateway route present, security group self-referencing OK
[PASS] CloudWatch log streams — all components running
[PASS] MWAA environments available
[PASS] SQS queue reachable
[WARN] SQS queue policy — cannot verify cross-account access (run from consumer account)
[PASS] requirements.txt — providers versions OK
[PASS] constraints.txt present
[PASS] .airflowignore present
```

#### Step 6: Confirm with User

Present discovered environments, pre-flight results, and ask before proceeding with any changes.

#### Step 7: Upload DAGs and Fix Issues

1. **Upload DAGs** — `aws s3 cp` each DAG to the correct MWAA S3 bucket
2. **Fix requirements.txt** — if pre-flight flagged missing providers, ask user for confirmation, then upload updated requirements.txt
3. **Upload constraints.txt and .airflowignore** — if missing, ask user for confirmation, then upload them

#### Step 8: Verify Triggerer Health

Check consumer environment triggerer status via CloudWatch `airflow-{ENV}-Triggerer` logs or MWAA CLI health endpoint.

#### Step 9: Offer to Trigger Producer

Use `aws mwaa create-cli-token` + MWAA CLI endpoint (`/aws_mwaa/cli`) to:
1. Run `dags reserialize` on both environments to ensure DAGs are parsed
2. Offer to trigger a test run of the producer DAG

**Every step that modifies state must ask user for confirmation first.**

### Fallback Checklist

If auto-deploy is not possible (no AWS CLI, credentials issues, user declines):

1. Verify SQS queue exists and has a cross-account policy allowing the producer account to send messages
2. Upload producer DAG to producer MWAA S3 bucket under `dags/`
3. Upload consumer DAG to consumer MWAA S3 bucket under `dags/`
4. Ensure both MWAA environments have `constraints.txt` and `.airflowignore` in the `dags/` folder
5. Ensure both MWAA `requirements.txt` include `apache-airflow-providers-amazon>=9.22.0` and `apache-airflow-providers-common-messaging>=2.0.0`
6. In consumer Airflow UI: verify Triggerer is healthy, then enable the consumer DAG
7. Run `dags reserialize` if DAGs don't appear in the UI
8. Trigger the producer DAG — consumer should fire within seconds

For IAM policies, SQS setup, or infrastructure: see `references/infrastructure-guide.md`.

## Gotchas and Troubleshooting

Consult `references/troubleshooting-guide.md` for the full list. Key ones to mention proactively when relevant:

- **NAT Gateway is mandatory** — MWAA private subnets MUST have a `0.0.0.0/0 → NAT Gateway` route. Without it, Scheduler/Worker/Triggerer/DAGProcessor containers cannot reach AWS services. The WebServer and health API may appear healthy (misleading), but tasks will never execute. This is the #1 root cause of "everything looks green but nothing works". Always verify with `aws logs describe-log-streams` — if Scheduler/Worker log groups have 0 streams, it's a networking issue.
- **Don't trust health API alone** — `/api/v2/monitor/health` can report "healthy" even when components aren't functional. Cross-check by verifying CloudWatch log streams exist for Scheduler, Worker, DAGProcessing, and Triggerer.
- **Import errors after network fix** — if requirements.txt failed to install when NAT was missing, fixing the network alone won't help. Must trigger `update-environment` with a new `requirements-s3-object-version` to force reinstall on all containers.
- **`dags reserialize` runs on webserver only** — it can succeed even when DAG processor is broken. If import errors persist in the UI after reserialize, the DAG processor container has different dependencies.
- **Session token bug** — Asset Watchers stop after ~30 min in older providers. Require `providers-amazon>=9.22.0`.
- **Failed tasks don't emit events** — if the producer task fails, no SQS message is sent.
- **Never use permanent-state triggers** (e.g., S3KeyTrigger) as Asset Watchers — they fire infinitely because the condition stays true after the first check. Only use consumable-event triggers like `MessageQueueTrigger`.
- **Keep DAG top-level code lightweight** — the DAG processor re-parses every file on each cycle. `MessageQueueTrigger` and `Asset` objects are cheap to construct, but avoid putting API calls, heavy imports, or database queries at module level next to them.
- **Design consumer tasks to handle duplicate messages** — a producer retry on transient SQS failure can send the same message twice. Use UPSERT logic and deduplicate on `run_id` from the payload.
- **Wait for DAG parsing after S3 upload** — don't trigger a DAG right after `aws s3 cp`. Run `dags reserialize` via the MWAA CLI or wait for it to appear in the UI. File sync, parsing, and serialization need time.
- **Alternative patterns exist** — if the user's scenario is same-account one-to-one triggering, suggest `MwaaTriggerDagRunOperator` instead. If the trigger is a persistent state (file exists), suggest a deferrable Sensor. See `references/troubleshooting-guide.md` for the full comparison.
