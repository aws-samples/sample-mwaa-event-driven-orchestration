# Gotchas and Troubleshooting

## Gotchas to Mention Proactively

Only mention these if relevant to the user's situation — don't dump all of them.

- **Asset URIs are case-sensitive** — `s3://Bucket/data` and `s3://bucket/data` are different assets. (Source: [Airflow assets docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html))
- **Never use permanent-state triggers as Asset Watchers** (e.g., S3KeyTrigger) — they fire infinitely. SQS MessageQueueTrigger is safe because messages are consumed. (Source: [Airflow event scheduling docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html))
- **Session token bug** — Asset Watchers stopped after ~30 min in older providers. Fixed in `apache-airflow-providers-amazon>=9.22.0`. (Source: [Airflow issue #51213](https://github.com/apache/airflow/issues/51213))
- **`waiter_delay` parameter** — controls SQS polling frequency (10-30s typical). Lower = faster, higher = fewer API calls. (Source: [Astronomer docs](https://www.astronomer.io/docs/learn/airflow-event-driven-scheduling))
- **Simpler alternative exists** — if the user doesn't need SQS durability, `MwaaTriggerDagRunOperator` can directly trigger DAGs in a remote MWAA environment without SQS. (Source: [Airflow Amazon provider docs](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/mwaa.html))
- **Failed/skipped tasks don't emit asset events** — only successful task completions update assets. If the producer task fails, no SQS message is sent and the consumer won't trigger. (Source: [Airflow asset scheduling docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html))
- **AND scheduling blocks on all assets** — if the consumer DAG uses `schedule=[asset_a & asset_b]`, ALL assets must update before the DAG fires. (Source: [Airflow asset scheduling docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html))
- **Consumer DAG may receive `None` as `logical_date`** — event-driven DAGs can have multiple simultaneous runs. Don't assume `logical_date` follows a regular schedule. (Source: [Astronomer event-driven scheduling docs](https://www.astronomer.io/docs/learn/airflow-event-driven-scheduling))
- **Never store secrets in asset URIs or `extra` fields** — these are stored unencrypted in the Airflow metadata database. (Source: [Airflow assets docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html))
- **Frequent polling can bottleneck the DB** — each Asset Watcher poll cycle writes to the metadata DB. With many watchers, consider increasing `waiter_delay`. (Source: [AIP-82 analysis](https://blog.zhu424.dev/open-source-contribution/external-event-driven-scheduling-in-airflow/))

## Troubleshooting Table

Only surface these when the user reports a problem.

| Symptom | Cause | Fix |
|---------|-------|-----|
| Triggerer unhealthy | Provider import error | Check CloudWatch `airflow-{ENV}-Triggerer` logs for `ImportError` |
| Consumer DAG never triggers | Asset URI mismatch or DAG paused | Ensure Asset + schedule in same file, DAG enabled |
| SQS messages piling up | Triggerer down or watcher failed | Check triggerer health + logs |
| AccessDenied on SQS | Missing IAM policy (producer or queue) | See `references/infrastructure-guide.md` |
| Duplicate DAG runs | `VisibilityTimeout` too short | Increase to 300s+ |
| Watcher stops after ~30 min | Session token expiration bug | Upgrade to `providers-amazon>=9.22.0` |
| DAG triggers infinitely | Wrong trigger type (permanent state) | Only use `MessageQueueTrigger` for Asset Watchers |
| Tasks stuck in `up_for_retry` with no PID/hostname, Worker log group has 0 streams | Private subnets missing NAT Gateway — workers can't reach AWS services | Add NAT Gateway to the VPC and route `0.0.0.0/0` from private subnets to it. See details below. |
| Health API says healthy but Scheduler/Worker/DAGProcessing have 0 CloudWatch log streams | NAT Gateway missing — health heartbeats come from metadatabase (internal), but containers can't write to CloudWatch or start workers | Verify private subnet route table has `0.0.0.0/0 → nat-xxx`. Don't trust `/api/v2/monitor/health` alone — always cross-check with CloudWatch log streams. |
| DAGs not visible in Airflow UI after S3 upload | DAG processor hasn't parsed the new files yet | Run `dags reserialize` via MWAA CLI token. Note: this runs on the **webserver**, not the DAG processor — it forces a one-time parse. |
| `ModuleNotFoundError: No module named 'airflow.providers.common.messaging'` (import error on consumer DAG) | DAG processor container missing the package — requirements.txt installation failed (e.g., no NAT Gateway at startup time) | Fix the network issue first, then trigger `update-environment` with a new `requirements-s3-object-version` to force MWAA to reinstall dependencies on all containers. |
| MWAA CLI endpoint returns 307 redirect | `/aws_mwaa/cli` redirects to `/aws_mwaa/cli/` | Always use `-L` flag with curl to follow redirects. |
| `dags reserialize` succeeds but import error persists in UI | `reserialize` runs on webserver (which has the package), but DAG processor container has a different dependency state | Must trigger `update-environment` to restart all containers with fresh dependency installation. |
## VPC Networking — Root Cause of Most "Invisible" Failures

**This is the #1 issue encountered in practice.** MWAA runs Scheduler, Worker, Triggerer, and DAG Processor in Fargate containers inside private subnets. These containers need outbound internet access to reach AWS services (CloudWatch Logs, SQS, Celery broker, ECR, etc.).

Reference: [AWS MWAA Networking Requirements](https://docs.aws.amazon.com/mwaa/latest/userguide/networking-about.html)

**Required networking setup (public routing mode):**
1. **2 private subnets** in **different AZs** — MWAA requires this for HA
2. **2 public subnets** in **different AZs** — for NAT Gateways
3. **2 NAT Gateways** (one per public subnet, each with an Elastic IP) — AWS recommends 2 for HA. Private subnet route tables must have `0.0.0.0/0 → NAT Gateway`
4. **Internet Gateway** attached to the VPC — public subnets must route `0.0.0.0/0 → IGW`
5. **Security group** — must allow **self-referencing inbound** (all traffic from same SG) and **all outbound** (`0.0.0.0/0`). Max 5 security groups.
6. **NACLs** — inbound and outbound must allow all traffic

**Required networking setup (private routing mode):**
1. **2 private subnets** in **different AZs**
2. **No NAT Gateway or Internet Gateway** — instead, use **VPC Endpoints** for each AWS service (S3, SQS, CloudWatch Logs, ECR, etc.)
3. All VPC endpoints must have **private DNS enabled** and be associated to both private subnets
4. **Security group** — same self-referencing inbound + all outbound rule
5. **NACLs** — inbound allow all, outbound deny all (traffic goes through VPC endpoints)

**How to diagnose:**
```bash
# Check route table for the MWAA private subnets
aws ec2 describe-route-tables --filters "Name=association.subnet-id,Values=<SUBNET_ID>" \
  --query 'RouteTables[0].Routes[*].{Dest: DestinationCidrBlock, Nat: NatGatewayId, Igw: GatewayId}'
# If you only see "local" and no NAT, that's the problem

# Check CloudWatch log streams (the real health indicator)
for lg in Scheduler Worker DAGProcessing Triggerer; do
  echo "=== $lg ==="
  aws logs describe-log-streams --log-group-name "airflow-<ENV>-$lg" --limit 1 --query 'logStreams[*].logStreamName'
done
# If all are empty [], components aren't running — likely a networking issue
```

**Misleading signals:**
- The **WebServer** works fine without NAT (it has its own network path via MWAA's internal load balancer)
- The **health API** (`/api/v2/monitor/health`) may report "healthy" because heartbeats are written to the metadatabase internally
- `dags reserialize` via CLI token runs on the **webserver**, so it succeeds even when DAG processor is broken
- S3 DAG sync may appear to work (DAGs show up after `reserialize`) but actual execution fails
