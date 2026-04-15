# Infrastructure Guide: SQS Queue and IAM for Cross-Account MWAA

This is a reference for users who need to set up the SQS queue and IAM policies. The main skill generates the DAG code — this guide covers the prerequisites.

## SQS Queue Setup (Consumer Account)

Create the queue in the **consumer** account:

```bash
aws sqs create-queue \
    --queue-name mwaa-cross-account-events \
    --attributes '{
        "MessageRetentionPeriod": "86400",
        "VisibilityTimeout": "300",
        "ReceiveMessageWaitTimeSeconds": "20"
    }'
```

Recommended: add a dead-letter queue with `maxReceiveCount` of 3–5.

## Cross-Account IAM

Both sides need policies — if either is missing, operations fail silently.

**Producer execution role** (identity policy):
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["sqs:SendMessage", "sqs:GetQueueUrl"],
    "Resource": "arn:aws:sqs:<REGION>:<CONSUMER_ACCOUNT_ID>:<QUEUE_NAME>"
  }]
}
```

**SQS queue resource policy** (consumer account):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowProducerSend",
      "Effect": "Allow",
      "Principal": {"AWS": "arn:aws:iam::<PRODUCER_ACCOUNT_ID>:role/<PRODUCER_ROLE>"},
      "Action": ["sqs:SendMessage", "sqs:GetQueueUrl"],
      "Resource": "arn:aws:sqs:<REGION>:<CONSUMER_ACCOUNT_ID>:<QUEUE_NAME>"
    },
    {
      "Sid": "AllowConsumerReceive",
      "Effect": "Allow",
      "Principal": {"AWS": "arn:aws:iam::<CONSUMER_ACCOUNT_ID>:role/<CONSUMER_ROLE>"},
      "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"],
      "Resource": "arn:aws:sqs:<REGION>:<CONSUMER_ACCOUNT_ID>:<QUEUE_NAME>"
    }
  ]
}
```

**Test before deploying DAGs:**
```bash
aws sqs send-message \
    --queue-url https://sqs.<REGION>.amazonaws.com/<CONSUMER_ACCOUNT_ID>/<QUEUE_NAME> \
    --message-body '{"test": true}'
```

## Consumer MWAA Requirements

The consumer environment needs these in `requirements.txt`:

```
--constraint "/usr/local/airflow/dags/constraints.txt"

apache-airflow-providers-amazon>=9.22.0
apache-airflow-providers-common-messaging>=2.0.0
```

Upload the Airflow 3.0.6 constraints file to `dags/` folder and add `.airflowignore` containing `constraints\.txt`.

> Test requirements locally with [aws-mwaa-docker-images](https://github.com/aws/aws-mwaa-local-runner) before deploying. (Source: [AWS MWAA dependencies best practices](https://docs.aws.amazon.com/mwaa/latest/userguide/best-practices-dependencies.html))

## Security Notes

- **MWAA default SQS wildcard**: The default execution role has `sqs:*` on `arn:aws:sqs:*:*:airflow-celery-*` across any account. Tighten this for cross-account setups. (Source: [CeleryStrike](https://github.com/AI-redteam/CeleryStrike))
- **Asset `can_create` permission**: Grants trigger permissions on all downstream DAGs that depend on those assets. Scope carefully in shared environments. (Source: [Airflow assets docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html))
- **Enable SQS encryption** (SSE-SQS or SSE-KMS) for production. If using KMS, both MWAA execution roles need `kms:Decrypt` and `kms:GenerateDataKey`.
