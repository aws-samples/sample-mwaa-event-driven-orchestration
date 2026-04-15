# Auto-Deploy Guide

After generating both DAG files, follow these steps to auto-detect MWAA environments and deploy.

## Step 4: Discover MWAA Environments

### 4a. Check AWS CLI access

Run: `aws sts get-caller-identity`

If this fails, tell the user their AWS credentials are not configured and fall back to the manual checklist (see "Fallback Checklist" below). Do not proceed with auto-deploy.

### 4b. Discover MWAA environments

Extract the region from the SQS queue URL (e.g., `us-east-1` from `https://sqs.us-east-1.amazonaws.com/...`). Run:

```bash
aws mwaa list-environments --region <sqs_region>
```

If the user has multiple AWS profiles for different accounts, ask which profiles correspond to producer and consumer. Then run `list-environments` with `--profile <profile>` for each.

- **One environment found**: ask "I found `<name>` in `<region>`. Is this producer or consumer? What's the AWS CLI profile for the other account?"
- **Two environments found**: ask "I found `<env1>` and `<env2>`. Which is producer and which is consumer?"

### 4c. Get environment details

For each environment:

```bash
aws mwaa get-environment --name <env_name> --region <region> [--profile <profile>]
```

Extract:
- `SourceBucketArn` — strip `arn:aws:s3:::` prefix to get bucket name
- `DagS3Path` — S3 prefix for DAGs (usually `dags`)
- `RequirementsS3Path` — path to requirements.txt (if set)
- `AirflowVersion` — verify it's 3.x
- `Status` — should be `AVAILABLE`

### 4d. Ask user to confirm deployment

Present what was discovered:
> "I found your MWAA environments:
> - **Producer**: `<env_name>` → `s3://<bucket>/<dag_path>/`
> - **Consumer**: `<env_name>` → `s3://<bucket>/<dag_path>/`
>
> Would you like me to upload the DAGs and check requirements automatically?"

Only proceed if the user confirms.

## Step 5: Deploy DAGs to S3

```bash
aws s3 cp <producer_dag_file> s3://<producer_bucket>/<dag_path>/ --region <region> [--profile <profile>]
aws s3 cp <consumer_dag_file> s3://<consumer_bucket>/<dag_path>/ --region <region> [--profile <profile>]
```

Report success/failure for each upload.

## Step 6: Check and Update Consumer Requirements

Required packages:
```
apache-airflow-providers-amazon>=9.22.0
apache-airflow-providers-common-messaging>=2.0.0
```

### 6a. Download current requirements.txt

If `RequirementsS3Path` was found:
```bash
aws s3 cp s3://<consumer_bucket>/<requirements_path> /tmp/mwaa_requirements.txt --region <region> [--profile <profile>]
```

Read the file and check if both packages are present with sufficient versions.

### 6b. Determine what's needed

- Both present with sufficient versions → "Requirements already satisfied", skip update
- Missing or versions too low → tell user what needs to be added/updated
- No `RequirementsS3Path` configured → tell user they need to configure a requirements file in MWAA environment settings first

### 6c. Update if needed

Ask user: "The consumer environment is missing these packages: `<list>`. Should I update the requirements.txt and upload it?"

If confirmed:
1. Add/update missing lines in the downloaded file
2. Upload: `aws s3 cp /tmp/mwaa_requirements.txt s3://<consumer_bucket>/<requirements_path> --region <region> [--profile <profile>]`
3. Warn: "MWAA will take a few minutes to update. Environment status will temporarily show `UPDATING`."

## Step 7: Verify Consumer Triggerer Health

### 7a. Check environment status

```bash
aws mwaa get-environment --name <consumer_env> --region <region> [--profile <profile>] --query 'Environment.Status'
```

- `AVAILABLE` → proceed
- `UPDATING` → "Environment is updating. Wait until status returns to AVAILABLE before testing."
- `UNAVAILABLE` → warn user, suggest checking MWAA console

### 7b. Check triggerer via CloudWatch (best-effort)

```bash
aws logs filter-log-events \
  --log-group-name "airflow-<consumer_env>-Triggerer" \
  --start-time $(date -d '10 minutes ago' +%s000 2>/dev/null || date -v-10M +%s000) \
  --limit 5 \
  --region <region> [--profile <profile>]
```

- Log events returned → "Triggerer is active (recent log entries found)"
- No log group or no events → "Could not verify triggerer health from CloudWatch. Check Airflow UI."
- `ImportError` in logs → warn about missing provider packages

## Step 8: Offer to Trigger the Producer

Ask: "Everything is deployed. Would you like me to trigger the producer DAG to test the end-to-end flow?"

If confirmed:

```bash
CLI_TOKEN=$(aws mwaa create-cli-token --name <producer_env> --region <region> [--profile <profile>] --query 'CliToken' --output text)
WEB_SERVER=$(aws mwaa get-environment --name <producer_env> --region <region> [--profile <profile>] --query 'Environment.WebserverUrl' --output text)

curl -s --request POST \
  "https://${WEB_SERVER}/aws_mwaa/cli" \
  --header "Authorization: Bearer ${CLI_TOKEN}" \
  --header "Content-Type: text/plain" \
  --data-raw "dags trigger <producer_dag_id>"
```

If successful: "Producer DAG triggered. The consumer should fire within seconds once the producer completes. Check the consumer Airflow UI to monitor."

## Fallback Checklist

If auto-deploy is not possible (no AWS CLI, credentials issues, user declines):

1. Upload producer DAG to producer MWAA S3 bucket under `dags/`
2. Upload consumer DAG to consumer MWAA S3 bucket under `dags/`
3. Ensure consumer MWAA has the required packages in `requirements.txt`
4. In consumer Airflow UI: verify Triggerer is healthy, then enable the consumer DAG
5. Trigger the producer DAG — consumer should fire within seconds
