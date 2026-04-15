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
            queue_url = 'https://sqs.<REGION>.amazonaws.com/<CONSUMER_ACCOUNT_ID>/mwaa-asset-events'

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
