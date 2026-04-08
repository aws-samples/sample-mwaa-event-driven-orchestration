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
    def publish_to_sqs(processed_data, logical_date, dag, run_id):
        """Publish message to SQS queue"""
        try:
            sqs_hook = SqsHook(aws_conn_id='aws_default')
            queue_url = 'https://sqs.<REGION>.amazonaws.com/<CONSUMER_ACCOUNT_ID>/mwaa-asset-events'
            
            # Create message payload
            message = {
                "timestamp": logical_date.isoformat(),
                "dag_id": dag.dag_id,
                "run_id": run_id,
                "data": processed_data
            }
            
            # Send message to SQS
            response = sqs_hook.send_message(
                queue_url=queue_url,
                message_body=json.dumps(message),
                message_attributes={
                    'dag_id': {'StringValue': dag.dag_id, 'DataType': 'String'}
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
