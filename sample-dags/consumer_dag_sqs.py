# Consumer DAG - Receives messages from SQS queue using MessageQueueTrigger (using TaskFlow API)
from airflow.decorators import dag, task
from airflow.sdk import Asset, AssetWatcher
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from datetime import datetime
import json

# Define the trigger
queue_trigger = MessageQueueTrigger(
    scheme="sqs",
    sqs_queue="https://sqs.<REGION>.amazonaws.com/<CONSUMER_ACCOUNT_ID>/mwaa-asset-events",
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
