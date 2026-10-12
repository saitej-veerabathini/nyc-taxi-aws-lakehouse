import json
import re
from urllib.parse import unquote_plus

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from pendulum import datetime

from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)


SQS_QUEUE_URL = (
    "https://sqs.us-east-1.amazonaws.com/"
    "367396826461/nyc-taxi-yellow-bronze-events"
)


@dag(
    dag_id="nyc_taxi_yellow_s3_listener",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "s3", "sqs"],
)
def yellow_s3_listener():

    wait_for_bronze_file = SqsSensor(
        task_id="wait_for_bronze_file",
        sqs_queue=SQS_QUEUE_URL,
        max_messages=1,
        wait_time_seconds=20,
        poke_interval=30,
        timeout=60 * 60,
        mode="reschedule",
        delete_message_on_reception=False,
    )

    @task
    def extract_partition(messages: list[dict]) -> dict:
        if not messages:
            raise ValueError("No SQS messages were received")

        # SQS message Body contains the actual S3 event JSON.
        body = json.loads(messages[0]["Body"])

        # Ignore the message created while configuring S3 notifications.
        if body.get("Event") == "s3:TestEvent":
            raise ValueError(
                "Received an S3 test event instead of an object-created event"
            )

        record = body["Records"][0]

        bucket_name = record["s3"]["bucket"]["name"]
        object_key = unquote_plus(record["s3"]["object"]["key"])

        pattern = (
            r"^bronze/yellow/"
            r"year=(\d{4})/"
            r"month=(0[1-9]|1[0-2])/"
            r"[^/]+\.parquet$"
        )

        match = re.match(pattern, object_key)

        if not match:
            raise ValueError(
                f"Unexpected Bronze object key: {object_key}"
            )

        partition = {
            "bucket": bucket_name,
            "object_key": object_key,
            "process_mode": "incremental",
            "load_year": match.group(1),
            "load_month": match.group(2),
            "dry_run": False,
            "max_rows": 0,
        }

        print(f"Detected object: s3://{bucket_name}/{object_key}")
        print(
            "Detected partition: "
            f"year={partition['load_year']}, "
            f"month={partition['load_month']}"
        )

        return partition

    @task
    def delete_sqs_message(messages: list[dict]) -> None:
        import boto3

        if not messages:
            raise ValueError("No SQS messages were provided for deletion")

        receipt_handle = messages[0]["ReceiptHandle"]

        sqs_client = boto3.client(
            "sqs",
            region_name="us-east-1",
        )

        sqs_client.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=receipt_handle,
        )

        print("Successfully processed and deleted the SQS message")

    received_messages = wait_for_bronze_file.output["messages"]

    partition = extract_partition(received_messages)

    trigger_yellow_pipeline = TriggerDagRunOperator(
        task_id="trigger_yellow_pipeline",
        trigger_dag_id="nyc_taxi_yellow_pipeline",
        conf={
            "process_mode": (
                "{{ ti.xcom_pull(task_ids='extract_partition')"
                "['process_mode'] }}"
            ),
            "load_year": (
                "{{ ti.xcom_pull(task_ids='extract_partition')"
                "['load_year'] }}"
            ),
            "load_month": (
                "{{ ti.xcom_pull(task_ids='extract_partition')"
                "['load_month'] }}"
            ),
            "dry_run": False,
            "max_rows": 0,
        },
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    delete_processed_message = delete_sqs_message(received_messages)

    (
        partition
        >> trigger_yellow_pipeline
        >> delete_processed_message
    )


yellow_s3_listener()
