import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="nyc_taxi_aws_identity",
    description="Verify read-only AWS access from Airflow",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "aws", "validation"],
)
def nyc_taxi_aws_identity():

    @task
    def verify_aws_identity():
        import boto3

        sts_client = boto3.client(
            "sts",
            region_name="us-east-1",
        )

        identity = sts_client.get_caller_identity()

        account_id = identity["Account"]
        principal_arn = identity["Arn"]

        expected_account_id = "367396826461"

        if account_id != expected_account_id:
            raise ValueError(
                "Airflow connected to the wrong AWS account. "
                f"Expected {expected_account_id}, received {account_id}."
            )

        print(f"AWS account: {account_id}")
        print(f"AWS principal: {principal_arn}")

        return {
            "account_id": account_id,
            "principal_arn": principal_arn,
        }

    verify_aws_identity()


nyc_taxi_aws_identity()