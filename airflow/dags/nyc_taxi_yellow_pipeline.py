import time

import pendulum

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sdk import Param, dag, get_current_context, task


AWS_REGION = "us-east-1"
GLUE_JOB_NAME = "nyc-taxi-yellow-bronze-to-silver"
SILVER_CRAWLER_NAME = "nyc-taxi-yellow-silver-crawler"


@dag(
    dag_id="nyc_taxi_yellow_pipeline",
    description="Orchestrate NYC Taxi Yellow Bronze-to-Silver processing",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["nyc-taxi", "yellow", "glue"],
    params={
        "process_mode": Param(
            default="incremental",
            type="string",
            enum=["incremental", "full_refresh"],
            description="Process one month or rebuild all Silver data",
        ),
        "load_year": Param(
            default="2009",
            type="string",
            pattern=r"^\d{4}$",
            description="Required for incremental processing",
        ),
        "load_month": Param(
            default="01",
            type="string",
            pattern=r"^(0[1-9]|1[0-2])$",
            description="Required for incremental processing",
        ),
        "dry_run": Param(
            default=True,
            type="boolean",
            description="Transform and profile data without writing Silver",
        ),
        "max_rows": Param(
            default=1000,
            type="integer",
            minimum=0,
            description="Limit records during a dry run; zero means unlimited",
        ),
    },
)
def yellow_pipeline():

    @task
    def validate_parameters():
        context = get_current_context()
        params = context["params"]

        process_mode = params["process_mode"]
        load_year = str(params["load_year"])
        load_month = str(params["load_month"])
        dry_run = bool(params["dry_run"])
        max_rows = int(params["max_rows"])

        if process_mode not in {"incremental", "full_refresh"}:
            raise ValueError(
                "process_mode must be incremental or full_refresh."
            )

        if process_mode == "incremental":
            if len(load_year) != 4 or not load_year.isdigit():
                raise ValueError(
                    "load_year must contain exactly four digits."
                )

            if (
                len(load_month) != 2
                or not load_month.isdigit()
                or not 1 <= int(load_month) <= 12
            ):
                raise ValueError(
                    "load_month must be between 01 and 12."
                )

        if max_rows < 0:
            raise ValueError("max_rows must be zero or greater.")

        validated = {
            "process_mode": process_mode,
            "load_year": load_year,
            "load_month": load_month,
            "dry_run": dry_run,
            "max_rows": max_rows,
        }

        print(f"Validated parameters: {validated}")

        return validated

    validated_parameters = validate_parameters()

    run_glue_job = GlueJobOperator(
        task_id="run_bronze_to_silver_glue_job",

        # This job is already created and managed by Terraform.
        job_name=GLUE_JOB_NAME,
        update_config=False,

        script_args={
            "--PROCESS_MODE": "{{ params.process_mode }}",
            "--LOAD_YEAR": "{{ params.load_year }}",
            "--LOAD_MONTH": "{{ params.load_month }}",
            "--DRY_RUN": "{{ params.dry_run }}",
            "--MAX_ROWS": "{{ params.max_rows }}",
        },

        aws_conn_id=None,
        region_name=AWS_REGION,

        wait_for_completion=True,
        verbose=True,
        stop_job_run_on_kill=True,

        # If the Airflow task retries while the same Glue run is still active,
        # reconnect to that run instead of submitting another one.
        resume_glue_job_on_retry=True,

        job_poll_interval=15,
        waiter_delay=30,
        waiter_max_attempts=60,
    )

    @task
    def run_silver_crawler():
        import boto3

        crawler_client = boto3.client(
            "glue",
            region_name=AWS_REGION,
        )

        crawler_before_start = crawler_client.get_crawler(
            Name=SILVER_CRAWLER_NAME
        )["Crawler"]

        previous_start_time = (
            crawler_before_start
            .get("LastCrawl", {})
            .get("StartTime")
        )

        print(f"Starting crawler: {SILVER_CRAWLER_NAME}")

        crawler_client.start_crawler(
            Name=SILVER_CRAWLER_NAME
        )

        timeout_seconds = 30 * 60
        poll_interval_seconds = 15
        started_waiting_at = time.monotonic()

        while True:
            crawler = crawler_client.get_crawler(
                Name=SILVER_CRAWLER_NAME
            )["Crawler"]

            crawler_state = crawler["State"]
            last_crawl = crawler.get("LastCrawl", {})
            current_start_time = last_crawl.get("StartTime")
            last_status = last_crawl.get("Status")

            print(
                f"Crawler state={crawler_state}, "
                f"last_status={last_status}, "
                f"start_time={current_start_time}"
            )

            new_crawl_finished = (
                crawler_state == "READY"
                and current_start_time is not None
                and current_start_time != previous_start_time
            )

            if new_crawl_finished:
                if last_status == "SUCCEEDED":
                    print(
                        f"Crawler completed successfully: "
                        f"{SILVER_CRAWLER_NAME}"
                    )

                    return {
                        "crawler_name": SILVER_CRAWLER_NAME,
                        "status": last_status,
                        "start_time": str(current_start_time),
                    }

                raise RuntimeError(
                    f"Crawler finished with status: {last_status}"
                )

            elapsed_seconds = (
                time.monotonic() - started_waiting_at
            )

            if elapsed_seconds >= timeout_seconds:
                raise TimeoutError(
                    f"Crawler did not finish within "
                    f"{timeout_seconds} seconds."
                )

            time.sleep(poll_interval_seconds)

    silver_crawler = run_silver_crawler()

    @task.bash(
        task_id="run_dbt_build",
        append_env=True,
        env={
            "AWS_PROFILE": "default",
            "AWS_REGION": AWS_REGION,
            "AWS_DEFAULT_REGION": AWS_REGION,
            "PROCESS_MODE": "{{ params.process_mode }}",
            "LOAD_YEAR": "{{ params.load_year }}",
            "LOAD_MONTH": "{{ params.load_month }}",
            "DRY_RUN": "{{ params.dry_run | lower }}",
        },
    )
    def run_dbt_build() -> str:
        return r"""
        set -euo pipefail

        cd /opt/airflow/project/dbt

        if [ "$DRY_RUN" = "true" ]; then
            echo "Glue ran in dry-run mode."
            echo "Skipping dbt because Silver was not changed."
            exit 0
        fi

        if [ "$PROCESS_MODE" = "full_refresh" ]; then
            echo "Running a full dbt refresh."

            dbt build \
                --project-dir /opt/airflow/project/dbt \
                --profiles-dir /opt/airflow/project/dbt \
                --select \
                    dim_date \
                    gold_yellow_daily_metrics \
                    gold_yellow_hourly_demand \
                --full-refresh
        else
            echo "Running incremental dbt processing."
            echo "Year=$LOAD_YEAR"
            echo "Month=$LOAD_MONTH"

            dbt build \
                --project-dir /opt/airflow/project/dbt \
                --profiles-dir /opt/airflow/project/dbt \
                --select \
                    dim_date \
                    gold_yellow_daily_metrics \
                    gold_yellow_hourly_demand \
                --vars \
                    "{\"load_year\": $LOAD_YEAR, \"load_month\": $LOAD_MONTH}"
        fi
        """

    dbt_build = run_dbt_build()

    (
        validated_parameters
        >> run_glue_job
        >> silver_crawler
        >> dbt_build
    )


yellow_pipeline()
