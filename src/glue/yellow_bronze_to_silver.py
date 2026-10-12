import json
import re
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F 

COMMON_ARGUMENTS = [
    "JOB_NAME",
    "SOURCE_PATH",
    "TARGET_PATH",
    "PROCESS_MODE",
    "DRY_RUN",
    "MAX_ROWS"
]

INCREMENTAL_ARGUMENTS = [
    "LOAD_YEAR",
    "LOAD_MONTH"
]

def normalize_column_name(column_name):
    # Convert column names to lowercase snake_case.

    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", column_name)
    normalized = re.sub(r"_+", "_", normalized)

    return normalized.strip("_").lower()

def normalize_columns(dataframe):

    original_columns = dataframe.columns 
    normalized_columns = [
        normalize_column_name(column)
        for column in original_columns
    ]

    if len(normalized_columns) != len(set(normalized_columns)):
        raise ValueError(
            "Column normalization created duplicate names. "
            f"Original columns: {original_columns}; "
            f"normalized columns: {normalized_columns}"
        )

    return dataframe.toDF(*normalized_columns)

def profile_dataframe(dataframe):

    dataframe.cache()

    row_count = dataframe.count()

    null_expressions = [
        F.sum(
            F.when(F.col(column).isNull(), 1).otherwise(0)
        ).alias(column)
        for column in dataframe.columns
    ]

    null_counts = dataframe.agg(*null_expressions).first().asDict()

    return{
        "row_count": row_count,
        "column_count": len(dataframe.columns),
        "null_counts": null_counts
    }

def transform(dataframe):
    dataframe = (
        dataframe
        .withColumn("_trip_pickup_datetime", F.col("trip_pickup_datetime"))
        .withColumn("_trip_dropoff_datetime", F.col("trip_dropoff_datetime"))
        .withColumn("_lower_payment_type", F.lower(F.trim(F.col("payment_type"))))
        .withColumn("_trip_distance", F.round(F.col("trip_distance"),2).cast("decimal(10,2)"))
    )

    dataframe = (
        dataframe
        .withColumn("trip_pickup_datetime", F.to_timestamp("_trip_pickup_datetime", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("trip_dropoff_datetime", F.to_timestamp("_trip_dropoff_datetime", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("fare_amount", F.col("fare_amt").cast("decimal(12,2)"))
        .withColumn("surcharge_amount", F.col("surcharge").cast("decimal(12,2)"))
        .withColumn("mta_tax_amount", F.col("mta_tax").cast("decimal(12,2)"))
        .withColumn("tip_amount", F.col("tip_amt").cast("decimal(12,2)"))
        .withColumn("tolls_amount", F.col("tolls_amt").cast("decimal(12,2)"))
        .withColumn("total_amount", F.col("total_amt").cast("decimal(12,2)"))
        .withColumn("payment_type", F.col("_lower_payment_type"))
        .withColumn("trip_distance", F.col("_trip_distance"))
    )    

    return dataframe

def quality(dataframe):
    dataframe = (
        dataframe
        .withColumn("dq_quality_warnings", 
                    F.array(
                        F.when(
                            F.col("trip_dropoff_datetime") < F.col("trip_pickup_datetime"),
                            F.lit("DROPOFF_BEFORE_PICKUP")
                        ),
                        F.when(
                            ~F.col("passenger_count").between(1,8),
                            F.lit("INVALID_PASSENGER_COUNT")
                        ),
                        F.when(
                            F.col("trip_distance") <= 0,
                            F.lit("INVALID_TRIP_DISTANCE")
                        ),
                        F.when(
                            ((F.coalesce(F.col("fare_amount"), F.lit(0).cast("decimal(12,2)")) + 
                            F.coalesce(F.col("surcharge_amount"), F.lit(0).cast("decimal(12,2)")) + 
                            F.coalesce(F.col("mta_tax_amount"), F.lit(0).cast("decimal(12,2)")) + 
                            F.coalesce(F.col("tip_amount"), F.lit(0).cast("decimal(12,2)")) + 
                            F.coalesce(F.col("tolls_amount"), F.lit(0).cast("decimal(12,2)"))) - 
                            (F.coalesce(F.col("total_amount"), F.lit(0).cast("decimal(12,2)")))) > F.lit(0.01),
                            F.lit("NON_RECONCILED_TOTAL_AMOUNT")
                        )
                    ))
        .withColumn("dq_quality_warnings",
                    F.filter(
                        F.col("dq_quality_warnings"),
                        lambda error: error.isNotNull()
                    ))
        .withColumn("dq_quality_warnings_count",
                    F.size(F.col("dq_quality_warnings")))
    )
    
    return dataframe

def resolve_job_arguments():
    args = getResolvedOptions(sys.argv, COMMON_ARGUMENTS)

    process_mode = args["PROCESS_MODE"].strip().lower()

    if process_mode not in ("incremental","full_refresh"):
        raise ValueError(
            "PROCESS_MODE must be 'incremental' or 'full_refresh'. "
            f"Received: {args['PROCESS_MODE']}"
        )

    if process_mode == "incremental":
        incremental_args = getResolvedOptions(sys.argv, INCREMENTAL_ARGUMENTS)
        args.update(incremental_args)

    return args

def main():
    args = resolve_job_arguments()

    source_path = args["SOURCE_PATH"].rstrip("/") + "/"
    target_path = args["TARGET_PATH"].rstrip("/") + "/"

    process_mode = args["PROCESS_MODE"].strip().lower()

    dry_run_value = args["DRY_RUN"].strip().lower()
    max_rows = int(args["MAX_ROWS"])

    if max_rows < 0:
        raise ValueError("MAX_ROWS must be zero or greater.")

    if dry_run_value not in ("true","false"):
        raise ValueError(
            "DRY_RUN must be either 'true' or 'false'. "
            f"Received: {args['DRY_RUN']}"
        )

    dry_run = dry_run_value == 'true'

    print(f"Dry-run mode: {dry_run}")

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    if process_mode == "incremental":
        load_year = args["LOAD_YEAR"].strip()
        load_month = args["LOAD_MONTH"].strip()

        if not re.fullmatch(r"\d{4}", load_year):
            raise ValueError(
                "LOAD_YEAR must contain exactly four digits. "
                f"Received: {args['LOAD_YEAR']}"
            )

        if not re.fullmatch(r"(0[1-9]|1[0-2])", load_month):
            raise ValueError(
                "LOAD_MONTH must be between 01 and 12. "
                f"Received: {args['LOAD_YEAR']}"
            )

        source_read_path = (
            f"{source_path}"
            f"year={load_year}/"
            f"month={load_month}/"
        )

        partition_overwrite_mode = "dynamic"

    else:
        load_year = None
        load_month = None

        source_read_path = source_path
        partition_overwrite_mode = "static"

    print(f"Process mode: {process_mode}")
    print(f"Reading bronze data from: {source_read_path}")

    if process_mode == "incremental":
        print(f"Processing partition: year={load_year}/month={load_month}")
    else:
        print("Processing all available Bronze partitions.")

    bronze_df = (
        spark
        .read
        .option("basePath", source_path)
        .parquet(source_read_path)
    )

    print("Bronze schema: ")
    bronze_df.printSchema()

    if dry_run and max_rows > 0:
        bronze_df = bronze_df.limit(max_rows)

    normalized_df = normalize_columns(bronze_df)

    required_partition_columns = {"year","month"}
    missing_partition_columns = (
        required_partition_columns - set(normalized_df.columns)
    )

    if missing_partition_columns:
        raise ValueError(
            "Required partition columns were not discovered: "
            f"{sorted(missing_partition_columns)}"
        )

    normalized_df = (
        normalized_df
        .withColumn(
            "year",
            F.lpad(F.col("year").cast("string"), 4, "0")
        )
        .withColumn(
            "month",
            F.lpad(F.col("month").cast("string"), 2, "0")
        )
    )

    if process_mode == "incremental":
        unexpected_partition_count = (
            normalized_df
            .filter(
                (F.col("year") != load_year)
                | (F.col("month") != load_month)
            )
            .limit(1)
            .count()
        )

        if unexpected_partition_count > 0:
            raise ValueError(
                "Bronze records do not match the requested partition."
                f"Expected year={load_year}, month={load_month}."
            )

    profile = profile_dataframe(normalized_df)

    print("Bronze profile: ")
    print(json.dumps(profile, indent=2, default=str))

    transformed_df = transform(normalized_df)

    quality_df = quality(transformed_df)

    deduplicated_df = quality_df.dropDuplicates()
    deduplicated_count = deduplicated_df.count()
    duplicate_count = profile["row_count"] - deduplicated_count

    print(f"Original rows: {profile['row_count']}")
    print(f"Duplicate rows: {duplicate_count}")
    print(f"Silver rows: {deduplicated_count}")

    silver_df = (
        deduplicated_df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_layer", F.lit("bronze"))
        .withColumn("_dataset", F.lit("yellow"))
    )

    print("Proposed silver schema:")
    

    silver_df = (
        silver_df.select(
            "vendor_name",
            "trip_pickup_datetime",
            "trip_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "start_lon",
            "start_lat",
            "rate_code",
            "store_and_forward",
            "end_lon",
            "end_lat",
            "payment_type",
            "fare_amount",
            "surcharge_amount",
            "mta_tax_amount",
            "tip_amount",
            "tolls_amount",
            "total_amount",
            "dq_quality_warnings",
            "dq_quality_warnings_count",
            "year",
            "month",
            "_ingested_at",
            "_source_layer",
            "_dataset"
        )
    )

    silver_df.printSchema()

    if dry_run:
        print("DRY RUN: Silver output will not be written.")

        print("Sample transformed rows:")
        silver_df.show(1, truncate=False, vertical=True)

        print(f"Transformed row count: {silver_df.count():,}")
    else:
        print(f"Writing silver data to: {target_path}")

        spark.conf.set(
            "spark.sql.sources.partitionOverwriteMode",
            partition_overwrite_mode
        )

        (
            silver_df.write
            .mode("overwrite")
            .format("parquet")
            .option("compression","snappy")
            .partitionBy("year", "month")
            .save(target_path)
        )

        print("Silver write completed successfully.")


    normalized_df.unpersist()
    job.commit()

if __name__ == "__main__":
    main()