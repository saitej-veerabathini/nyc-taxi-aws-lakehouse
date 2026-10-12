resource "aws_glue_job" "yellow_bronze_to_silver" {
  name     = "${local.resource_prefix}-bronze-to-silver"
  role_arn = aws_iam_role.yellow_etl_job.arn

  glue_version = "5.1"
  worker_type  = "G.1X"

  number_of_workers = 2
  timeout           = 15
  max_retries       = 0
  execution_class   = "FLEX"

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.glue_scripts["yellow_bronze_to_silver.py"].key}"
  }

  default_arguments = {
    "--SOURCE_PATH"  = "s3://${aws_s3_bucket.lakehouse.bucket}/bronze/yellow"
    "--TARGET_PATH"  = "s3://${aws_s3_bucket.lakehouse.bucket}/silver/yellow"
    "--PROCESS_MODE" = "incremental"
    "--DRY_RUN"      = "false"
    "--MAX_ROWS"     = "0"
    "--TempDir"      = "s3://${aws_s3_bucket.lakehouse.bucket}/temp/glue"

    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [
    aws_iam_role_policy_attachment.yellow_etl_job_glue,
    aws_iam_role_policy.yellow_etl_job_s3,
    aws_s3_object.glue_scripts
  ]

  tags = merge(local.common_tags, {
    SourceLayer = "bronze"
    TargetLayer = "silver"
  })

}