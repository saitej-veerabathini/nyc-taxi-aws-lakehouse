resource "aws_athena_workgroup" "analytics" {
  name        = "${local.resource_prefix}-analytics"
  description = "Athena workgroup for NYC Taxi dbt analytics"

  configuration {
    enforce_workgroup_configuration    = false
    publish_cloudwatch_metrics_enabled = true

    # Optional safety limit: 5 GB scanned per query
    bytes_scanned_cutoff_per_query = 5368709120

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.bucket}/athena-results/dbt/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = local.common_tags
}

resource "aws_glue_catalog_database" "gold" {
  name        = "${local.catalog_prefix}_gold"
  description = "Gold analytics models managed by dbt"

  tags = local.common_tags
}