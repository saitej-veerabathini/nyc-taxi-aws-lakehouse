resource "aws_glue_catalog_database" "bronze" {
  name        = "${local.catalog_prefix}_bronze"
  description = "Data Catalog database for raw NYC Taxi datasets"
  tags        = local.common_tags
}

resource "aws_glue_crawler" "bronze_yellow" {
  name          = "${local.resource_prefix}-bronze-crawler"
  database_name = aws_glue_catalog_database.bronze.name
  role          = aws_iam_role.bronze_yellow_crawler.arn
  table_prefix  = "bronze_"
  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/bronze/yellow/"
  }
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
  depends_on = [
    aws_iam_role_policy_attachment.bronze_yellow_crawler_glue,
    aws_iam_role_policy.bronze_yellow_crawler_s3
  ]
  tags = merge(local.common_tags, {
    Layer = "bronze"
  })
}