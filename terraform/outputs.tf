output "aws_account_id" {
  description = "AWS account used by Terraform"
  value       = data.aws_caller_identity.current.account_id
}

output "aws_principal_arn" {
  description = "AWS identity used by Terraform"
  value       = data.aws_caller_identity.current.arn
}

output "lakehouse_bucket_name" {
  description = "Name of the NYC Taxi Lakehouse bucket"
  value       = aws_s3_bucket.lakehouse.bucket
}

output "lakehouse_bucket_arn" {
  description = "ARN of the NYC Taxi Lakehouse bucket"
  value       = aws_s3_bucket.lakehouse.arn
}

output "database_bronze" {
  description = "Glue database containing bronze tables"
  value       = aws_glue_catalog_database.bronze.name
}

output "crawler_bronze" {
  description = "Crawler for bronze yellow taxi data"
  value       = aws_glue_crawler.bronze_yellow.name
}

output "databse_silver" {
  description = "Glue database containing silver tables"
  value       = aws_glue_catalog_database.silver.name
}

output "crawler_silver" {
  description = "Crawler for silver yellow taxi data"
  value       = aws_glue_crawler.silver_yellow.name
}

# output "bronze_yellow_crawler_role_arn" {
#   description = "IAM role used by the bronze yellow crawler"
#   value       = aws_iam_role.bronze_yellow_crawler.arn
# }

output "glue_scripts_keys" {
  description = "S3 keys of uploaded Glue scripts"
  value = sort([
    for script in values(aws_s3_object.glue_scripts) : script.key
  ])
}

output "athena_workgroup_name" {
  description = "Athena workgroup used by dbt"
  value       = aws_athena_workgroup.analytics.name
}

output "databse_gold" {
  description = "Glue database containing dbt gold models"
  value       = aws_glue_catalog_database.gold.name
}