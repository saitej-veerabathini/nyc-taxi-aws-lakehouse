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

output "bronze_glue_database_name" {
  description = "Glue database containing bronze tables"
  value       = aws_glue_catalog_database.bronze.name
}

output "bronze_yellow_crawler_name" {
  description = "Crawler for bronze yellow taxi data"
  value       = aws_glue_crawler.bronze_yellow.name
}

output "bronze_yellow_crawler_role_arn" {
  description = "IAM role used by the bronze yellow crawler"
  value       = aws_iam_role.bronze_yellow_crawler.arn
}