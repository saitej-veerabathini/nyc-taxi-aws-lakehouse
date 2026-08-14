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