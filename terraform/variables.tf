variable "aws_region" {
  description = "AWS region where the lakehouse will be deployed"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Name used to identify project resources"
  type        = string
  default     = "nyc-taxi-aws-lakehouse"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket for the NYC Taxi Lakehouse"
  type        = string
  default     = "nyc-taxi-aws-lakehouse-us-east-1"

}