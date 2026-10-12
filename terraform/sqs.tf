resource "aws_sqs_queue" "bronze_events_dlq" {
  name                      = "nyc-taxi-yellow-bronze-events-dlq"
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "bronze_events" {
  name                       = "nyc-taxi-yellow-bronze-events"
  visibility_timeout_seconds = 3600
  message_retention_seconds  = 345600
  receive_wait_time_seconds  = 20
  sqs_managed_sse_enabled    = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.bronze_events_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_s3_bucket_notification" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  queue {
    queue_arn = aws_sqs_queue.bronze_events.arn
    events    = ["s3:ObjectCreated:*"]

    filter_prefix = "bronze/yellow/"
    filter_suffix = ".parquet"
  }

  depends_on = [
    aws_sqs_queue_policy.bronze_events
  ]
}