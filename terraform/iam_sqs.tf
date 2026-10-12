data "aws_iam_policy_document" "allow_s3_to_bronze_events" {
  statement {
    sid    = "AllowS3ToSendMessages"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    actions = [
      "sqs:SendMessage"
    ]

    resources = [
      aws_sqs_queue.bronze_events.arn
    ]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.lakehouse.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sqs_queue_policy" "bronze_events" {
  queue_url = aws_sqs_queue.bronze_events.id
  policy    = data.aws_iam_policy_document.allow_s3_to_bronze_events.json
}