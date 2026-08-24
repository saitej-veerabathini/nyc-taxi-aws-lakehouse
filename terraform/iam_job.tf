resource "aws_iam_role" "yellow_etl_job" {
  name = "AWSGlueServiceRole-${local.resource_prefix}-etl"

  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json

  tags = merge(local.common_tags, {
    Purpose = "Glue ETL job"
  })
}

resource "aws_iam_role_policy_attachment" "yellow_etl_job_glue" {
  role       = aws_iam_role.yellow_etl_job.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "yellow_etl_job_s3" {
  statement {
    sid    = "ReadBucketMetadata"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation"
    ]

    resources = [
      aws_s3_bucket.lakehouse.arn
    ]
  }

  statement {
    sid    = "ListRequiredPrefixes"
    effect = "Allow"

    actions = [
      "s3:ListBucket"
    ]

    resources = [
      aws_s3_bucket.lakehouse.arn
    ]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"

      values = [
        "bronze/yellow",
        "bronze/yellow/*",
        "silver/yellow",
        "silver/yellow/*",
        "scripts/glue",
        "scripts/glue/*",
        "temp",
        "temp/*"
      ]
    }
  }

  statement {
    sid    = "ReadBronzeAndScripts"
    effect = "Allow"

    actions = [
      "s3:GetObject"
    ]

    resources = [
      "${aws_s3_bucket.lakehouse.arn}/bronze/yellow/*",
      "${aws_s3_bucket.lakehouse.arn}/scripts/glue/*"
    ]
  }

  statement {
    sid    = "WriteSilverAndTemporaryData"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = [
      "${aws_s3_bucket.lakehouse.arn}/silver/yellow/*",
      "${aws_s3_bucket.lakehouse.arn}/temp/*"
    ]
  }
}

resource "aws_iam_role_policy" "yellow_etl_job_s3" {
  name   = "${local.resource_prefix}-etl-s3-access"
  role   = aws_iam_role.yellow_etl_job.id
  policy = data.aws_iam_policy_document.yellow_etl_job_s3.json
}