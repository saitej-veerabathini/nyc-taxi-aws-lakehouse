data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect = "Allow"
    actions = [
      "sts:AssumeRole"
    ]
    principals {
      type = "Service"
      identifiers = [
        "glue.amazonaws.com"
      ]
    }
  }
}

resource "aws_iam_role" "bronze_yellow_crawler" {
  name               = "AWSGlueServiceRole-${local.resource_prefix}-bronze-crawler"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  tags = merge(local.common_tags, {
    Layer   = "bronze"
    Purpose = "Glue crawler"
  })
}

resource "aws_iam_role_policy_attachment" "bronze_yellow_crawler_glue" {
  role       = aws_iam_role.bronze_yellow_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"

}

data "aws_iam_policy_document" "bronze_yellow_crawler_s3" {
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
    sid    = "ListBronzeYellowPrefix"
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
        "bronze/yellow/",
        "bronze/yellow/*"
      ]
    }
  }

  statement {
    sid    = "ReadBronzeYellowObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject"
    ]
    resources = [
      "${aws_s3_bucket.lakehouse.arn}/bronze/yellow/*"
    ]
  }
}

resource "aws_iam_role_policy" "bronze_yellow_crawler_s3" {
  name   = "${local.resource_prefix}-bronze-crawler-s3-read"
  role   = aws_iam_role.bronze_yellow_crawler.id
  policy = data.aws_iam_policy_document.bronze_yellow_crawler_s3.json
}

resource "aws_iam_role" "silver_yellow_crawler" {
  name               = "AWSGlueServiceRole-${local.resource_prefix}-silver-crawler"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  tags = merge(local.common_tags, {
    Layer   = "silver"
    Purpose = "Glue crawler"
  })
}

resource "aws_iam_role_policy_attachment" "silver_yellow_crawler_glue" {
  role       = aws_iam_role.silver_yellow_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "silver_yellow_crawler_s3" {
  statement {
    sid       = "ReadBucketMetadata"
    effect    = "Allow"
    resources = [aws_s3_bucket.lakehouse.arn]
    actions = [
      "s3:GetBucketLocation"
    ]
  }

  statement {
    sid    = "ListSilverYellowPrefix"
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
        "silver/yellow",
        "silver/yellow/",
        "silver/yellow/*"
      ]
    }
  }

  statement {
    sid    = "ReadSilverYellowObjects"
    effect = "Allow"
    resources = [
      "${aws_s3_bucket.lakehouse.arn}/silver/yellow/*"
    ]
    actions = [
      "s3:GetObject"
    ]
  }
}

resource "aws_iam_role_policy" "silver_yellow_crawler_s3" {
  name   = "${local.resource_prefix}-silver-crawler-s3-read"
  role   = aws_iam_role.silver_yellow_crawler.id
  policy = data.aws_iam_policy_document.silver_yellow_crawler_s3.json
}