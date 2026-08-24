locals {
  glue_scripts_directory = "${path.module}/../src/glue"

  glue_script_files = fileset(
    local.glue_scripts_directory,
    "**/*.py"
  )
}

resource "aws_s3_object" "glue_scripts" {
  for_each = local.glue_script_files

  bucket = aws_s3_bucket.lakehouse.id
  key    = "scripts/glue/${each.value}"

  source = "${local.glue_scripts_directory}/${each.value}"

  source_hash = filemd5(
    "${local.glue_scripts_directory}/${each.value}"
  )

  content_type = "text/x-python"
}