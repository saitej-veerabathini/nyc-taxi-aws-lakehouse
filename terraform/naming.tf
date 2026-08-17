locals {
  project_name = "nyc-taxi"
  dataset_name = "yellow"

  #General AWS resource names
  resource_prefix = "${local.project_name}-${local.dataset_name}"

  #Glue/Athena SQL-compatible identifiers
  catalog_prefix = replace(local.resource_prefix, "-", "_")

  common_tags = {
    Project   = local.project_name
    Dataset   = local.dataset_name
    ManagedBy = "Terraform"
    Purpose   = "Learning"
  }
}