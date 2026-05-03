#' @file column_templates.R
#' @use Standard column definitions for common MAMBA patterns
#' @principle MP032 (DRY), SO_R001 (Structure), DM_R001 (Data Standards)
#' @author Claude
#' @date 2025-12-24
#'
#' Reusable Column Template Specifications for ETL Tables
#'
#' This file provides pre-defined column specifications following the SCHEMA_001 pattern
#' for commonly used columns in MAMBA ETL pipelines. These templates ensure consistency
#' across tables and reduce the risk of misconfiguration.
#'
#' Usage:
#'   source("scripts/global_scripts/01_db/generate_create_table_query/column_templates.R")
#'   query <- generate_create_table_query(
#'     con = con,
#'     target_table = "sales_raw",
#'     column_defs = list(
#'       COLUMN_TEMPLATE_platform_id,
#'       list(name = "order_id", type = "VARCHAR(50)", not_null = TRUE),
#'       COLUMN_TEMPLATE_etl_timestamps$created_at
#'     )
#'   )

#### platform_id Column Template ####

#' Standard platform_id column definition
#'
#' Matches df_platform.csv schema with VARCHAR string codes (e.g., 'CBZ', 'AMZ', 'EBY').
#' This template should be used for all ETL tables to ensure consistent platform identification.
#'
#' Verified platform_ids:
#' - CBZ: Chubb platform (主要电商平台)
#' - AMZ: Amazon platform
#' - EBY: eBay platform
#'
#' @usage
#'   source("scripts/global_scripts/01_db/generate_create_table_query/column_templates.R")
#'
#'   # Use in column_defs list:
#'   query <- generate_create_table_query(
#'     con = con,
#'     target_table = "sales_raw",
#'     column_defs = list(
#'       COLUMN_TEMPLATE_platform_id,
#'       list(name = "order_id", type = "VARCHAR(50)", not_null = TRUE)
#'     )
#'   )
COLUMN_TEMPLATE_platform_id <- list(
  name = "platform_id",
  type = "VARCHAR(10)",
  not_null = TRUE,
  description = "Platform identifier matching df_platform.csv (e.g., 'CBZ', 'AMZ', 'EBY')",
  check = "length(platform_id) <= 10",
  example_values = c("CBZ", "AMZ", "EBY"),
  foreign_key = list(
    table = "df_platform",
    column = "platform_id",
    on_delete = "RESTRICT"
  ),
  notes = "Always use VARCHAR with explicit length, not INTEGER. This column must match platform_ids in df_platform.csv"
)

#### ETL Timestamp Columns Template ####

#' Standard timestamp columns for ETL tables
#'
#' Provides consistent timestamp column definitions for ETL pipelines.
#' Includes created_at (required) and updated_at (optional) columns.
#'
#' @usage
#'   source("scripts/global_scripts/01_db/generate_create_table_query/column_templates.R")
#'
#'   # Use individual timestamps:
#'   query <- generate_create_table_query(
#'     con = con,
#'     target_table = "sales_raw",
#'     column_defs = list(
#'       list(name = "order_id", type = "VARCHAR(50)", not_null = TRUE),
#'       COLUMN_TEMPLATE_etl_timestamps$created_at,
#'       COLUMN_TEMPLATE_etl_timestamps$updated_at
#'     )
#'   )
COLUMN_TEMPLATE_etl_timestamps <- list(

  created_at = list(
    name = "created_at",
    type = "TIMESTAMP",
    not_null = TRUE,
    default = "CURRENT_TIMESTAMP",
    description = "Record creation timestamp (UTC). Automatically set to current time.",
    notes = "Required for all ETL tables for audit trail and data lineage"
  ),

  updated_at = list(
    name = "updated_at",
    type = "TIMESTAMP",
    not_null = FALSE,
    description = "Last record update timestamp (UTC). NULL if never updated.",
    notes = "Optional. Use when tracking incremental updates to raw data"
  )

)

#### Helper Function: Apply Templates ####

#' Apply column templates to generate column definitions
#'
#' Convenience function to combine template columns with custom columns,
#' reducing repetitive code when building column_defs lists.
#'
#' @param templates List of template columns to include (e.g., COLUMN_TEMPLATE_platform_id)
#' @param custom_columns Additional custom column definitions as a list
#'
#' @return List of complete column definitions ready for generate_create_table_query()
#'
#' @examples
#' source("scripts/global_scripts/01_db/generate_create_table_query/column_templates.R")
#'
#' # Example: Create ETL table with platform_id and timestamps
#' columns <- apply_column_templates(
#'   templates = list(COLUMN_TEMPLATE_platform_id),
#'   custom_columns = list(
#'     list(name = "order_id", type = "VARCHAR(50)", not_null = TRUE),
#'     list(name = "amount", type = "DECIMAL(10,2)", not_null = TRUE),
#'     COLUMN_TEMPLATE_etl_timestamps$created_at
#'   )
#' )
#'
#' query <- generate_create_table_query(
#'   con = con,
#'   target_table = "sales_raw",
#'   column_defs = columns
#' )
apply_column_templates <- function(templates = list(), custom_columns = list()) {
  # Combine templates and custom columns
  combined <- c(templates, custom_columns)

  # Validate that all elements are lists with 'name' field
  if (!all(sapply(combined, function(x) is.list(x) && !is.null(x$name)))) {
    stop("All column definitions must be lists with a 'name' field")
  }

  return(combined)
}
