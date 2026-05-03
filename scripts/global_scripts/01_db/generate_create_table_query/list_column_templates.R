#' @file list_column_templates.R
#' @use Standard column definitions for common MAMBA ETL patterns
#' @principle MP032 (DRY), SO_R001 (Structure), DM_R001 (Data Standards)
#' @principle SO_R002 (File Naming - list_ prefix for list object definition files)
#' @author Claude
#' @date 2025-12-24
#'
#' Reusable Column Template Specifications
#'
#' This file provides pre-defined column specifications as list objects following
#' the SCHEMA_001 pattern for commonly used columns in MAMBA ETL pipelines.
#' These templates ensure consistency across tables and reduce misconfiguration risk.
#'
#' Naming Convention:
#' - File: list_column_templates.R (list_ prefix indicates list object definitions)
#' - Objects: list_column_template_* (lowercase with underscores per R conventions)
#'
#' Usage:
#'   source("scripts/global_scripts/01_db/generate_create_table_query/list_column_templates.R")
#'
#'   query <- generate_create_table_query(
#'     con = con,
#'     target_table = "sales_raw",
#'     column_defs = list(
#'       list_column_template_platform_id,
#'       list(name = "order_id", type = "VARCHAR(50)", not_null = TRUE),
#'       list_column_template_etl_timestamps$created_at
#'     )
#'   )

#### Standard platform_id Column ####

#' Standard platform_id column definition
#'
#' Matches df_platform.csv schema with VARCHAR string codes (e.g., 'CBZ', 'AMZ', 'EBY').
#' This template should be used for all ETL tables to ensure consistent platform identification.
#'
#' Verified platform_ids:
#' - CBZ: Chubb platform (主要电商平台)
#' - AMZ: Amazon platform
#' - EBY: eBay platform
list_column_template_platform_id <- list(
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

#### ETL Timestamp Columns ####

#' Standard timestamp columns for ETL tables
#'
#' Provides consistent timestamp column definitions for ETL pipelines.
#' Includes created_at (required) and updated_at (optional) components.
list_column_template_etl_timestamps <- list(

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
