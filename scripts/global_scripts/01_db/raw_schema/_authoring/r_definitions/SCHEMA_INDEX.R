# =============================================================================
# MAMBA-WISER DATA SCHEMA INDEX
# =============================================================================
# Purpose: Master index of all data schemas for MAMBA-WISER synchronization
# Version: 1.0
# Created: 2025-01-28
# =============================================================================

# =============================================================================
# OVERVIEW
# =============================================================================
#
# This index documents the complete data schema specifications for the
# MAMBA-WISER ecosystem. These schemas ensure data compatibility and
# consistency between the two applications.
#
# MAMBA Database Location:
# /Users/che/Library/CloudStorage/Dropbox/che_workspace/projects/ai_martech/l4_enterprise/MAMBA/data/app_data/app_data.duckdb
#
# WISER Database Location:
# /Users/che/Library/CloudStorage/Dropbox/che_workspace/projects/ai_martech/l4_enterprise/WISER/data/app_data/
#
# =============================================================================

# =============================================================================
# SCHEMA CATALOG
# =============================================================================

SCHEMA_CATALOG <- list(

  # ---------------------------------------------------------------------------
  # SCHEMA_001: Poisson Analysis Tables
  # ---------------------------------------------------------------------------
  poisson_analysis = list(
    file = "SCHEMA_001_poisson_analysis.R",
    tables = c(
      "df_cbz_poisson_analysis_alf",
      "df_cbz_poisson_analysis_all",
      "df_cbz_poisson_analysis_irf",
      "df_cbz_poisson_analysis_pre",
      "df_cbz_poisson_analysis_rek",
      "df_cbz_poisson_analysis_tur",
      "df_cbz_poisson_analysis_wak",
      "df_eby_poisson_analysis_*"
    ),
    description = "Stores Poisson regression coefficients and model statistics",
    critical_columns = c("coefficient", "incidence_rate_ratio", "p_value"),
    usage = "Time series analysis, sales prediction models"
  ),

  # ---------------------------------------------------------------------------
  # SCHEMA_002: Position Analysis Table
  # ---------------------------------------------------------------------------
  position_analysis = list(
    file = "SCHEMA_002_position_analysis.R",
    tables = "df_position",
    description = "Product positioning based on comment property ratings",
    critical_columns = c("product_line_id", "product_id", "brand"),
    comment_properties = 33, # Number of Chinese comment property columns
    usage = "Competitive positioning, radar charts, market analysis"
  ),

  # ---------------------------------------------------------------------------
  # SCHEMA_003: Customer Profile and DNA
  # ---------------------------------------------------------------------------
  customer_data = list(
    file = "SCHEMA_003_customer_profile.R",
    tables = c("df_profile_by_customer", "df_dna_by_customer"),
    description = "Customer identification and behavioral DNA metrics",
    critical_columns = c("customer_id", "platform_id", "r_value", "f_value", "m_value"),
    rfm_metrics = TRUE,
    advanced_metrics = c("clv", "cai", "cri", "pcv"),
    usage = "Customer segmentation, lifetime value analysis, RFM analysis"
  ),

  # ---------------------------------------------------------------------------
  # SCHEMA_004: Time Series Sales Data
  # ---------------------------------------------------------------------------
  time_series = list(
    file = "SCHEMA_004_time_series.R",
    tables = c(
      "df_cbz_sales_complete_time_series",
      "df_cbz_sales_complete_time_series_alf",
      "df_cbz_sales_complete_time_series_irf",
      "df_cbz_sales_complete_time_series_pre",
      "df_cbz_sales_complete_time_series_rek",
      "df_cbz_sales_complete_time_series_tur",
      "df_cbz_sales_complete_time_series_wak"
    ),
    description = "Enriched time series sales data with temporal features",
    critical_columns = c("time", "sales", "year", "day"),
    temporal_features = c("month_1-12", "monday-sunday"),
    enrichment_columns = 150, # Approximate number in enriched tables
    usage = "Input data for Poisson regression, time series forecasting"
  ),

  # ---------------------------------------------------------------------------
  # Additional Tables (Not yet schematized)
  # ---------------------------------------------------------------------------
  metadata = list(
    tables = c("df_cbz_time_frame_complete", "df_time_range"),
    description = "Time frame and range metadata tables",
    status = "Pending documentation"
  )
)

# =============================================================================
# SCHEMA LOADING FUNCTIONS
# =============================================================================

# Load all schema definitions
load_all_schemas <- function(base_path = NULL) {
  if (is.null(base_path)) {
    # Schema authoring source-of-truth relocated 2026-04-27 per amended MP102 + MP156
    # (spectra change glue-layer-prerawdata-bridge, issue #489).
    base_path <- "scripts/global_scripts/01_db/raw_schema/_authoring/r_definitions"
  }

  schemas <- list()

  # Load each schema file
  for (schema in SCHEMA_CATALOG) {
    if (!is.null(schema$file)) {
      file_path <- file.path(base_path, schema$file)
      if (file.exists(file_path)) {
        source(file_path)
        schemas[[schema$file]] <- TRUE
      } else {
        warning(paste("Schema file not found:", file_path))
        schemas[[schema$file]] <- FALSE
      }
    }
  }

  return(schemas)
}

# =============================================================================
# VALIDATION ORCHESTRATOR
# =============================================================================

validate_database_schemas <- function(con) {
  # Validate all schemas in a database connection

  validation_results <- list()

  # Validate Poisson analysis tables
  poisson_tables <- grep("poisson_analysis", dbListTables(con), value = TRUE)
  for (table in poisson_tables) {
    validation_results[[table]] <- validate_poisson_analysis_table(con, table)
  }

  # Validate position table
  if (dbExistsTable(con, "df_position")) {
    validation_results[["df_position"]] <- validate_position_table(con, "df_position")
  }

  # Validate customer tables
  validation_results[["customer"]] <- validate_customer_tables(con)

  # Validate time series tables
  ts_tables <- grep("sales_complete_time_series", dbListTables(con), value = TRUE)
  for (table in ts_tables) {
    validation_results[[table]] <- validate_time_series_table(con, table)
  }

  # Summary
  total_tables <- length(validation_results)
  valid_tables <- sum(sapply(validation_results, function(x) x$valid))

  cat("\n=== SCHEMA VALIDATION SUMMARY ===\n")
  cat(sprintf("Total tables validated: %d\n", total_tables))
  cat(sprintf("Valid tables: %d\n", valid_tables))
  cat(sprintf("Invalid tables: %d\n", total_tables - valid_tables))

  # Report issues
  for (name in names(validation_results)) {
    if (!validation_results[[name]]$valid) {
      cat(sprintf("\n[ERROR] %s: %s\n", name, validation_results[[name]]$error))
      if (!is.null(validation_results[[name]]$missing_columns)) {
        cat("  Missing columns:", paste(validation_results[[name]]$missing_columns, collapse = ", "), "\n")
      }
    }
  }

  return(validation_results)
}

# =============================================================================
# ETL PIPELINE TEMPLATES
# =============================================================================

# Template for creating Poisson analysis data
etl_poisson_pipeline <- function(con_source, con_target, product_line, platform = "cbz") {

  # 1. Extract time series data
  ts_table <- sprintf("df_%s_sales_complete_time_series_%s", platform, product_line)

  if (!dbExistsTable(con_source, ts_table)) {
    stop(paste("Source table not found:", ts_table))
  }

  ts_data <- dbGetQuery(con_source, sprintf("SELECT * FROM %s", ts_table))

  # 2. Run Poisson regression
  # (Model fitting code would go here)

  # 3. Prepare results using schema function
  # results_df <- prepare_poisson_analysis_data(model, product_line, platform)

  # 4. Write to target database
  target_table <- sprintf("df_%s_poisson_analysis_%s", platform, product_line)
  # dbWriteTable(con_target, target_table, results_df, overwrite = TRUE)

  message(paste("ETL completed for", target_table))
}

# =============================================================================
# USAGE EXAMPLES
# =============================================================================

# Example 1: Connect to MAMBA and validate schemas
example_validate_mamba <- function() {
  library(DBI)
  library(duckdb)

  # Connect to MAMBA database
  con <- dbConnect(
    duckdb::duckdb(),
    "/Users/che/Library/CloudStorage/Dropbox/che_workspace/projects/ai_martech/l4_enterprise/MAMBA/data/app_data/app_data.duckdb",
    read_only = TRUE
  )

  # Validate schemas
  results <- validate_database_schemas(con)

  # Disconnect
  dbDisconnect(con)

  return(results)
}

# Example 2: Check for missing coefficient columns
example_check_coefficient <- function() {
  library(DBI)
  library(duckdb)

  con <- dbConnect(
    duckdb::duckdb(),
    "data/app_data/wiser.duckdb"
  )

  # Check all Poisson tables for coefficient column
  poisson_tables <- grep("poisson", dbListTables(con), value = TRUE)

  for (table in poisson_tables) {
    cols <- dbListFields(con, table)
    has_coefficient <- "coefficient" %in% cols

    cat(sprintf("%s: coefficient column %s\n",
                table,
                ifelse(has_coefficient, "EXISTS", "MISSING")))
  }

  dbDisconnect(con)
}

# Example 3: Create missing tables based on schema
example_create_tables <- function() {
  library(DBI)
  library(duckdb)

  con <- dbConnect(duckdb::duckdb(), "data/app_data/wiser.duckdb")

  # Create Poisson analysis table
  dbExecute(con, SCHEMA_poisson_analysis$examples$create_table)

  # Create position table
  dbExecute(con, SCHEMA_position_analysis$examples$create_table)

  dbDisconnect(con)
}

# =============================================================================
# TROUBLESHOOTING GUIDE
# =============================================================================

# Common issues and solutions:
#
# 1. Missing 'coefficient' column error:
#    - Check schema using validate_poisson_analysis_table()
#    - Ensure source data includes all required columns
#    - Re-run ETL pipeline with updated schema
#
# 2. Comment properties in wrong encoding:
#    - Ensure database connection uses UTF-8
#    - Check locale settings: Sys.setlocale("LC_ALL", "zh_TW.UTF-8")
#
# 3. Referential integrity violations:
#    - Run validate_customer_tables() to check relationships
#    - Ensure df_profile_by_customer is populated before df_dna_by_customer
#
# 4. Time series data gaps:
#    - Use validate_time_series_table() to check data quality
#    - Fill missing dates with zero sales if needed for analysis
#
# =============================================================================

message("MAMBA-WISER Schema Index loaded successfully")
message("Use load_all_schemas() to load all schema definitions")
message("Use validate_database_schemas(con) to validate a database")