#' @title Create Pre-computed DNA Plot Data Tables
#' @description Creates tables for storing pre-computed ECDF, histogram, and
#'              summary statistics data for DNA Distribution Analysis.
#'              This moves computation from app runtime to ETL time.
#'
#' @principles
#' - MP055: Special Treatment of 'ALL' Category
#' - MP064: ETL-Derivation Separation (pre-compute in DRV phase)
#' - P77: Performance Optimization
#'
#' @created 2026-01-26
#' @author Claude Code

#' Create df_dna_plot_data table
#'
#' @description
#' Stores pre-computed ECDF curves and histogram data for DNA distribution
#' visualization. This table replaces the need to load raw data and compute
#' distributions at app runtime.
#'
#' Table Schema:
#' - platform_id: 'all' or specific platform ('amz', 'eby', 'cbz', etc.)
#' - product_line_id: 'all' or specific product line ID
#' - metric: 'm_value', 'r_value', 'f_value', 'ipt_mean'
#' - chart_type: 'ecdf', 'histogram'
#' - x_value: X-axis value
#' - y_value: Y-axis value (cumulative % for ECDF, count for histogram)
#' - row_order: Ordering for sequential access
#' - total_count: Total records in this group
#' - created_at: Timestamp of data generation
#'
#' @param conn DBI connection to app_data.duckdb
#' @param drop_if_exists Logical. If TRUE, drops existing table first.
#' @return Logical indicating success
#' @export
create_df_dna_plot_data <- function(conn, drop_if_exists = FALSE) {
  if (!inherits(conn, "DBIConnection")) {
    stop("Invalid database connection")
  }

  table_name <- "df_dna_plot_data"

  if (drop_if_exists && DBI::dbExistsTable(conn, table_name)) {
    DBI::dbRemoveTable(conn, table_name)
    message(sprintf("  Dropped existing table: %s", table_name))
  }

  sql <- "
    CREATE TABLE IF NOT EXISTS df_dna_plot_data (
      platform_id       VARCHAR NOT NULL,
      product_line_id   VARCHAR NOT NULL DEFAULT 'all',
      metric            VARCHAR NOT NULL,
      chart_type        VARCHAR NOT NULL,
      x_value           DOUBLE,
      y_value           DOUBLE,
      row_order         INTEGER,
      total_count       INTEGER,
      created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (platform_id, product_line_id, metric, chart_type, row_order)
    )
  "

  tryCatch({
    DBI::dbExecute(conn, sql)
    message(sprintf("  Created table: %s", table_name))
    TRUE
  }, error = function(e) {
    warning(sprintf("Failed to create %s: %s", table_name, e$message))
    FALSE
  })
}

#' Create df_dna_category_counts table
#'
#' @description
#' Stores pre-aggregated category counts for bar charts in DNA distribution.
#' Used for categorical fields like nes_status and f_value bar charts.
#'
#' @param conn DBI connection to app_data.duckdb
#' @param drop_if_exists Logical. If TRUE, drops existing table first.
#' @return Logical indicating success
#' @export
create_df_dna_category_counts <- function(conn, drop_if_exists = FALSE) {
  if (!inherits(conn, "DBIConnection")) {
    stop("Invalid database connection")
  }

  table_name <- "df_dna_category_counts"

  if (drop_if_exists && DBI::dbExistsTable(conn, table_name)) {
    DBI::dbRemoveTable(conn, table_name)
    message(sprintf("  Dropped existing table: %s", table_name))
  }

  sql <- "
    CREATE TABLE IF NOT EXISTS df_dna_category_counts (
      platform_id       VARCHAR NOT NULL,
      product_line_id   VARCHAR NOT NULL DEFAULT 'all',
      category_field    VARCHAR NOT NULL,
      category_value    VARCHAR NOT NULL,
      count             INTEGER,
      percentage        DOUBLE,
      created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (platform_id, product_line_id, category_field, category_value)
    )
  "

  tryCatch({
    DBI::dbExecute(conn, sql)
    message(sprintf("  Created table: %s", table_name))
    TRUE
  }, error = function(e) {
    warning(sprintf("Failed to create %s: %s", table_name, e$message))
    FALSE
  })
}

#' Create df_dna_summary_stats table
#'
#' @description
#' Stores pre-computed summary statistics (mean, median, sd, quartiles)
#' for each metric in DNA distribution analysis.
#'
#' @param conn DBI connection to app_data.duckdb
#' @param drop_if_exists Logical. If TRUE, drops existing table first.
#' @return Logical indicating success
#' @export
create_df_dna_summary_stats <- function(conn, drop_if_exists = FALSE) {
  if (!inherits(conn, "DBIConnection")) {
    stop("Invalid database connection")
  }

  table_name <- "df_dna_summary_stats"

  if (drop_if_exists && DBI::dbExistsTable(conn, table_name)) {
    DBI::dbRemoveTable(conn, table_name)
    message(sprintf("  Dropped existing table: %s", table_name))
  }

  sql <- "
    CREATE TABLE IF NOT EXISTS df_dna_summary_stats (
      platform_id       VARCHAR NOT NULL,
      product_line_id   VARCHAR NOT NULL DEFAULT 'all',
      metric            VARCHAR NOT NULL,
      n                 INTEGER,
      mean_val          DOUBLE,
      median_val        DOUBLE,
      sd_val            DOUBLE,
      min_val           DOUBLE,
      max_val           DOUBLE,
      q1_val            DOUBLE,
      q3_val            DOUBLE,
      created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (platform_id, product_line_id, metric)
    )
  "

  tryCatch({
    DBI::dbExecute(conn, sql)
    message(sprintf("  Created table: %s", table_name))
    TRUE
  }, error = function(e) {
    warning(sprintf("Failed to create %s: %s", table_name, e$message))
    FALSE
  })
}

#' Create all DNA plot data tables
#'
#' @description
#' Convenience function to create all three pre-computed DNA plot tables.
#'
#' @param conn DBI connection to app_data.duckdb
#' @param drop_if_exists Logical. If TRUE, drops existing tables first.
#' @return Logical indicating all tables created successfully
#' @export
create_all_dna_plot_tables <- function(conn, drop_if_exists = FALSE) {
  message("[DNA Plot Tables] Creating pre-computed tables...")

  results <- c(
    create_df_dna_plot_data(conn, drop_if_exists),
    create_df_dna_category_counts(conn, drop_if_exists),
    create_df_dna_summary_stats(conn, drop_if_exists)
  )

  if (all(results)) {
    message("  All DNA plot tables created successfully")
  } else {
    warning("  Some tables failed to create")
  }

  all(results)
}

#' Verify DNA plot tables exist and have expected structure
#'
#' @param conn DBI connection to app_data.duckdb
#' @return List with verification results
#' @export
verify_dna_plot_tables <- function(conn) {
  tables <- c("df_dna_plot_data", "df_dna_category_counts", "df_dna_summary_stats")

  results <- list()

  for (tbl in tables) {
    exists <- DBI::dbExistsTable(conn, tbl)
    row_count <- if (exists) {
      DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) as n FROM %s", tbl))$n[1]
    } else {
      NA
    }

    results[[tbl]] <- list(
      exists = exists,
      row_count = row_count
    )
  }

  results
}
