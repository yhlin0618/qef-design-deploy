#' @title D01_08 Core: Pre-compute RSV Classification
#' @description
#' Pre-computes RSV (Risk-Stability-Value) classification and marketing
#' strategy assignment for all customers. This moves classify_rsv_and_strategy()
#' from app runtime to DRV time, eliminating 10-15s render delay for
#' rsvMatrix, marketingDecision, and customerExport components.
#'
#' This is part of the D01 (DNA/Customer) derivation group:
#' - D01_00 ~ D01_05: DNA and Customer Profile processing
#' - D01_06: Master Execution (orchestrates D01_00 ~ D01_05)
#' - D01_07: Pre-compute DNA Plot Data (ECDF, histograms, summaries)
#' - D01_08: Pre-compute RSV Classification (this script)
#'
#' PERFORMANCE IMPACT:
#' - Before: 3 components each query 100k+ rows + classify in R (10-15s each)
#' - After: 3 components read pre-computed table (~50ms each)
#'
#' @principles
#' - UX_P002: Fast Query Rendering (Tier 1 pre-computation)
#' - MP064: ETL-Derivation Separation (pre-compute in DRV phase)
#' - MP029: No Fake Data (uses real df_dna_by_customer data)
#' - DM_R044: Derivation Implementation Standard
#' - DEV_R038: Core Function + Platform Wrapper Pattern
#'
#' @created 2026-03-02
#' @author Claude Code

#####
# DERIVATION: D01_08 Pre-compute RSV Classification
# GROUP: D01 (Customer DNA Analysis)
# SEQUENCE: 08
# CORE_FUNCTION: global_scripts/16_derivations/fn_D01_08_core.R
# CONSUMES: app_data.df_dna_by_customer
# PRODUCES: app_data.df_rsv_classified
# DEPENDS_ON_DRV: D01_05 (df_dna_by_customer must exist)
# PRINCIPLE: UX_P002, MP064, DEV_R038
#####

# The 17 columns selected from df_dna_by_customer for RSV classification.
# Column aliases map DuckDB names to the names expected by classify_rsv_and_strategy().
RSV_SELECT_COLUMNS <- paste0(
  "customer_id, nrec_prob, cri AS cri_value, cri_ecdf, clv AS clv_value, nes_status,",
  " cai_value, nt AS ni_count, r_value, f_value, m_value, total_spent AS spent_total, ipt_mean,",
  " dna_r_score, dna_f_score, dna_m_score, be2 AS be2_prob"
)

#' Run D01_08: Pre-compute RSV Classification for one platform/product_line
#'
#' @param platform_id Character. Platform to process ('all' or specific ID)
#' @param product_line_id Character. Product line to process ('all' or specific ID)
#' @param config Optional configuration list
#' @return List with execution summary
#' @export
run_D01_08 <- function(platform_id = "all",
                       product_line_id = "all",
                       config = NULL) {

  start_time <- Sys.time()
  error_occurred <- FALSE
  test_passed <- FALSE
  rows_written <- 0L

  if (!exists("db_path_list", inherits = TRUE)) {
    stop("db_path_list not initialized. Run autoinit() first.")
  }

  app_data_path <- db_path_list$app_data
  if (!file.exists(app_data_path)) {
    stop(sprintf("App data database not found: %s", app_data_path))
  }

  message(sprintf("D01_08: Processing platform=%s, product_line_id=%s",
                  platform_id, product_line_id))

  # ===========================================================================
  # MAIN
  # ===========================================================================
  tryCatch({
    con <- dbConnectDuckdb(app_data_path, read_only = FALSE)
    on.exit({
      if (!is.null(con) && isTRUE(tryCatch(DBI::dbIsValid(con), error = function(e) FALSE))) {
        dbDisconnect(con, shutdown = TRUE)
      }
    }, add = TRUE)

    # Check source table
    if (!DBI::dbExistsTable(con, "df_dna_by_customer")) {
      stop("Source table df_dna_by_customer not found")
    }

    # Create output table if it doesn't exist
    if (!DBI::dbExistsTable(con, "df_rsv_classified")) {
      message("  Creating df_rsv_classified table...")
      DBI::dbExecute(con, "CREATE TABLE df_rsv_classified (
        platform_id VARCHAR,
        product_line_id_filter VARCHAR,
        customer_id VARCHAR,
        nrec_prob DOUBLE,
        cri_value DOUBLE,
        cri_ecdf DOUBLE,
        clv_value DOUBLE,
        nes_status VARCHAR,
        cai_value DOUBLE,
        ni_count INTEGER,
        r_value DOUBLE,
        f_value DOUBLE,
        m_value DOUBLE,
        spent_total DOUBLE,
        ipt_mean DOUBLE,
        dna_r_score DOUBLE,
        dna_f_score DOUBLE,
        dna_m_score DOUBLE,
        be2_prob DOUBLE,
        r_level VARCHAR,
        s_level VARCHAR,
        v_level VARCHAR,
        rsv_key VARCHAR,
        customer_type VARCHAR,
        rsv_action VARCHAR,
        rfm_score INTEGER,
        clv_level VARCHAR,
        marketing_strategy VARCHAR,
        marketing_purpose VARCHAR,
        marketing_recommendation VARCHAR,
        cai_text VARCHAR,
        created_at TIMESTAMP
      )")
    }

    # Build WHERE clause
    platform_filter <- if (platform_id != "all") {
      sprintf("platform_id = %s", DBI::dbQuoteString(con, platform_id))
    } else {
      "1=1"
    }

    has_product_line <- tryCatch({
      "product_line_id_filter" %in% DBI::dbListFields(con, "df_dna_by_customer")
    }, error = function(e) FALSE)

    product_line_filter <- if (has_product_line && product_line_id != "all") {
      sprintf("product_line_id_filter = %s", DBI::dbQuoteString(con, product_line_id))
    } else {
      "1=1"
    }

    where_clause <- sprintf("%s AND %s", platform_filter, product_line_filter)

    # Query source data
    sql <- sprintf("SELECT %s FROM df_dna_by_customer WHERE %s",
                   RSV_SELECT_COLUMNS, where_clause)
    df <- DBI::dbGetQuery(con, sql)

    if (nrow(df) == 0) {
      message("  No data for this combination, skipping.")
      return(invisible(list(
        platform_id = platform_id,
        product_line_id = product_line_id,
        success = TRUE,
        rows_written = 0L,
        execution_time_secs = as.numeric(difftime(Sys.time(), start_time, units = "secs"))
      )))
    }

    message(sprintf("  Loaded %d records from df_dna_by_customer", nrow(df)))

    # Run classification
    df <- classify_rsv_and_strategy(df)

    # Add key columns
    df$platform_id <- platform_id
    df$product_line_id_filter <- product_line_id
    df$created_at <- Sys.time()

    # Delete existing data for this combination (DELETE + INSERT pattern)
    delete_sql <- sprintf(
      "DELETE FROM df_rsv_classified WHERE platform_id = %s AND product_line_id_filter = %s",
      DBI::dbQuoteString(con, platform_id),
      DBI::dbQuoteString(con, product_line_id)
    )
    DBI::dbExecute(con, delete_sql)

    # Insert new data
    DBI::dbWriteTable(con, "df_rsv_classified", df, append = TRUE)
    rows_written <- nrow(df)

    message(sprintf("  Wrote %d classified records", rows_written))

    # Explicitly close the write connection before TEST opens a read connection
    try(dbDisconnect(con, shutdown = TRUE), silent = TRUE)
    con <- NULL

  }, error = function(e) {
    message(sprintf("ERROR in D01_08 MAIN: %s", e$message))
    error_occurred <<- TRUE
  })

  # ===========================================================================
  # TEST
  # ===========================================================================
  if (!error_occurred) {
    tryCatch({
      message("  Validating output...")

      con <- dbConnectDuckdb(app_data_path, read_only = TRUE)
      on.exit({
        if (!is.null(con) && isTRUE(tryCatch(DBI::dbIsValid(con), error = function(e) FALSE))) {
          dbDisconnect(con, shutdown = TRUE)
        }
      }, add = TRUE)

      validation_sql <- sprintf(
        "SELECT COUNT(*) AS n FROM df_rsv_classified WHERE platform_id = %s AND product_line_id_filter = %s",
        DBI::dbQuoteString(con, platform_id),
        DBI::dbQuoteString(con, product_line_id)
      )
      result <- DBI::dbGetQuery(con, validation_sql)

      if (result$n > 0) {
        message(sprintf("    Validation PASSED: %d rows in df_rsv_classified", result$n))
        test_passed <- TRUE
      } else {
        message("    Validation WARNING: 0 rows (may be expected for some combinations)")
        test_passed <- TRUE
      }

    }, error = function(e) {
      message(sprintf("ERROR in D01_08 TEST: %s", e$message))
      test_passed <<- FALSE
    })
  }

  # ===========================================================================
  # SUMMARIZE
  # ===========================================================================
  end_time <- Sys.time()
  execution_time <- as.numeric(difftime(end_time, start_time, units = "secs"))

  summary <- list(
    platform_id = platform_id,
    product_line_id = product_line_id,
    success = test_passed && !error_occurred,
    rows_written = rows_written,
    execution_time_secs = execution_time
  )

  if (summary$success) {
    message(sprintf("  D01_08 completed: %d rows (%.2fs)",
                    rows_written, execution_time))
  } else {
    message(sprintf("  D01_08 FAILED for platform=%s, product_line_id=%s",
                    platform_id, product_line_id))
  }

  invisible(summary)
}


#' Run D01_08 for all platforms
#'
#' @description
#' Processes all enabled platforms plus 'all'.
#' For each platform, also processes 'all' plus each available product_line_id.
#' Reuses the same enumeration pattern as D01_07.
#'
#' @param enabled_platforms Character vector. Platforms to process.
#' @param config Optional configuration
#' @return List of results for each platform/product_line combination
#' @export
run_D01_08_all_platforms <- function(enabled_platforms = c("cbz"),
                                     config = NULL) {
  platforms_to_run <- unique(c("all", enabled_platforms))

  message("════════════════════════════════════════════════════════════════════")
  message("D01_08: Pre-compute RSV Classification")
  message("════════════════════════════════════════════════════════════════════")
  message(sprintf("Platforms: %s", paste(platforms_to_run, collapse = ", ")))
  message("")

  results <- list()

  if (!exists("db_path_list", inherits = TRUE)) {
    stop("db_path_list not initialized. Run autoinit() first.")
  }

  app_data_path <- db_path_list$app_data

  # Connect once to discover product_line_id values
  con <- dbConnectDuckdb(app_data_path, read_only = TRUE)
  on.exit({
    if (!is.null(con)) {
      try(dbDisconnect(con, shutdown = TRUE), silent = TRUE)
    }
  }, add = TRUE)

  has_product_line <- tryCatch({
    DBI::dbExistsTable(con, "df_dna_by_customer") &&
      "product_line_id_filter" %in% DBI::dbListFields(con, "df_dna_by_customer")
  }, error = function(e) FALSE)

  get_product_lines <- function(platform_id) {
    if (!has_product_line) return("all")

    where_clause <- if (!is.null(platform_id) && platform_id != "all") {
      sprintf("WHERE platform_id = %s", DBI::dbQuoteString(con, platform_id))
    } else {
      ""
    }

    sql <- sprintf(
      "SELECT DISTINCT product_line_id_filter AS product_line_id FROM df_dna_by_customer %s",
      where_clause
    )

    ids <- tryCatch(DBI::dbGetQuery(con, sql)$product_line_id, error = function(e) character(0))
    ids <- ids[!is.na(ids) & ids != ""]
    unique(c("all", ids))
  }

  platform_product_lines <- setNames(vector("list", length(platforms_to_run)), platforms_to_run)
  for (plat in platforms_to_run) {
    platform_product_lines[[plat]] <- get_product_lines(plat)
  }

  # Close discovery connection before write operations
  try(dbDisconnect(con, shutdown = TRUE), silent = TRUE)
  con <- NULL

  for (plat in platforms_to_run) {
    message(sprintf("Processing platform: %s", plat))
    product_lines <- platform_product_lines[[plat]]

    for (pl_id in product_lines) {
      message(sprintf("  product_line_id: %s", pl_id))

      result <- tryCatch({
        run_D01_08(platform_id = plat, product_line_id = pl_id, config = config)
      }, error = function(e) {
        warning(sprintf("D01_08 failed for %s/%s: %s", plat, pl_id, e$message))
        list(platform_id = plat, product_line_id = pl_id, success = FALSE, error = e$message)
      })

      result_key <- paste(plat, pl_id, sep = ":")
      results[[result_key]] <- result
    }
  }

  # Summary
  message("")
  message("════════════════════════════════════════════════════════════════════")
  message("D01_08 Summary")
  message("════════════════════════════════════════════════════════════════════")

  success_count <- sum(vapply(results, function(x) isTRUE(x$success), logical(1)))
  total_rows <- sum(vapply(results, function(x) {
    if (is.numeric(x$rows_written)) x$rows_written else 0L
  }, numeric(1)))
  message(sprintf("Completed: %d/%d combinations successful, %d total rows",
                  success_count, length(results), total_rows))

  invisible(results)
}
