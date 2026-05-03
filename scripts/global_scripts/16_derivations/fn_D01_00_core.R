#' @title D01_00 Core Function - Consume ETL Output (Cross-Company)
#' @description Validates ETL-standardized sales tables before downstream derivations.
#' @param platform_id Character. Platform identifier (e.g., "cbz", "amz", "eby")
#' @param config Optional list. Platform config. If NULL, uses default table patterns.
#' @return List with success status and summary
#' @principle DM_R044, MP064, MP145, DEV_R037, DEV_R038

run_D01_00 <- function(platform_id, config = NULL) {
  if (missing(platform_id) || is.null(platform_id) || !nzchar(platform_id)) {
    stop("platform_id is required")
  }

  if (is.null(config)) {
    if (!exists("get_platform_config", mode = "function", inherits = TRUE)) {
      stop("get_platform_config() is required when config is NULL")
    }
    config <- get_platform_config(platform_id, warn = FALSE)
  }

  # ===========================================================================
  # PART 1: INITIALIZE
  # ===========================================================================

  connection_created_transformed <- FALSE
  state <- new.env(parent = emptyenv())
  state$error_occurred <- FALSE
  state$test_passed <- FALSE
  state$rows_found <- 0
  start_time <- Sys.time()
  drv_batch_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
  drv_script_name <- sprintf("%s_D01_00", platform_id)

  input_table_pattern <- if (!is.null(config$input_table_pattern)) {
    config$input_table_pattern
  } else {
    "df_%s_sales___standardized"
  }

  if (!exists("transformed_data") || !inherits(transformed_data, "DBIConnection")) {
    transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = TRUE)
    connection_created_transformed <- TRUE
  }

  # ===========================================================================
  # PART 2: MAIN
  # ===========================================================================

  tryCatch({
    required_table <- sprintf(input_table_pattern, platform_id)

    if (!DBI::dbExistsTable(transformed_data, required_table)) {
      stop(sprintf("Required input table %s not found in transformed_data", required_table))
    }

    columns <- DBI::dbListFields(transformed_data, required_table)
    required_cols <- c("customer_id", "payment_time", "lineproduct_price")
    missing_cols <- setdiff(required_cols, columns)
    if (length(missing_cols) > 0) {
      stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
    }

    if (!"platform_id" %in% columns) {
      message(sprintf("[%s] MAIN: platform_id column missing; will use platform_id value downstream", platform_id))
    }

    row_count <- DBI::dbGetQuery(
      transformed_data,
      sprintf("SELECT COUNT(*) AS n FROM %s", DBI::dbQuoteIdentifier(transformed_data, required_table))
    )$n
    state$rows_found <- as.integer(row_count)

    if (state$rows_found == 0) {
      stop("No rows found in input table")
    }

  }, error = function(e) {
    state$error_occurred <- TRUE
    message(sprintf("[%s] MAIN: ERROR - %s", platform_id, e$message))
  })

  # ===========================================================================
  # PART 3: TEST
  # ===========================================================================

  if (!state$error_occurred) {
    tryCatch({
      state$test_passed <- TRUE
      message(sprintf("[%s] TEST: Input table validated (%d rows)", platform_id, state$rows_found))
    }, error = function(e) {
      state$test_passed <- FALSE
      message(sprintf("[%s] TEST: ERROR - %s", platform_id, e$message))
    })
  }

  # ===========================================================================
  # PART 4: SUMMARIZE
  # ===========================================================================

  execution_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  summary_report <- list(
    success = !state$error_occurred && state$test_passed,
    platform_id = platform_id,
    rows_found = state$rows_found,
    execution_time_secs = execution_time,
    drv_script_name = drv_script_name,
    drv_batch_id = drv_batch_id
  )

  message(sprintf("[%s] SUMMARY: %s", platform_id, ifelse(summary_report$success, "SUCCESS", "FAILED")))
  message(sprintf("[%s] SUMMARY: Rows found: %d", platform_id, state$rows_found))
  message(sprintf("[%s] SUMMARY: Execution time (secs): %.2f", platform_id, execution_time))

  # ===========================================================================
  # PART 5: DEINITIALIZE
  # ===========================================================================

  if (connection_created_transformed && DBI::dbIsValid(transformed_data)) {
    DBI::dbDisconnect(transformed_data, shutdown = FALSE)
  }

  summary_report
}
