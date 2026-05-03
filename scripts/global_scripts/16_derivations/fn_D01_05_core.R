#' @title D01_05 Core Function - Final Validation (Cross-Company)
#' @description Validates D01 app_data outputs for a given platform.
#' @param platform_id Character. Platform identifier (e.g., "cbz", "amz", "eby", "shopify")
#' @param config Optional list. Platform config.
#' @return List with success status and summary
#' @principle DEV_R038, DEV_R037, DM_R044, MP064, MP144

run_D01_05 <- function(platform_id, config = NULL) {
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

  connection_created_app <- FALSE

  error_occurred <- FALSE
  test_passed <- FALSE
  rows_processed <- 0
  start_time <- Sys.time()

  required_tables <- if (!is.null(config$required_tables) && length(config$required_tables) > 0) {
    config$required_tables
  } else {
    c("df_profile_by_customer", "df_dna_by_customer", "df_segments_by_customer")
  }

  required_dna_columns <- c("customer_id", "platform_id", "product_line_id_filter")
  required_profile_columns <- c("customer_id", "platform_id")
  required_segments_columns <- c("customer_id", "platform_id", "product_line_id_filter")

  if (!exists("app_data") || !inherits(app_data, "DBIConnection")) {
    app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = TRUE)
    connection_created_app <- TRUE
  }

  # ===========================================================================
  # PART 2: MAIN
  # ===========================================================================

  tryCatch({
    for (table_name in required_tables) {
      if (!DBI::dbExistsTable(app_data, table_name)) {
        stop(sprintf("Missing app_data table: %s", table_name))
      }
    }

    dna_rows <- dplyr::tbl(app_data, "df_dna_by_customer") |>
      dplyr::filter(platform_id == !!platform_id) |>
      dplyr::count() |>
      dplyr::collect()
    profile_rows <- dplyr::tbl(app_data, "df_profile_by_customer") |>
      dplyr::filter(platform_id == !!platform_id) |>
      dplyr::count() |>
      dplyr::collect()
    segments_rows <- dplyr::tbl(app_data, "df_segments_by_customer") |>
      dplyr::filter(platform_id == !!platform_id) |>
      dplyr::count() |>
      dplyr::collect()

    dna_count <- if (nrow(dna_rows) == 0) 0L else as.integer(dna_rows$n[[1]])
    profile_count <- if (nrow(profile_rows) == 0) 0L else as.integer(profile_rows$n[[1]])
    segments_count <- if (nrow(segments_rows) == 0) 0L else as.integer(segments_rows$n[[1]])

    if (dna_count <= 0) {
      stop(sprintf("No DNA rows found for platform_id=%s", platform_id))
    }
    if (profile_count <= 0) {
      stop(sprintf("No profile rows found for platform_id=%s", platform_id))
    }
    if (segments_count <= 0) {
      stop(sprintf("No segment rows found for platform_id=%s", platform_id))
    }

    dna_sample <- dplyr::tbl(app_data, "df_dna_by_customer") |>
      dplyr::filter(platform_id == !!platform_id) |>
      head(1) |>
      dplyr::collect()
    profile_sample <- dplyr::tbl(app_data, "df_profile_by_customer") |>
      dplyr::filter(platform_id == !!platform_id) |>
      head(1) |>
      dplyr::collect()
    segments_sample <- dplyr::tbl(app_data, "df_segments_by_customer") |>
      dplyr::filter(platform_id == !!platform_id) |>
      head(1) |>
      dplyr::collect()

    missing_dna_cols <- setdiff(required_dna_columns, names(dna_sample))
    if (length(missing_dna_cols) > 0) {
      stop(sprintf("Missing required columns in df_dna_by_customer: %s", paste(missing_dna_cols, collapse = ", ")))
    }

    missing_profile_cols <- setdiff(required_profile_columns, names(profile_sample))
    if (length(missing_profile_cols) > 0) {
      stop(sprintf("Missing required columns in df_profile_by_customer: %s", paste(missing_profile_cols, collapse = ", ")))
    }

    missing_segments_cols <- setdiff(required_segments_columns, names(segments_sample))
    if (length(missing_segments_cols) > 0) {
      stop(sprintf("Missing required columns in df_segments_by_customer: %s", paste(missing_segments_cols, collapse = ", ")))
    }

    rows_processed <- dna_count

  }, error = function(e) {
    error_occurred <<- TRUE
    message(sprintf("[%s] MAIN: ERROR - %s", platform_id, e$message))
  })

  # ===========================================================================
  # PART 3: TEST
  # ===========================================================================

  if (!error_occurred) {
    tryCatch({
      test_passed <- TRUE
      message(sprintf("[%s] TEST: Final validation passed", platform_id))
    }, error = function(e) {
      test_passed <<- FALSE
      message(sprintf("[%s] TEST: ERROR - %s", platform_id, e$message))
    })
  }

  # ===========================================================================
  # PART 4: SUMMARIZE
  # ===========================================================================

  execution_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  summary_report <- list(
    success = !error_occurred && test_passed,
    platform_id = platform_id,
    rows_processed = rows_processed,
    execution_time_secs = execution_time,
    outputs = c("validation_result")
  )

  message(sprintf("[%s] SUMMARY: %s", platform_id, ifelse(summary_report$success, "SUCCESS", "FAILED")))
  message(sprintf("[%s] SUMMARY: Rows validated: %d", platform_id, rows_processed))
  message(sprintf("[%s] SUMMARY: Execution time (secs): %.2f", platform_id, execution_time))

  # ===========================================================================
  # PART 5: DEINITIALIZE
  # ===========================================================================

  if (connection_created_app && DBI::dbIsValid(app_data)) {
    DBI::dbDisconnect(app_data, shutdown = FALSE)
  }

  summary_report
}
