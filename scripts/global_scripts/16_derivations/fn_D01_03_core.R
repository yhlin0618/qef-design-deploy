#' @title D01_03 Core Function - Customer Profile Creation (Cross-Company)
#' @description Core logic for customer profile creation from standardized sales data.
#'              Follows DEV_R038: Core Function + Platform Wrapper Pattern.
#'              This function is shared across all companies via git subrepo.
#' @param platform_id Character. Platform identifier (e.g., "cbz", "amz", "eby", "shopify")
#' @param config Optional list. Platform config. If NULL, uses default table patterns.
#' @return List with success status and summary
#' @principle DEV_R038, DEV_R037, DM_R044, MP064, MP145

run_D01_03 <- function(platform_id, config = NULL) {
  # Validate platform_id is provided
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
  connection_created_cleansed <- FALSE

  error_occurred <- FALSE
  test_passed <- FALSE
  rows_processed <- 0
  start_time <- Sys.time()
  drv_batch_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
  drv_script_name <- sprintf("%s_D01_03", platform_id)

  # Default table patterns (can be overridden by config)
  input_table_pattern <- if (!is.null(config$input_table_pattern)) {
    config$input_table_pattern
  } else {
    "df_%s_sales___standardized"
  }

  output_profile_table <- if (!is.null(config$output_profile_table)) {
    config$output_profile_table
  } else {
    "df_profile_by_customer___cleansed"
  }

  # Source metadata function
  metadata_fn_path <- file.path(GLOBAL_DIR, "04_utils", "fn_add_drv_metadata.R")
  if (file.exists(metadata_fn_path)) source(metadata_fn_path)
  if (!exists("add_drv_metadata", mode = "function")) {
    stop("Missing required function add_drv_metadata")
  }

  # Establish database connections
  if (!exists("transformed_data") || !inherits(transformed_data, "DBIConnection")) {
    transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = TRUE)
    connection_created_transformed <- TRUE
  }

  if (!exists("cleansed_data") || !inherits(cleansed_data, "DBIConnection")) {
    cleansed_data <- dbConnectDuckdb(db_path_list$cleansed_data, read_only = FALSE)
    connection_created_cleansed <- TRUE
  }

  # Helper: Write platform-specific data to shared table
  write_platform_table <- function(con, table_name, data, platform_value) {
    if (!DBI::dbExistsTable(con, table_name)) {
      DBI::dbWriteTable(con, table_name, data, overwrite = TRUE)
      return(invisible())
    }
    table_id <- DBI::dbQuoteIdentifier(con, table_name)
    DBI::dbExecute(
      con,
      sprintf("DELETE FROM %s WHERE platform_id = ?", table_id),
      params = list(platform_value)
    )
    DBI::dbWriteTable(con, table_name, data, append = TRUE, overwrite = FALSE)
  }

  # Helper: Coalesce multiple columns
  coalesce_columns <- function(df, columns, default = NA_character_) {
    available <- columns[columns %in% names(df)]
    if (length(available) == 0) {
      return(rep(default, nrow(df)))
    }
    Reduce(dplyr::coalesce, lapply(available, function(col) df[[col]]))
  }

  # ===========================================================================
  # PART 2: MAIN
  # ===========================================================================

  tryCatch({
    required_table <- sprintf(input_table_pattern, platform_id)

    if (!DBI::dbExistsTable(transformed_data, required_table)) {
      stop(sprintf("Required input table %s not found in transformed_data", required_table))
    }

    message(sprintf("[%s] MAIN: Loading sales data from %s...", platform_id, required_table))

    sales_data <- dplyr::tbl(transformed_data, required_table) |>
      dplyr::select(dplyr::any_of(c(
        "customer_id",
        "platform_id",
        "buyer_name",
        "buyer_email",
        "customer_email",
        "customer_mobile",
        "customer_name",
        "receiver_name",
        "ship_postal_code",
        "email"
      ))) |>
      dplyr::collect()

    if (nrow(sales_data) == 0) {
      stop("No sales data found in source table")
    }

    if (!"customer_id" %in% names(sales_data)) {
      stop("Missing required column: customer_id")
    }

    sales_data$customer_id <- suppressWarnings(as.integer(sales_data$customer_id))

    platform_value <- if ("platform_id" %in% names(sales_data)) {
      as.character(sales_data$platform_id)
    } else {
      rep(platform_id, nrow(sales_data))
    }

    buyer_name_raw <- coalesce_columns(
      sales_data,
      c("buyer_name", "customer_name", "receiver_name")
    )
    email_raw <- coalesce_columns(
      sales_data,
      c("email", "buyer_email", "customer_email", "customer_mobile", "ship_postal_code")
    )

    buyer_name <- ifelse(
      is.na(buyer_name_raw) | buyer_name_raw == "",
      as.character(sales_data$customer_id),
      buyer_name_raw
    )

    customer_profile <- tibble::tibble(
      customer_id = sales_data$customer_id,
      platform_id = platform_value,
      buyer_name = buyer_name,
      email = email_raw
    ) |>
      dplyr::filter(!is.na(customer_id)) |>
      dplyr::distinct(customer_id, platform_id, .keep_all = TRUE)

    if (nrow(customer_profile) == 0) {
      stop("No valid customer profiles after processing")
    }

    customer_profile <- add_drv_metadata(customer_profile, drv_script_name, drv_batch_id)

    message(sprintf("[%s] MAIN: Writing %d profiles to cleansed_data...", platform_id, nrow(customer_profile)))
    write_platform_table(cleansed_data, output_profile_table, customer_profile, platform_id)
    rows_processed <- nrow(customer_profile)

  }, error = function(e) {
    error_occurred <<- TRUE
    message(sprintf("[%s] MAIN: ERROR - %s", platform_id, e$message))
  })

  # ===========================================================================
  # PART 3: TEST
  # ===========================================================================

  if (!error_occurred) {
    tryCatch({
      if (!DBI::dbExistsTable(cleansed_data, output_profile_table)) {
        stop(sprintf("Output table %s was not created", output_profile_table))
      }

      profile_sample <- dplyr::tbl(cleansed_data, output_profile_table) |>
        dplyr::filter(platform_id == !!platform_id) |>
        head(5) |>
        dplyr::collect()

      required_cols <- c("customer_id", "platform_id", "buyer_name", "email")
      missing_cols <- setdiff(required_cols, names(profile_sample))
      if (length(missing_cols) > 0) {
        stop(sprintf("Missing required columns: %s", paste(missing_cols, collapse = ", ")))
      }

      test_passed <- TRUE
      message(sprintf("[%s] TEST: Output table verified", platform_id))

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
    outputs = c(output_profile_table)
  )

  message(sprintf("[%s] SUMMARY: %s", platform_id, ifelse(summary_report$success, "SUCCESS", "FAILED")))
  message(sprintf("[%s] SUMMARY: Rows processed: %d", platform_id, rows_processed))
  message(sprintf("[%s] SUMMARY: Execution time (secs): %.2f", platform_id, execution_time))

  # ===========================================================================
  # PART 5: DEINITIALIZE
  # ===========================================================================

  if (connection_created_transformed && DBI::dbIsValid(transformed_data)) {
    DBI::dbDisconnect(transformed_data, shutdown = FALSE)
  }
  if (connection_created_cleansed && DBI::dbIsValid(cleansed_data)) {
    DBI::dbDisconnect(cleansed_data, shutdown = FALSE)
  }

  summary_report
}
