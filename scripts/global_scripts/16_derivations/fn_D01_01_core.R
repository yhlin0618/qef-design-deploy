#' @title D01_01 Core Function - Customer Aggregation (Cross-Company)
#' @description Aggregates standardized sales into customer-by-date and customer-level tables.
#' @param platform_id Character. Platform identifier (e.g., "cbz", "amz", "eby")
#' @param config Optional list. Platform config. If NULL, uses default table patterns.
#' @return List with success status and summary
#' @principle DM_R044, MP064, MP145, DEV_R037, DEV_R038

run_D01_01 <- function(platform_id, config = NULL) {
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
  connection_created_processed <- FALSE
  state <- new.env(parent = emptyenv())
  state$error_occurred <- FALSE
  state$test_passed <- FALSE
  state$rows_processed <- 0
  start_time <- Sys.time()
  drv_batch_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
  drv_script_name <- sprintf("%s_D01_01", platform_id)

  input_table_pattern <- if (!is.null(config$input_table_pattern)) {
    config$input_table_pattern
  } else {
    "df_%s_sales___standardized"
  }

  output_by_date_pattern <- if (!is.null(config$output_by_date_pattern)) {
    config$output_by_date_pattern
  } else {
    "df_%s_sales_by_customer_by_date"
  }

  output_by_customer_pattern <- if (!is.null(config$output_by_customer_pattern)) {
    config$output_by_customer_pattern
  } else {
    "df_%s_sales_by_customer"
  }

  transform_fn_path <- file.path(GLOBAL_DIR, "17_transform", "fn_transform_sales_to_sales_by_customer.by_date.R")
  transform_by_customer_fn_path <- file.path(GLOBAL_DIR, "17_transform", "fn_transform_sales_by_customer.by_date_to_sales_by_customer.R")

  if (file.exists(transform_fn_path)) source(transform_fn_path)
  if (file.exists(transform_by_customer_fn_path)) source(transform_by_customer_fn_path)

  if (!exists("transform_sales_to_sales_by_customer.by_date")) {
    stop("Missing required function transform_sales_to_sales_by_customer.by_date")
  }
  if (!exists("transform_sales_by_customer.by_date_to_sales_by_customer")) {
    stop("Missing required function transform_sales_by_customer.by_date_to_sales_by_customer")
  }

  metadata_fn_path <- file.path(GLOBAL_DIR, "04_utils", "fn_add_drv_metadata.R")
  if (file.exists(metadata_fn_path)) source(metadata_fn_path)

  if (!exists("transformed_data") || !inherits(transformed_data, "DBIConnection")) {
    transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = TRUE)
    connection_created_transformed <- TRUE
  }

  if (!exists("processed_data") || !inherits(processed_data, "DBIConnection")) {
    processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = FALSE)
    connection_created_processed <- TRUE
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
        "payment_time",
        "lineproduct_price",
        "platform_id",
        "product_line_id"
      ))) |>
      dplyr::collect()

    if (nrow(sales_data) == 0) {
      stop("No sales data found in source table")
    }

    required_columns <- c("customer_id", "payment_time", "lineproduct_price")
    missing_columns <- setdiff(required_columns, names(sales_data))
    if (length(missing_columns) > 0) {
      stop("Missing required columns: ", paste(missing_columns, collapse = ", "))
    }

    platform_value <- if ("platform_id" %in% names(sales_data)) {
      as.character(sales_data$platform_id)
    } else {
      rep(platform_id, nrow(sales_data))
    }

    product_line_value <- if ("product_line_id" %in% names(sales_data)) {
      as.character(sales_data$product_line_id)
    } else {
      rep(NA_character_, nrow(sales_data))
    }

    sales_data <- sales_data |>
      dplyr::mutate(
        payment_time = as.POSIXct(payment_time),
        total_spent = as.numeric(lineproduct_price),
        platform_id = as.character(platform_value),
        customer_id = as.integer(customer_id),
        product_line_id_filter = dplyr::if_else(
          is.na(product_line_value) | !nzchar(product_line_value),
          "unclassified",
          product_line_value
        )
      ) |>
      dplyr::filter(
        !is.na(customer_id),
        !is.na(payment_time),
        !is.na(total_spent)
      )

    if (all(is.na(product_line_value)) || all(sales_data$product_line_id_filter == "unclassified")) {
      message(sprintf("[%s] MAIN: product_line_id missing or empty; defaulting to 'unclassified'.", platform_id))
    }

    if (nrow(sales_data) == 0) {
      stop("No valid sales records after preprocessing")
    }

    build_customer_tables <- function(df_subset, product_line_id_filter_value) {
      message(sprintf(
        "[%s] MAIN: Aggregating sales for product_line_id_filter = %s...",
        platform_id, product_line_id_filter_value
      ))

      sales_by_customer_by_date_local <- transform_sales_to_sales_by_customer.by_date(
        df = data.table::as.data.table(df_subset[, c("customer_id", "payment_time", "total_spent", "platform_id", "product_line_id_filter")]),
        first_cols = c("platform_id", "product_line_id_filter"),
        time = "payment_time",
        verbose = TRUE
      )

      sales_by_customer_local <- transform_sales_by_customer.by_date_to_sales_by_customer(
        df = sales_by_customer_by_date_local,
        first_cols = c("min_time_by_date", "ni", "ipt", "platform_id", "product_line_id_filter"),
        verbose = TRUE
      )

      if (!"product_line_id_filter" %in% names(sales_by_customer_by_date_local)) {
        sales_by_customer_by_date_local$product_line_id_filter <- product_line_id_filter_value
      }
      if (!"product_line_id_filter" %in% names(sales_by_customer_local)) {
        sales_by_customer_local$product_line_id_filter <- product_line_id_filter_value
      }

      list(by_date = sales_by_customer_by_date_local, by_customer = sales_by_customer_local)
    }

    product_line_values <- unique(sales_data$product_line_id_filter)
    product_line_values <- product_line_values[!is.na(product_line_values) & nzchar(product_line_values)]
    if (length(product_line_values) == 0) {
      product_line_values <- "unclassified"
    }

    if (length(product_line_values) == 1) {
      result_tables <- build_customer_tables(sales_data, product_line_values[[1]])
      sales_by_customer_by_date <- result_tables$by_date
      sales_by_customer <- result_tables$by_customer
    } else {
      sales_by_customer_by_date_list <- list()
      sales_by_customer_list <- list()
      for (product_line_value in product_line_values) {
        df_subset <- sales_data[sales_data$product_line_id_filter == product_line_value, , drop = FALSE]
        if (nrow(df_subset) == 0) next
        result_tables <- build_customer_tables(df_subset, product_line_value)
        sales_by_customer_by_date_list[[product_line_value]] <- result_tables$by_date
        sales_by_customer_list[[product_line_value]] <- result_tables$by_customer
      }
      sales_by_customer_by_date <- dplyr::bind_rows(sales_by_customer_by_date_list)
      sales_by_customer <- dplyr::bind_rows(sales_by_customer_list)
    }

    if (!"platform_id" %in% names(sales_by_customer)) {
      sales_by_customer$platform_id <- platform_id
    }
    if (!"product_line_id_filter" %in% names(sales_by_customer_by_date)) {
      sales_by_customer_by_date$product_line_id_filter <- "unclassified"
    }
    if (!"product_line_id_filter" %in% names(sales_by_customer)) {
      sales_by_customer$product_line_id_filter <- "unclassified"
    }

    if (exists("add_drv_metadata", mode = "function")) {
      sales_by_customer_by_date <- add_drv_metadata(sales_by_customer_by_date, drv_script_name, drv_batch_id)
      sales_by_customer <- add_drv_metadata(sales_by_customer, drv_script_name, drv_batch_id)
    }

    output_by_date_table <- sprintf(output_by_date_pattern, platform_id)
    output_by_customer_table <- sprintf(output_by_customer_pattern, platform_id)

    DBI::dbWriteTable(
      processed_data,
      output_by_date_table,
      as.data.frame(sales_by_customer_by_date),
      overwrite = TRUE
    )

    DBI::dbWriteTable(
      processed_data,
      output_by_customer_table,
      as.data.frame(sales_by_customer),
      overwrite = TRUE
    )

    state$rows_processed <- nrow(sales_by_customer)

  }, error = function(e) {
    state$error_occurred <- TRUE
    message(sprintf("[%s] MAIN: ERROR - %s", platform_id, e$message))
  })

  # ===========================================================================
  # PART 3: TEST
  # ===========================================================================

  if (!state$error_occurred) {
    tryCatch({
      output_by_date_table <- sprintf(output_by_date_pattern, platform_id)
      output_by_customer_table <- sprintf(output_by_customer_pattern, platform_id)

      if (!DBI::dbExistsTable(processed_data, output_by_date_table)) {
        stop(sprintf("Output table %s was not created", output_by_date_table))
      }
      if (!DBI::dbExistsTable(processed_data, output_by_customer_table)) {
        stop(sprintf("Output table %s was not created", output_by_customer_table))
      }

      sample_by_date <- dplyr::tbl(processed_data, output_by_date_table) |>
        head(5) |>
        dplyr::collect()
      sample_by_customer <- dplyr::tbl(processed_data, output_by_customer_table) |>
        head(5) |>
        dplyr::collect()

      required_by_date <- c("customer_id", "sum_spent_by_date", "count_transactions_by_date", "min_time_by_date")
      required_by_customer <- c("customer_id", "sum_sales_by_customer", "sum_transactions_by_customer", "ipt", "ni")

      missing_by_date <- setdiff(required_by_date, names(sample_by_date))
      missing_by_customer <- setdiff(required_by_customer, names(sample_by_customer))

      if (length(missing_by_date) > 0) {
        stop(sprintf("Missing required columns in by-date table: %s", paste(missing_by_date, collapse = ", ")))
      }
      if (length(missing_by_customer) > 0) {
        stop(sprintf("Missing required columns in customer table: %s", paste(missing_by_customer, collapse = ", ")))
      }

      state$test_passed <- TRUE
      message(sprintf("[%s] TEST: Output tables verified", platform_id))

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
    rows_processed = state$rows_processed,
    execution_time_secs = execution_time,
    outputs = c(sprintf(output_by_date_pattern, platform_id),
                sprintf(output_by_customer_pattern, platform_id))
  )

  message(sprintf("[%s] SUMMARY: %s", platform_id, ifelse(summary_report$success, "SUCCESS", "FAILED")))
  message(sprintf("[%s] SUMMARY: Rows processed: %d", platform_id, state$rows_processed))
  message(sprintf("[%s] SUMMARY: Execution time (secs): %.2f", platform_id, execution_time))

  # ===========================================================================
  # PART 5: DEINITIALIZE
  # ===========================================================================

  if (connection_created_transformed && DBI::dbIsValid(transformed_data)) {
    DBI::dbDisconnect(transformed_data, shutdown = FALSE)
  }
  if (connection_created_processed && DBI::dbIsValid(processed_data)) {
    DBI::dbDisconnect(processed_data, shutdown = FALSE)
  }

  summary_report
}
