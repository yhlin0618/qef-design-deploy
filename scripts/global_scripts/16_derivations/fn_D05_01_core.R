#' @title D05_01 Core Function - Macro Monthly Summary (Cross-Company)
#' @description Aggregates standardized sales into monthly macro trends:
#'   revenue, order count, active/new customers, MoM/YoY growth rates.
#' @param platform_id Character. Platform identifier (e.g., "amz", "cbz")
#' @param config Optional list. Platform config. If NULL, uses defaults.
#' @return List with success status and summary
#' @principle MP064, DM_R044, MP145, DEV_R037

run_D05_01 <- function(platform_id, config = NULL) {
  if (missing(platform_id) || is.null(platform_id) || !nzchar(platform_id)) {
    stop("platform_id is required")
  }

  if (is.null(config)) {
    if (exists("get_platform_config", mode = "function", inherits = TRUE)) {
      config <- get_platform_config(platform_id, warn = FALSE)
    }
  }

  # ===========================================================================
  # PART 1: INITIALIZE
  # ===========================================================================

  connection_created_transformed <- FALSE
  connection_created_app <- FALSE
  state <- new.env(parent = emptyenv())
  state$error_occurred <- FALSE
  state$test_passed <- FALSE
  state$rows_written <- 0
  start_time <- Sys.time()
  drv_batch_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
  drv_script_name <- sprintf("%s_D05_01", platform_id)

  input_table_pattern <- if (!is.null(config$input_table_pattern)) {
    config$input_table_pattern
  } else {
    "df_%s_sales___standardized"
  }

  output_table <- "df_macro_monthly_summary"

  metadata_fn_path <- file.path(GLOBAL_DIR, "04_utils", "fn_add_drv_metadata.R")
  if (file.exists(metadata_fn_path)) source(metadata_fn_path)

  if (!exists("transformed_data") || !inherits(transformed_data, "DBIConnection")) {
    transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = TRUE)
    connection_created_transformed <- TRUE
  }

  if (!exists("app_data") || !inherits(app_data, "DBIConnection")) {
    app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
    connection_created_app <- TRUE
  }

  # Cross-platform-safe write helper (#374, same pattern as fn_D01_02_core
  # write_platform_table and fn_D03_01_core write_platform_table_d03).
  # Without this, sequential per-platform calls overwrite each other's rows.
  write_platform_table_d05 <- function(con, table_name, data, platform_value) {
    if (!DBI::dbExistsTable(con, table_name)) {
      DBI::dbWriteTable(con, table_name, data, overwrite = TRUE)
      return(invisible())
    }
    other_rows <- tryCatch({
      existing <- DBI::dbReadTable(con, table_name)
      if ("platform_id" %in% names(existing)) {
        as.data.frame(existing[existing$platform_id != platform_value, , drop = FALSE])
      } else {
        existing[FALSE, , drop = FALSE]
      }
    }, error = function(e) {
      message(sprintf("[%s] Could not read existing %s: %s", platform_value, table_name, e$message))
      NULL
    })
    DBI::dbRemoveTable(con, table_name)
    if (!is.null(other_rows) && nrow(other_rows) > 0) {
      merged <- tryCatch(
        dplyr::bind_rows(as.data.frame(other_rows), as.data.frame(data)),
        error = function(e) {
          message(sprintf(
            "[%s] WARNING: schema cannot be unified with existing rows for platforms (%s); dropping them. Re-run those platforms after this completes.",
            platform_value,
            paste(unique(other_rows$platform_id), collapse = ", ")
          ))
          as.data.frame(data)
        }
      )
      DBI::dbWriteTable(con, table_name, merged, overwrite = TRUE)
      message(sprintf("[%s] Merged: %d new + %d preserved from other platforms",
                      platform_value, nrow(data), nrow(other_rows)))
    } else {
      DBI::dbWriteTable(con, table_name, data, overwrite = TRUE)
    }
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

    sales_raw <- dplyr::tbl(transformed_data, required_table) |>
      dplyr::select(dplyr::any_of(c(
        "customer_id",
        "payment_time",
        "lineproduct_price",
        "platform_id",
        "product_line_id",
        "order_status",
        "amazon_order_id"
      ))) |>
      dplyr::collect()

    if (nrow(sales_raw) == 0) {
      stop("No sales data found in source table")
    }

    message(sprintf("[%s] MAIN: Loaded %d rows", platform_id, nrow(sales_raw)))

    # --- Identify order_id column (platform-agnostic) ---
    order_id_col <- intersect(
      c("amazon_order_id", "order_id", "order_number"),
      names(sales_raw)
    )
    if (length(order_id_col) == 0) {
      # Fallback: treat each row as one order
      sales_raw$order_id_proxy <- seq_len(nrow(sales_raw))
      order_id_col <- "order_id_proxy"
    } else {
      order_id_col <- order_id_col[1]
    }

    # --- Filter cancelled orders ---
    if ("order_status" %in% names(sales_raw)) {
      n_before <- nrow(sales_raw)
      sales_raw <- sales_raw[!grepl("(?i)cancel", sales_raw$order_status), ]
      message(sprintf("[%s] MAIN: Filtered cancelled orders: %d -> %d",
                       platform_id, n_before, nrow(sales_raw)))
    }

    # --- Preprocess ---
    sales_raw$payment_time <- as.POSIXct(sales_raw$payment_time)
    sales_raw$revenue <- as.numeric(sales_raw$lineproduct_price)
    sales_raw$customer_id <- as.integer(sales_raw$customer_id)

    # Platform ID
    if (!"platform_id" %in% names(sales_raw) || all(is.na(sales_raw$platform_id))) {
      sales_raw$platform_id <- platform_id
    }
    sales_raw$platform_id <- as.character(sales_raw$platform_id)

    # Product line
    if ("product_line_id" %in% names(sales_raw)) {
      sales_raw$product_line_id_filter <- dplyr::if_else(
        is.na(sales_raw$product_line_id) | !nzchar(as.character(sales_raw$product_line_id)),
        "unclassified",
        as.character(sales_raw$product_line_id)
      )
    } else {
      sales_raw$product_line_id_filter <- "unclassified"
    }

    # Filter invalid rows
    sales_raw <- sales_raw[
      !is.na(sales_raw$customer_id) &
      !is.na(sales_raw$payment_time) &
      !is.na(sales_raw$revenue),
    ]

    if (nrow(sales_raw) == 0) {
      stop("No valid sales records after preprocessing")
    }

    # Year-month column
    sales_raw$year_month <- format(sales_raw$payment_time, "%Y-%m")

    # Order ID for distinct counting
    sales_raw$order_id_val <- sales_raw[[order_id_col]]

    # --- Determine each customer's first purchase month ---
    first_purchase <- stats::aggregate(
      payment_time ~ customer_id,
      data = sales_raw,
      FUN = min
    )
    first_purchase$first_month <- format(first_purchase$payment_time, "%Y-%m")
    first_purchase <- first_purchase[, c("customer_id", "first_month")]

    # --- Aggregation function ---
    aggregate_monthly <- function(df_subset, pl_label) {
      message(sprintf("[%s] MAIN: Aggregating for product_line=%s (%d rows)...",
                       platform_id, pl_label, nrow(df_subset)))

      months <- sort(unique(df_subset$year_month))
      results <- vector("list", length(months))

      for (i in seq_along(months)) {
        m <- months[i]
        rows_m <- df_subset[df_subset$year_month == m, ]
        total_revenue <- sum(rows_m$revenue, na.rm = TRUE)
        order_count <- length(unique(rows_m$order_id_val))
        active_customers <- length(unique(rows_m$customer_id))

        # New customers: first purchase in this month
        customers_in_month <- unique(rows_m$customer_id)
        fp_subset <- first_purchase[first_purchase$customer_id %in% customers_in_month, ]
        new_customers <- sum(fp_subset$first_month == m, na.rm = TRUE)

        avg_order_value <- if (order_count > 0) total_revenue / order_count else 0

        results[[i]] <- data.frame(
          year_month = m,
          platform_id = platform_id,
          product_line_id_filter = pl_label,
          total_revenue = total_revenue,
          order_count = as.integer(order_count),
          active_customers = as.integer(active_customers),
          new_customers = as.integer(new_customers),
          avg_order_value = avg_order_value,
          stringsAsFactors = FALSE
        )
      }

      monthly_df <- do.call(rbind, results)

      # --- Calculate MoM ---
      monthly_df$mom_revenue_pct <- NA_real_
      if (nrow(monthly_df) > 1) {
        for (j in 2:nrow(monthly_df)) {
          prev_rev <- monthly_df$total_revenue[j - 1]
          if (!is.na(prev_rev) && prev_rev > 0) {
            monthly_df$mom_revenue_pct[j] <- (monthly_df$total_revenue[j] - prev_rev) / prev_rev * 100
          }
        }
      }

      # --- Calculate YoY ---
      monthly_df$yoy_revenue_pct <- NA_real_
      for (j in seq_len(nrow(monthly_df))) {
        ym <- monthly_df$year_month[j]
        yr <- as.integer(substr(ym, 1, 4))
        mo <- substr(ym, 6, 7)
        prev_ym <- sprintf("%04d-%s", yr - 1L, mo)
        prev_idx <- which(monthly_df$year_month == prev_ym)
        if (length(prev_idx) == 1) {
          prev_rev <- monthly_df$total_revenue[prev_idx]
          if (!is.na(prev_rev) && prev_rev > 0) {
            monthly_df$yoy_revenue_pct[j] <- (monthly_df$total_revenue[j] - prev_rev) / prev_rev * 100
          }
        }
      }

      monthly_df
    }

    # --- Per product line ---
    product_lines <- unique(sales_raw$product_line_id_filter)
    product_lines <- product_lines[!is.na(product_lines) & nzchar(product_lines)]
    if (length(product_lines) == 0) product_lines <- "unclassified"

    result_list <- vector("list", length(product_lines) + 1L)
    for (k in seq_along(product_lines)) {
      pl <- product_lines[k]
      df_pl <- sales_raw[sales_raw$product_line_id_filter == pl, ]
      if (nrow(df_pl) > 0) {
        result_list[[k]] <- aggregate_monthly(df_pl, pl)
      }
    }

    # --- "all" aggregate across product lines ---
    result_list[[length(product_lines) + 1L]] <- aggregate_monthly(sales_raw, "all")

    macro_summary <- do.call(rbind, result_list[!vapply(result_list, is.null, logical(1))])
    rownames(macro_summary) <- NULL

    # --- Add DRV metadata ---
    if (exists("add_drv_metadata", mode = "function")) {
      macro_summary <- add_drv_metadata(macro_summary, drv_script_name, drv_batch_id)
    }

    # --- Write to app_data ---
    message(sprintf("[%s] MAIN: Writing %d rows to %s in app_data...",
                     platform_id, nrow(macro_summary), output_table))

    # Cross-platform safe (#374)
    write_platform_table_d05(app_data, output_table, as.data.frame(macro_summary), platform_id)

    state$rows_written <- nrow(macro_summary)

  }, error = function(e) {
    state$error_occurred <- TRUE
    message(sprintf("[%s] MAIN: ERROR - %s", platform_id, e$message))
  })

  # ===========================================================================
  # PART 3: TEST
  # ===========================================================================

  if (!state$error_occurred) {
    tryCatch({
      if (!DBI::dbExistsTable(app_data, output_table)) {
        stop(sprintf("Output table %s was not created in app_data", output_table))
      }

      sample_data <- dplyr::tbl(app_data, output_table) |>
        utils::head(10) |>
        dplyr::collect()

      required_cols <- c("year_month", "platform_id", "product_line_id_filter",
                          "total_revenue", "order_count", "active_customers",
                          "new_customers", "avg_order_value",
                          "mom_revenue_pct", "yoy_revenue_pct")
      missing_cols <- setdiff(required_cols, names(sample_data))
      if (length(missing_cols) > 0) {
        stop(sprintf("Missing columns: %s", paste(missing_cols, collapse = ", ")))
      }

      # Verify "all" product line exists
      all_check <- dplyr::tbl(app_data, output_table) |>
        dplyr::filter(product_line_id_filter == "all") |>
        utils::head(1) |>
        dplyr::collect()
      if (nrow(all_check) == 0) {
        stop("Missing 'all' product line aggregate")
      }

      state$test_passed <- TRUE
      message(sprintf("[%s] TEST: Output table %s verified (%d rows)",
                       platform_id, output_table, state$rows_written))

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
    rows_written = state$rows_written,
    execution_time_secs = execution_time,
    outputs = output_table
  )

  message(sprintf("[%s] SUMMARY: %s", platform_id,
                   ifelse(summary_report$success, "SUCCESS", "FAILED")))
  message(sprintf("[%s] SUMMARY: Rows written: %d", platform_id, state$rows_written))
  message(sprintf("[%s] SUMMARY: Execution time: %.2f secs", platform_id, execution_time))

  # ===========================================================================
  # PART 5: DEINITIALIZE
  # ===========================================================================

  if (connection_created_transformed && DBI::dbIsValid(transformed_data)) {
    DBI::dbDisconnect(transformed_data, shutdown = FALSE)
  }
  if (connection_created_app && DBI::dbIsValid(app_data)) {
    DBI::dbDisconnect(app_data, shutdown = FALSE)
  }

  summary_report
}
