#' @title D01_02 Core Function - Customer Features + DNA (Cross-Company)
#' @description Computes customer-level features and executes DNA analysis.
#' @param platform_id Character. Platform identifier (e.g., "cbz", "amz", "eby")
#' @param config Optional list. Platform config. If NULL, uses default table patterns.
#' @return List with success status and summary
#' @principle DM_R044, MP064, MP145, DEV_R037, DEV_R038

run_D01_02 <- function(platform_id, config = NULL) {
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

  connection_created_processed <- FALSE
  connection_created_cleansed <- FALSE
  state <- new.env(parent = emptyenv())
  state$error_occurred <- FALSE
  state$test_passed <- FALSE
  state$rows_processed <- 0
  start_time <- Sys.time()
  drv_batch_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
  drv_script_name <- sprintf("%s_D01_02", platform_id)

  input_customer_table_pattern <- if (!is.null(config$input_table_pattern)) {
    config$input_table_pattern
  } else {
    "df_%s_sales_by_customer"
  }

  input_by_date_table_pattern <- if (!is.null(config$input_by_date_table_pattern)) {
    config$input_by_date_table_pattern
  } else {
    "df_%s_sales_by_customer_by_date"
  }

  output_features_table_pattern <- if (!is.null(config$output_table_pattern)) {
    config$output_table_pattern
  } else {
    "df_%s_customer_rfm"
  }

  output_dna_table <- if (!is.null(config$output_dna_table)) {
    config$output_dna_table
  } else {
    "df_dna_by_customer___cleansed"
  }

  metadata_fn_path <- file.path(GLOBAL_DIR, "04_utils", "fn_add_drv_metadata.R")
  if (file.exists(metadata_fn_path)) source(metadata_fn_path)

  analysis_dna_path <- file.path(GLOBAL_DIR, "04_utils", "fn_analysis_dna.R")
  if (!file.exists(analysis_dna_path)) {
    stop("Missing required function file: fn_analysis_dna.R")
  }
  source(analysis_dna_path)
  if (!exists("analysis_dna", mode = "function")) {
    stop("Missing required function analysis_dna")
  }

  # BTYD P(alive) — replaces logistic nrec_prob with BG/NBD model
  analysis_btyd_path <- file.path(GLOBAL_DIR, "04_utils", "fn_analysis_btyd.R")
  if (file.exists(analysis_btyd_path)) {
    source(analysis_btyd_path)
  }

  if (!exists("processed_data") || !inherits(processed_data, "DBIConnection")) {
    processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = FALSE)
    connection_created_processed <- TRUE
  }

  if (!exists("cleansed_data") || !inherits(cleansed_data, "DBIConnection")) {
    cleansed_data <- dbConnectDuckdb(db_path_list$cleansed_data, read_only = FALSE)
    connection_created_cleansed <- TRUE
  }

  write_platform_table <- function(con, table_name, data, platform_value) {
    if (!DBI::dbExistsTable(con, table_name)) {
      DBI::dbWriteTable(con, table_name, data, overwrite = TRUE)
      return(invisible())
    }

    table_id <- DBI::dbQuoteIdentifier(con, table_name)
    tryCatch({
      DBI::dbExecute(
        con,
        sprintf("DELETE FROM %s WHERE platform_id = ?", table_id),
        params = list(platform_value)
      )
      DBI::dbWriteTable(con, table_name, data, append = TRUE, overwrite = FALSE)
    }, error = function(e) {
      # Schema mismatch on append. Read existing rows for OTHER platforms,
      # union with new data, then overwrite. This preserves cross-platform
      # data instead of silently dropping it (issue #371 blocker 13).
      message(sprintf("[%s] Schema mismatch detected, merging %s safely...", platform_value, table_name))
      message(sprintf("[%s] Error was: %s", platform_value, e$message))

      other_rows <- tryCatch({
        existing <- DBI::dbReadTable(con, table_name)
        if ("platform_id" %in% names(existing)) {
          as.data.frame(existing[existing$platform_id != platform_value, , drop = FALSE])
        } else {
          existing[FALSE, , drop = FALSE]
        }
      }, error = function(e2) {
        message(sprintf("[%s] Could not read existing rows: %s", platform_value, e2$message))
        NULL
      })

      DBI::dbRemoveTable(con, table_name)

      if (!is.null(other_rows) && nrow(other_rows) > 0) {
        merged <- tryCatch(
          dplyr::bind_rows(as.data.frame(other_rows), as.data.frame(data)),
          error = function(e3) {
            # Schema cannot be unified — drop other_rows with explicit warning
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
    })
  }

  ensure_dna_fields <- function(df) {
    if (!"cai" %in% names(df)) {
      df$cai <- if ("cai_value" %in% names(df)) df$cai_value else NA_real_
    }
    if (!"cai_value" %in% names(df)) {
      df$cai_value <- if ("cai" %in% names(df)) df$cai else NA_real_
    }
    if (!"dna_m_score" %in% names(df)) {
      df$dna_m_score <- if ("m_ecdf" %in% names(df)) df$m_ecdf else NA_real_
    }
    if (!"dna_f_score" %in% names(df)) {
      df$dna_f_score <- if ("f_ecdf" %in% names(df)) df$f_ecdf else NA_real_
    }
    if (!"dna_r_score" %in% names(df)) {
      df$dna_r_score <- if ("r_ecdf" %in% names(df)) 1 - df$r_ecdf else NA_real_
    }
    if (!"dna_segment" %in% names(df)) {
      m_segment <- dplyr::case_when(
        is.na(df$dna_m_score) ~ NA_character_,
        df$dna_m_score >= 0.75 ~ "M4",
        df$dna_m_score >= 0.50 ~ "M3",
        df$dna_m_score >= 0.25 ~ "M2",
        TRUE ~ "M1"
      )
      f_segment <- dplyr::case_when(
        is.na(df$dna_f_score) ~ NA_character_,
        df$dna_f_score >= 0.67 ~ "F3",
        df$dna_f_score >= 0.33 ~ "F2",
        TRUE ~ "F1"
      )
      r_segment <- dplyr::case_when(
        is.na(df$dna_r_score) ~ NA_character_,
        df$dna_r_score >= 0.75 ~ "R4",
        df$dna_r_score >= 0.50 ~ "R3",
        df$dna_r_score >= 0.25 ~ "R2",
        TRUE ~ "R1"
      )
      df$dna_segment <- dplyr::if_else(
        is.na(m_segment) | is.na(f_segment) | is.na(r_segment),
        NA_character_,
        paste0(m_segment, f_segment, r_segment)
      )
    }
    df
  }

  convert_difftime_to_numeric <- function(df) {
    difftime_cols <- names(df)[vapply(df, function(col) inherits(col, "difftime"), logical(1))]
    for (col_name in difftime_cols) {
      df[[col_name]] <- as.numeric(df[[col_name]], units = "secs")
    }
    df
  }

  # ===========================================================================
  # PART 2: MAIN
  # ===========================================================================

  tryCatch({
    input_customer_table <- sprintf(input_customer_table_pattern, platform_id)
    input_by_date_table <- sprintf(input_by_date_table_pattern, platform_id)
    output_features_table <- sprintf(output_features_table_pattern, platform_id)

    if (!DBI::dbExistsTable(processed_data, input_customer_table)) {
      stop(sprintf("Required input table %s not found in processed_data", input_customer_table))
    }
    if (!DBI::dbExistsTable(processed_data, input_by_date_table)) {
      stop(sprintf("Required input table %s not found in processed_data", input_by_date_table))
    }

    customer_agg <- dplyr::tbl(processed_data, input_customer_table) |>
      dplyr::collect()
    sales_by_customer_by_date <- dplyr::tbl(processed_data, input_by_date_table) |>
      dplyr::collect()

    if (nrow(customer_agg) == 0) {
      stop("No customer aggregation data found")
    }
    if (nrow(sales_by_customer_by_date) == 0) {
      stop("No customer-by-date aggregation data found")
    }

    required_cols <- c(
      "customer_id",
      "sum_sales_by_customer",
      "sum_transactions_by_customer",
      "ipt",
      "min_time_by_date",
      "max_time_by_date"
    )
    missing_cols <- setdiff(required_cols, names(customer_agg))
    if (length(missing_cols) > 0) {
      stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
    }

    if (!"platform_id" %in% names(customer_agg)) {
      customer_agg$platform_id <- platform_id
    }
    if (!"platform_id" %in% names(sales_by_customer_by_date)) {
      sales_by_customer_by_date$platform_id <- platform_id
    }

    if (!"product_line_id_filter" %in% names(customer_agg)) {
      customer_agg$product_line_id_filter <- "unclassified"
    }
    if (!"product_line_id_filter" %in% names(sales_by_customer_by_date)) {
      sales_by_customer_by_date$product_line_id_filter <- "unclassified"
    }

    reference_dates <- customer_agg |>
      dplyr::group_by(product_line_id_filter) |>
      dplyr::summarise(reference_date = max(max_time_by_date, na.rm = TRUE), .groups = "drop")

    if (any(is.infinite(reference_dates$reference_date) | is.na(reference_dates$reference_date))) {
      stop("Unable to determine reference_date from max_time_by_date for one or more product lines")
    }

    customer_features <- customer_agg |>
      dplyr::left_join(reference_dates, by = "product_line_id_filter") |>
      dplyr::mutate(
        r_value = as.numeric(difftime(reference_date, max_time_by_date, units = "days")),
        f_value = as.numeric(sum_transactions_by_customer),
        m_value = as.numeric(sum_sales_by_customer),
        aov_value = dplyr::if_else(sum_transactions_by_customer > 0,
                                   as.numeric(sum_sales_by_customer / sum_transactions_by_customer),
                                   NA_real_),
        customer_tenure_days = as.numeric(difftime(reference_date, min_time_by_date, units = "days"))
      ) |>
      dplyr::group_by(product_line_id_filter) |>
      dplyr::mutate(
        r_ecdf = dplyr::percent_rank(desc(r_value)),
        f_ecdf = dplyr::percent_rank(f_value),
        m_ecdf = dplyr::percent_rank(m_value),
        r_label = dplyr::case_when(
          r_ecdf >= 0.67 ~ "Recent Buyer",
          r_ecdf >= 0.33 ~ "Medium Inactive",
          TRUE ~ "Long Inactive"
        ),
        f_label = dplyr::case_when(
          f_ecdf >= 0.67 ~ "High Frequency",
          f_ecdf >= 0.33 ~ "Medium Frequency",
          TRUE ~ "Low Frequency"
        ),
        m_label = dplyr::case_when(
          m_ecdf >= 0.67 ~ "High Value",
          m_ecdf >= 0.33 ~ "Medium Value",
          TRUE ~ "Low Value"
        )
      ) |>
      dplyr::ungroup()

    if (exists("add_drv_metadata", mode = "function")) {
      customer_features <- add_drv_metadata(customer_features, drv_script_name, drv_batch_id)
    }

    DBI::dbWriteTable(
      processed_data,
      output_features_table,
      as.data.frame(customer_features),
      overwrite = TRUE
    )

    product_line_values <- intersect(
      unique(customer_features$product_line_id_filter),
      unique(sales_by_customer_by_date$product_line_id_filter)
    )
    product_line_values <- product_line_values[!is.na(product_line_values) & nzchar(product_line_values)]
    if (length(product_line_values) == 0) {
      product_line_values <- "unclassified"
    }

    # Two-Pass BTYD Empirical Bayes: tracking lists for param collection and fallback
    btyd_params_collected <- list()
    btyd_status_collected <- list()
    btyd_by_date_cache <- list()

    customer_dna_list <- list()
    for (product_line_value in product_line_values) {
      feature_subset <- customer_features |>
        dplyr::filter(product_line_id_filter == !!product_line_value)
      by_date_subset <- sales_by_customer_by_date |>
        dplyr::filter(product_line_id_filter == !!product_line_value)

      if (nrow(feature_subset) == 0 || nrow(by_date_subset) == 0) {
        message(sprintf("[%s] MAIN: Skipping product_line_id_filter = %s (no data)", platform_id, product_line_value))
        next
      }

      message(sprintf("[%s] MAIN: Running analysis_dna() for product_line_id_filter = %s...", platform_id, product_line_value))
      dna_results <- analysis_dna(
        df_sales_by_customer = feature_subset,
        df_sales_by_customer_by_date = by_date_subset,
        skip_within_subject = FALSE,
        verbose = TRUE
      )

      customer_dna <- dna_results$data_by_customer
      if (is.null(customer_dna) || nrow(customer_dna) == 0) {
        message(sprintf("[%s] MAIN: analysis_dna() returned empty results for %s", platform_id, product_line_value))
        next
      }

      # Replace nrec_prob with BTYD P(alive) — Pass 1 (no fallback yet)
      if (exists("analysis_btyd", mode = "function")) {
        message(sprintf("[%s] MAIN: Running analysis_btyd() Pass 1 for %s...",
                        platform_id, product_line_value))
        tryCatch({
          customer_dna <- analysis_btyd(
            data_by_customer = customer_dna,
            df_sales_by_customer_by_date = by_date_subset,
            verbose = TRUE
          )
          # Collect BTYD metadata for Two-Pass Empirical Bayes
          btyd_status_val <- attr(customer_dna, "btyd_status")
          btyd_status_collected[[product_line_value]] <- if (is.null(btyd_status_val)) "unknown" else btyd_status_val
          btyd_params_collected[[product_line_value]] <- attr(customer_dna, "btyd_params")
          if (btyd_status_collected[[product_line_value]] %in% c("degenerate", "failed")) {
            btyd_by_date_cache[[product_line_value]] <- by_date_subset
          }
          message(sprintf("[%s] MAIN: BTYD Pass 1 status=%s for %s",
                          platform_id, btyd_status_collected[[product_line_value]], product_line_value))
        }, error = function(e) {
          message(sprintf("[%s] MAIN: BTYD failed (%s), keeping original nrec_prob",
                          platform_id, e$message))
          btyd_status_collected[[product_line_value]] <<- "failed"
          btyd_by_date_cache[[product_line_value]] <<- by_date_subset
        })
      }

      customer_dna <- ensure_dna_fields(customer_dna)
      customer_dna <- convert_difftime_to_numeric(customer_dna)
      customer_dna <- customer_dna |>
        dplyr::mutate(
          platform_id = platform_id,
          product_line_id_filter = product_line_value
        )

      if (exists("add_drv_metadata", mode = "function")) {
        customer_dna <- add_drv_metadata(customer_dna, drv_script_name, drv_batch_id)
      }

      customer_dna_list[[product_line_value]] <- customer_dna
    }

    if (length(customer_dna_list) == 0) {
      stop("analysis_dna() returned empty results for all product lines")
    }

    # --- Two-Pass BTYD: Pass 2 — Re-run failed slices with Empirical Bayes fallback ---
    if (exists("analysis_btyd", mode = "function") && length(btyd_params_collected) > 0) {
      successful_params <- btyd_params_collected[
        vapply(btyd_status_collected, function(s) identical(s, "estimated"), logical(1))
      ]
      needs_rerun <- names(btyd_status_collected)[
        vapply(btyd_status_collected, function(s) s %in% c("degenerate", "failed"), logical(1))
      ]

      if (length(successful_params) >= 1 && length(needs_rerun) > 0) {
        params_mat <- do.call(rbind, lapply(successful_params, function(p) {
          c(r = unname(p["r"]), alpha = unname(p["alpha"]),
            a = unname(p["a"]), b = unname(p["b"]))
        }))
        median_params <- apply(params_mat, 2, median)
        names(median_params) <- c("r", "alpha", "a", "b")

        message(sprintf("[%s] MAIN: BTYD Pass 2 — %d slices need fallback, %d successful params available",
                        platform_id, length(needs_rerun), length(successful_params)))
        message(sprintf("[%s] MAIN: Median fallback params: r=%.4f, alpha=%.4f, a=%.4f, b=%.4f",
                        platform_id, median_params["r"], median_params["alpha"],
                        median_params["a"], median_params["b"]))

        for (rerun_slice in needs_rerun) {
          if (!rerun_slice %in% names(customer_dna_list)) next
          if (!rerun_slice %in% names(btyd_by_date_cache)) next

          message(sprintf("[%s] MAIN: BTYD Pass 2 re-running %s with fallback params...",
                          platform_id, rerun_slice))
          tryCatch({
            customer_dna_list[[rerun_slice]] <- analysis_btyd(
              data_by_customer = customer_dna_list[[rerun_slice]],
              df_sales_by_customer_by_date = btyd_by_date_cache[[rerun_slice]],
              fallback_params = median_params,
              verbose = TRUE
            )
            message(sprintf("[%s] MAIN: BTYD Pass 2 completed for %s (status: %s)",
                            platform_id, rerun_slice,
                            attr(customer_dna_list[[rerun_slice]], "btyd_status")))
          }, error = function(e) {
            message(sprintf("[%s] MAIN: BTYD Pass 2 failed for %s (%s), keeping original nrec_prob",
                            platform_id, rerun_slice, e$message))
          })
        }
      } else if (length(needs_rerun) > 0) {
        message(sprintf("[%s] MAIN: BTYD Pass 2 — %d slices need fallback but no successful params available",
                        platform_id, length(needs_rerun)))
      }

      # Clean up cached data
      rm(btyd_by_date_cache)
    }

    # --- Generate proper "all" union slice (Issue #221) ---
    # "all" = union of ALL customers across product lines, not a literal category.
    # This mirrors D01_08's behavior: when product_line = "all", no filter is applied.
    message(sprintf("[%s] MAIN: Generating 'all' union slice from %d product lines...",
                    platform_id, length(product_line_values)))

    # Re-aggregate customer_agg across product lines at customer_id level
    all_customer_agg <- customer_agg |>
      dplyr::group_by(customer_id, platform_id) |>
      dplyr::summarize(
        sum_sales_by_customer = sum(sum_sales_by_customer, na.rm = TRUE),
        sum_transactions_by_customer = sum(sum_transactions_by_customer, na.rm = TRUE),
        min_time_by_date = min(min_time_by_date, na.rm = TRUE),
        max_time_by_date = max(max_time_by_date, na.rm = TRUE),
        .groups = "drop"
      ) |>
      dplyr::mutate(
        ni = sum_transactions_by_customer,
        ipt = dplyr::if_else(
          sum_transactions_by_customer > 1,
          as.numeric(difftime(max_time_by_date, min_time_by_date, units = "days")) /
            (sum_transactions_by_customer - 1),
          NA_real_
        ),
        product_line_id_filter = "all"
      )

    # Re-aggregate sales_by_customer_by_date across product lines
    all_by_date <- sales_by_customer_by_date |>
      dplyr::group_by(customer_id, date, platform_id) |>
      dplyr::summarize(
        sum_spent_by_date = sum(sum_spent_by_date, na.rm = TRUE),
        count_transactions_by_date = sum(count_transactions_by_date, na.rm = TRUE),
        min_time_by_date = min(min_time_by_date, na.rm = TRUE),
        .groups = "drop"
      ) |>
      dplyr::arrange(customer_id, date) |>
      dplyr::group_by(customer_id) |>
      dplyr::mutate(
        ni = dplyr::row_number(),
        times = as.numeric(difftime(date, min(date), units = "days")),
        ipt = dplyr::if_else(
          dplyr::row_number() > 1,
          as.numeric(difftime(date, dplyr::lag(date), units = "days")),
          NA_real_
        )
      ) |>
      dplyr::ungroup() |>
      dplyr::mutate(product_line_id_filter = "all")

    # Compute RFM features for the "all" slice
    all_reference_date <- max(all_customer_agg$max_time_by_date, na.rm = TRUE)
    all_features <- all_customer_agg |>
      dplyr::mutate(
        reference_date = all_reference_date,
        r_value = as.numeric(difftime(reference_date, max_time_by_date, units = "days")),
        f_value = as.numeric(sum_transactions_by_customer),
        m_value = as.numeric(sum_sales_by_customer),
        aov_value = dplyr::if_else(
          sum_transactions_by_customer > 0,
          as.numeric(sum_sales_by_customer / sum_transactions_by_customer),
          NA_real_
        ),
        customer_tenure_days = as.numeric(difftime(reference_date, min_time_by_date, units = "days"))
      ) |>
      dplyr::mutate(
        r_ecdf = dplyr::percent_rank(dplyr::desc(r_value)),
        f_ecdf = dplyr::percent_rank(f_value),
        m_ecdf = dplyr::percent_rank(m_value),
        r_label = dplyr::case_when(
          r_ecdf >= 0.67 ~ "Recent Buyer",
          r_ecdf >= 0.33 ~ "Medium Inactive",
          TRUE ~ "Long Inactive"
        ),
        f_label = dplyr::case_when(
          f_ecdf >= 0.67 ~ "High Frequency",
          f_ecdf >= 0.33 ~ "Medium Frequency",
          TRUE ~ "Low Frequency"
        ),
        m_label = dplyr::case_when(
          m_ecdf >= 0.67 ~ "High Value",
          m_ecdf >= 0.33 ~ "Medium Value",
          TRUE ~ "Low Value"
        )
      )

    message(sprintf("[%s] MAIN: 'all' union has %d unique customers", platform_id, nrow(all_features)))

    # Run analysis_dna() on the "all" union
    message(sprintf("[%s] MAIN: Running analysis_dna() for 'all' union...", platform_id))
    all_dna_results <- analysis_dna(
      df_sales_by_customer = all_features,
      df_sales_by_customer_by_date = all_by_date,
      skip_within_subject = FALSE,
      verbose = TRUE
    )

    all_dna <- all_dna_results$data_by_customer
    if (!is.null(all_dna) && nrow(all_dna) > 0) {
      # Run analysis_btyd() on the "all" union
      if (exists("analysis_btyd", mode = "function")) {
        message(sprintf("[%s] MAIN: Running analysis_btyd() for 'all' union...", platform_id))
        tryCatch({
          all_dna <- analysis_btyd(
            data_by_customer = all_dna,
            df_sales_by_customer_by_date = all_by_date,
            verbose = TRUE
          )
          all_btyd_status <- attr(all_dna, "btyd_status")
          message(sprintf("[%s] MAIN: BTYD 'all' union status=%s",
                          platform_id, if (is.null(all_btyd_status)) "unknown" else all_btyd_status))
        }, error = function(e) {
          message(sprintf("[%s] MAIN: BTYD 'all' union failed (%s), keeping original nrec_prob",
                          platform_id, e$message))
        })
      }

      all_dna <- ensure_dna_fields(all_dna)
      all_dna <- convert_difftime_to_numeric(all_dna)
      all_dna <- all_dna |>
        dplyr::mutate(
          platform_id = platform_id,
          product_line_id_filter = "all"
        )

      if (exists("add_drv_metadata", mode = "function")) {
        all_dna <- add_drv_metadata(all_dna, drv_script_name, drv_batch_id)
      }

      customer_dna_list[["all"]] <- all_dna
      message(sprintf("[%s] MAIN: 'all' union slice added (%d customers)", platform_id, nrow(all_dna)))
    } else {
      message(sprintf("[%s] MAIN: WARNING - 'all' union analysis_dna() returned empty", platform_id))
    }

    # Clean up "all" union temp objects
    rm(all_customer_agg, all_by_date, all_features, all_dna_results)
    if (exists("all_dna")) rm(all_dna)

    customer_dna <- dplyr::bind_rows(customer_dna_list)
    write_platform_table(cleansed_data, output_dna_table, as.data.frame(customer_dna), platform_id)

    state$rows_processed <- nrow(customer_dna)

  }, error = function(e) {
    state$error_occurred <- TRUE
    message(sprintf("[%s] MAIN: ERROR - %s", platform_id, e$message))
  })

  # ===========================================================================
  # PART 3: TEST
  # ===========================================================================

  if (!state$error_occurred) {
    tryCatch({
      output_features_table <- sprintf(output_features_table_pattern, platform_id)
      if (!DBI::dbExistsTable(processed_data, output_features_table)) {
        stop(sprintf("Output table %s was not created", output_features_table))
      }
      if (!DBI::dbExistsTable(cleansed_data, output_dna_table)) {
        stop(sprintf("Output table %s was not created", output_dna_table))
      }

      sample_features <- dplyr::tbl(processed_data, output_features_table) |>
        head(5) |>
        dplyr::collect()
      sample_dna <- dplyr::tbl(cleansed_data, output_dna_table) |>
        dplyr::filter(platform_id == !!platform_id) |>
        head(5) |>
        dplyr::collect()

      required_features_cols <- c("customer_id", "platform_id", "product_line_id_filter", "r_value", "f_value", "m_value")
      missing_features_cols <- setdiff(required_features_cols, names(sample_features))
      if (length(missing_features_cols) > 0) {
        stop(sprintf("Missing required columns in features output: %s", paste(missing_features_cols, collapse = ", ")))
      }

      required_dna_cols <- c("customer_id", "platform_id", "product_line_id_filter", "dna_segment", "nes_status")
      missing_dna_cols <- setdiff(required_dna_cols, names(sample_dna))
      if (length(missing_dna_cols) > 0) {
        stop(sprintf("Missing required columns in DNA output: %s", paste(missing_dna_cols, collapse = ", ")))
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
    outputs = c(sprintf(output_features_table_pattern, platform_id), output_dna_table)
  )

  message(sprintf("[%s] SUMMARY: %s", platform_id, ifelse(summary_report$success, "SUCCESS", "FAILED")))
  message(sprintf("[%s] SUMMARY: Rows processed: %d", platform_id, state$rows_processed))
  message(sprintf("[%s] SUMMARY: Execution time (secs): %.2f", platform_id, execution_time))

  # ===========================================================================
  # PART 5: DEINITIALIZE
  # ===========================================================================

  if (connection_created_processed && DBI::dbIsValid(processed_data)) {
    DBI::dbDisconnect(processed_data, shutdown = FALSE)
  }
  if (connection_created_cleansed && DBI::dbIsValid(cleansed_data)) {
    DBI::dbDisconnect(cleansed_data, shutdown = FALSE)
  }

  summary_report
}
