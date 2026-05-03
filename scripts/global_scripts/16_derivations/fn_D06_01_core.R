#' @title D06_01 Core Function - Product Recommendation (Cross-Company)
#' @description Generates personalized product recommendations using Random Forest
#'   for the first recommendation and conditional probability transition matrix
#'   for subsequent recommendations. Adapted from precision_marketing archive
#'   (fn_recommendation_item_id.R).
#' @param platform_id Character. Platform identifier (e.g., "amz", "cbz")
#' @param config Optional list. Platform config overrides.
#' @return List with success status and summary
#' @principle MP064, MP029, DEV_R052, DM_R044

run_D06_01 <- function(platform_id, config = NULL) {
  if (missing(platform_id) || is.null(platform_id) || !nzchar(platform_id)) {
    stop("platform_id is required")
  }

  # ===========================================================================
  # PART 1: INITIALIZE
  # ===========================================================================

  connection_created_transformed <- FALSE
  connection_created_app <- FALSE
  state <- new.env(parent = emptyenv())
  state$error_occurred <- FALSE
  state$test_passed <- FALSE
  state$rows_processed <- 0
  start_time <- Sys.time()
  drv_batch_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
  drv_script_name <- sprintf("%s_D06_01", platform_id)

  # Config defaults
  input_table_pattern <- if (!is.null(config$input_table_pattern)) {
    config$input_table_pattern
  } else {
    "df_%s_sales___standardized"
  }

  output_table_pattern <- if (!is.null(config$output_table_pattern)) {
    config$output_table_pattern
  } else {
    "df_product_recommendations"
  }

  recommended_number <- if (!is.null(config$recommended_number)) {
    config$recommended_number
  } else {
    5L
  }

  min_purchases <- if (!is.null(config$min_purchases)) {
    config$min_purchases
  } else {
    2L
  }

  rf_trees <- if (!is.null(config$rf_trees)) {
    config$rf_trees
  } else {
    100L
  }

  max_factor_levels <- if (!is.null(config$max_factor_levels)) {
    config$max_factor_levels
  } else {
    50L
  }

  seed <- if (!is.null(config$seed)) config$seed else 42L

  # Load metadata utility if available
  metadata_fn_path <- file.path(GLOBAL_DIR, "04_utils", "fn_add_drv_metadata.R")
  if (file.exists(metadata_fn_path)) source(metadata_fn_path)

  # Check randomForest availability (degrade gracefully if missing)
  rf_available <- requireNamespace("randomForest", quietly = TRUE)
  if (!rf_available) {
    message(sprintf("[%s] D06_01: WARNING - 'randomForest' package not installed. Falling back to popularity + conditional probability only.",
                    platform_id))
  }

  # Database connections
  if (!exists("transformed_data") || !inherits(transformed_data, "DBIConnection")) {
    transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = TRUE)
    connection_created_transformed <- TRUE
  }

  if (!exists("app_data") || !inherits(app_data, "DBIConnection")) {
    app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
    connection_created_app <- TRUE
  }

  # ===========================================================================
  # PART 2: MAIN
  # ===========================================================================

  tryCatch({
    set.seed(seed)

    required_table <- sprintf(input_table_pattern, platform_id)
    if (!DBI::dbExistsTable(transformed_data, required_table)) {
      stop(sprintf("Required input table %s not found in transformed_data", required_table))
    }

    message(sprintf("[%s] D06_01: Loading sales data from %s...", platform_id, required_table))

    sales_data <- dplyr::tbl(transformed_data, required_table) |>
      dplyr::select(dplyr::any_of(c(
        "customer_id", "asin", "purchase_date", "product_line_id"
      ))) |>
      dplyr::collect()

    if (nrow(sales_data) == 0) {
      stop("No sales data found in source table")
    }

    # Validate required columns
    required_cols <- c("customer_id", "asin", "purchase_date")
    missing_cols <- setdiff(required_cols, names(sales_data))
    if (length(missing_cols) > 0) {
      stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
    }

    # Clean data
    sales_data <- sales_data |>
      dplyr::filter(
        !is.na(customer_id),
        !is.na(asin),
        !is.na(purchase_date)
      ) |>
      dplyr::mutate(
        purchase_date = as.POSIXct(purchase_date),
        customer_id = as.integer(customer_id),
        asin = as.character(asin),
        product_line_id = dplyr::if_else(
          is.na(product_line_id) | !nzchar(as.character(product_line_id)),
          "unclassified",
          as.character(product_line_id)
        )
      )

    message(sprintf("[%s] D06_01: %d sales records, %d unique customers, %d unique products",
                    platform_id, nrow(sales_data),
                    length(unique(sales_data$customer_id)),
                    length(unique(sales_data$asin))))

    # -------------------------------------------------------------------------
    # Step 1: Build purchase sequences
    # -------------------------------------------------------------------------

    message(sprintf("[%s] D06_01: Building purchase sequences...", platform_id))

    df_sorted <- sales_data |>
      dplyr::arrange(customer_id, purchase_date) |>
      dplyr::group_by(customer_id) |>
      dplyr::mutate(
        purchase_seq = dplyr::row_number(),
        total_purchases = dplyr::n(),
        prev_asin = dplyr::lag(asin, default = NA_character_),
        prev_product_line = dplyr::lag(product_line_id, default = NA_character_)
      ) |>
      dplyr::ungroup()

    # Filter for customers with sufficient history
    eligible_customers <- df_sorted |>
      dplyr::filter(total_purchases >= min_purchases) |>
      dplyr::pull(customer_id) |>
      unique()

    message(sprintf("[%s] D06_01: %d customers with %d+ purchases (eligible for recommendation)",
                    platform_id, length(eligible_customers), min_purchases))

    if (length(eligible_customers) == 0) {
      stop("No customers with sufficient purchase history found")
    }

    # -------------------------------------------------------------------------
    # Step 2: Handle factor level limit for randomForest (max 53 categories)
    # -------------------------------------------------------------------------

    all_asins <- unique(df_sorted$asin)
    n_asins <- length(all_asins)

    if (n_asins > max_factor_levels) {
      message(sprintf("[%s] D06_01: %d unique ASINs exceeds RF limit (%d). Grouping rare products...",
                      platform_id, n_asins, max_factor_levels))

      asin_freq <- sort(table(df_sorted$asin), decreasing = TRUE)
      top_asins <- names(asin_freq)[seq_len(min(max_factor_levels, length(asin_freq)))]

      df_sorted <- df_sorted |>
        dplyr::mutate(
          asin_rf = ifelse(asin %in% top_asins, asin, "__other__"),
          prev_asin_rf = ifelse(is.na(prev_asin), NA_character_,
                                ifelse(prev_asin %in% top_asins, prev_asin, "__other__"))
        )
    } else {
      df_sorted <- df_sorted |>
        dplyr::mutate(
          asin_rf = asin,
          prev_asin_rf = prev_asin
        )
    }

    # -------------------------------------------------------------------------
    # Step 3: Train Random Forest model
    # -------------------------------------------------------------------------

    message(sprintf("[%s] D06_01: Preparing training data...", platform_id))

    train_data <- df_sorted |>
      dplyr::filter(
        customer_id %in% eligible_customers,
        !is.na(prev_asin_rf)
      ) |>
      dplyr::mutate(
        asin_rf = as.factor(asin_rf),
        prev_asin_rf = as.factor(prev_asin_rf),
        product_line_id = as.factor(product_line_id)
      )

    use_rf <- rf_available && nrow(train_data) >= 10

    if (!use_rf) {
      message(sprintf("[%s] D06_01: WARNING - Only %d training records (< 10). Falling back to popularity-based recommendations.",
                      platform_id, nrow(train_data)))
      rf_model <- NULL
    } else {
      message(sprintf("[%s] D06_01: Training Random Forest (%d trees, %d training records)...",
                      platform_id, rf_trees, nrow(train_data)))

      rf_model <- randomForest::randomForest(
        asin_rf ~ prev_asin_rf + product_line_id,
        data = train_data,
        ntree = rf_trees,
        importance = FALSE
      )

      message(sprintf("[%s] D06_01: RF model trained. OOB error rate: %.2f%%",
                      platform_id, rf_model$err.rate[rf_trees, "OOB"] * 100))
    }

    # -------------------------------------------------------------------------
    # Step 4: Build conditional probability transition matrix (ASIN-level)
    # -------------------------------------------------------------------------

    message(sprintf("[%s] D06_01: Building ASIN transition matrix...", platform_id))

    item_transitions <- df_sorted |>
      dplyr::filter(!is.na(prev_asin)) |>
      dplyr::group_by(prev_asin, asin) |>
      dplyr::summarize(count = dplyr::n(), .groups = "drop") |>
      dplyr::group_by(prev_asin) |>
      dplyr::mutate(prob = count / sum(count)) |>
      dplyr::select(prev_asin, asin, prob) |>
      dplyr::ungroup()

    # -------------------------------------------------------------------------
    # Step 5: Generate recommendations per customer
    # -------------------------------------------------------------------------

    message(sprintf("[%s] D06_01: Generating recommendations for %d customers...",
                    platform_id, length(eligible_customers)))

    # Get each customer's last purchase info
    last_purchases <- df_sorted |>
      dplyr::filter(customer_id %in% eligible_customers) |>
      dplyr::group_by(customer_id) |>
      dplyr::filter(purchase_seq == max(purchase_seq)) |>
      dplyr::ungroup()

    # Pre-compute global popularity ranking (used as fallback)
    global_popularity <- names(sort(table(sales_data$asin), decreasing = TRUE))

    # Recommendation function for a single customer row
    recommend_for_customer <- function(row_idx) {
      cust_row <- last_purchases[row_idx, , drop = FALSE]
      cust_id <- cust_row$customer_id
      recommendations <- character(recommended_number)

      # First recommendation: RF prediction (only if model was trained)
      rf_pred <- NULL
      if (use_rf && !is.null(rf_model)) {
        pred_data <- data.frame(
          prev_asin_rf = factor(cust_row$asin_rf, levels = levels(train_data$prev_asin_rf)),
          product_line_id = factor(cust_row$product_line_id, levels = levels(train_data$product_line_id))
        )

        # Handle unseen factor levels
        if (is.na(pred_data$prev_asin_rf[1])) {
          pred_data$prev_asin_rf[1] <- levels(train_data$prev_asin_rf)[1]
        }
        if (is.na(pred_data$product_line_id[1])) {
          pred_data$product_line_id[1] <- levels(train_data$product_line_id)[1]
        }

        rf_pred <- tryCatch({
          pred_probs <- predict(rf_model, pred_data, type = "prob")
          pred_probs_df <- as.data.frame(pred_probs)
          valid_cols <- intersect(colnames(pred_probs_df), c(all_asins, "__other__"))
          if (length(valid_cols) > 0) {
            probs_valid <- pred_probs_df[1, valid_cols, drop = FALSE]
            best <- colnames(probs_valid)[which.max(as.numeric(probs_valid[1, ]))]
            if (best == "__other__") NULL else best
          } else {
            NULL
          }
        }, error = function(e) NULL)
      }

      # If RF gives a valid ASIN, use it; otherwise fall back to conditional prob
      if (!is.null(rf_pred) && rf_pred %in% all_asins) {
        recommendations[1] <- rf_pred
      } else {
        # Fall back to conditional probability from last purchased ASIN
        cond <- item_transitions |>
          dplyr::filter(prev_asin == cust_row$asin) |>
          dplyr::arrange(dplyr::desc(prob))
        if (nrow(cond) > 0) {
          recommendations[1] <- cond$asin[1]
        } else {
          recommendations[1] <- global_popularity[1]
        }
      }

      # Subsequent recommendations: conditional probability chain
      for (i in seq(2, recommended_number)) {
        prev_rec <- recommendations[i - 1]
        cond_probs <- item_transitions |>
          dplyr::filter(
            prev_asin == prev_rec,
            !(asin %in% recommendations[seq_len(i - 1)])
          ) |>
          dplyr::arrange(dplyr::desc(prob))

        if (nrow(cond_probs) > 0) {
          recommendations[i] <- cond_probs$asin[1]
        } else {
          # Fall back to global popular items not yet recommended
          remaining <- setdiff(global_popularity, recommendations[seq_len(i - 1)])
          if (length(remaining) > 0) {
            recommendations[i] <- remaining[1]
          } else {
            recommendations[i] <- NA_character_
          }
        }
      }

      c(customer_id = cust_id, setNames(recommendations, sprintf("recommended_asin_%02d", seq_len(recommended_number))))
    }

    # Process all customers
    results_list <- lapply(seq_len(nrow(last_purchases)), recommend_for_customer)

    # Combine into data frame
    recommendations_df <- as.data.frame(do.call(rbind, results_list), stringsAsFactors = FALSE)
    recommendations_df$customer_id <- as.integer(recommendations_df$customer_id)

    # Add metadata columns
    recommendations_df$platform_id <- platform_id
    recommendations_df$model_type <- if (use_rf) "rf_conditional_prob" else "popularity_conditional_prob"
    recommendations_df$recommended_count <- recommended_number
    recommendations_df$generated_at <- Sys.time()

    if (exists("add_drv_metadata", mode = "function")) {
      recommendations_df <- add_drv_metadata(recommendations_df, drv_script_name, drv_batch_id)
    }

    message(sprintf("[%s] D06_01: Generated recommendations for %d customers",
                    platform_id, nrow(recommendations_df)))

    # -------------------------------------------------------------------------
    # Step 6: Write to app_data
    # -------------------------------------------------------------------------

    output_table <- output_table_pattern

    # Multi-platform safe: delete rows for this platform_id, then append
    if (DBI::dbExistsTable(app_data, output_table)) {
      DBI::dbExecute(
        app_data,
        sprintf("DELETE FROM %s WHERE platform_id = ?", output_table),
        params = list(platform_id)
      )
      DBI::dbWriteTable(app_data, output_table, as.data.frame(recommendations_df), append = TRUE)
    } else {
      DBI::dbWriteTable(app_data, output_table, as.data.frame(recommendations_df))
    }

    state$rows_processed <- nrow(recommendations_df)

    message(sprintf("[%s] D06_01: Wrote %d rows to app_data.%s",
                    platform_id, nrow(recommendations_df), output_table))

  }, error = function(e) {
    state$error_occurred <- TRUE
    message(sprintf("[%s] D06_01: ERROR - %s", platform_id, e$message))
  })

  # ===========================================================================
  # PART 3: TEST
  # ===========================================================================

  if (!state$error_occurred) {
    tryCatch({
      output_table <- output_table_pattern

      if (!DBI::dbExistsTable(app_data, output_table)) {
        stop(sprintf("Output table %s was not created in app_data", output_table))
      }

      sample_data <- dplyr::tbl(app_data, output_table) |>
        head(5) |>
        dplyr::collect()

      required_output_cols <- c("customer_id", "recommended_asin_01", "platform_id")
      missing_output <- setdiff(required_output_cols, names(sample_data))
      if (length(missing_output) > 0) {
        stop(sprintf("Missing required columns in output: %s", paste(missing_output, collapse = ", ")))
      }

      total_rows <- DBI::dbGetQuery(app_data, sprintf(
        "SELECT COUNT(*) AS n FROM %s WHERE platform_id = '%s'", output_table, platform_id
      ))$n
      if (total_rows == 0) {
        stop("Output table has no rows for this platform")
      }

      na_check <- DBI::dbGetQuery(app_data, sprintf(
        "SELECT COUNT(*) AS n FROM %s WHERE platform_id = '%s' AND recommended_asin_01 IS NULL",
        output_table, platform_id
      ))$n
      if (na_check > 0) {
        message(sprintf("[%s] D06_01: WARNING - %d rows have NULL first recommendation", platform_id, na_check))
      }

      state$test_passed <- TRUE
      message(sprintf("[%s] D06_01: TEST PASSED - %d recommendations verified", platform_id, total_rows))

    }, error = function(e) {
      state$test_passed <- FALSE
      message(sprintf("[%s] D06_01: TEST FAILED - %s", platform_id, e$message))
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
    outputs = output_table_pattern
  )

  message(sprintf("[%s] D06_01: %s", platform_id, ifelse(summary_report$success, "SUCCESS", "FAILED")))
  message(sprintf("[%s] D06_01: Rows: %d | Time: %.2f secs",
                  platform_id, state$rows_processed, execution_time))

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
